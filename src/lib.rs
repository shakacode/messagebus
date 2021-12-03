mod builder;
mod envelop;
pub mod error;
mod handler;
mod receiver;
pub mod receivers;
mod relay;
mod stats;
mod trait_object;
pub mod type_tag;

pub mod __reexport {
    pub use ctor;
    pub use serde;
}

#[macro_use]
extern crate log;

pub mod derive {
    pub use messagebus_derive::*;
}

// privavte
use core::{
    any::Any,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};
use smallvec::SmallVec;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::Mutex;

use builder::BusBuilder;
use error::{Error, SendError, StdSyncSendError};
use receiver::{Permit, Receiver};
use stats::Stats;

// public
pub use builder::Module;
pub use ctor;
pub use envelop::{IntoBoxedMessage, Message, MessageBounds, SharedMessage, TypeTag, TypeTagged};
pub use handler::*;
pub use receiver::{
    Action, Event, EventBoxed, ReciveTypedReceiver, ReciveUntypedReceiver, SendTypedReceiver,
    SendUntypedReceiver, TypeTagAccept, TypeTagAcceptItem,
};
pub use relay::Relay;
pub use type_tag::{deserialize_shared_message, register_shared_message};
pub type Untyped = Arc<dyn Any + Send + Sync>;

type LookupQuery = (TypeTag, Option<TypeTag>, Option<TypeTag>);

static ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SendOptions {
    Broadcast,
    Except(u64),
    Direct(u64),
    Random,
    Balanced,
}

impl Default for SendOptions {
    fn default() -> Self {
        Self::Broadcast
    }
}

pub struct BusInner {
    receivers: HashSet<Receiver>,
    lookup: HashMap<LookupQuery, SmallVec<[Receiver; 4]>>,
    closed: AtomicBool,
    maintain: Mutex<()>,
}

impl BusInner {
    pub(crate) fn new(receivers: HashSet<Receiver>) -> Self {
        let mut lookup = HashMap::new();
        for recv in receivers.iter() {
            for (msg, resp) in recv.iter_types() {
                lookup
                    .entry((msg.clone(), None, None))
                    .or_insert_with(HashSet::new)
                    .insert(recv.clone());

                if let Some((resp, err)) = resp {
                    lookup
                        .entry((msg.clone(), Some(resp.clone()), None))
                        .or_insert_with(HashSet::new)
                        .insert(recv.clone());

                    lookup
                        .entry((msg.clone(), None, Some(err.clone())))
                        .or_insert_with(HashSet::new)
                        .insert(recv.clone());

                    lookup
                        .entry((msg, Some(resp), Some(err)))
                        .or_insert_with(HashSet::new)
                        .insert(recv.clone());
                }
            }
        }

        let lookup = lookup
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().collect()))
            .collect();

        Self {
            receivers,
            lookup,
            closed: AtomicBool::new(false),
            maintain: Mutex::new(()),
        }
    }
}

#[derive(Clone)]
pub struct Bus {
    inner: Arc<BusInner>,
}

impl Bus {
    #[inline]
    pub fn build() -> BusBuilder {
        BusBuilder::new()
    }

    pub fn is_closing(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn init(&self) {
        for r in self.inner.receivers.iter() {
            r.init(self).unwrap();
        }
    }

    pub async fn ready(&self) {
        for r in self.inner.receivers.iter() {
            r.ready().await;
        }
    }

    pub async fn close(&self) {
        let _handle = self.inner.maintain.lock().await;
        self.inner.closed.store(true, Ordering::SeqCst);

        for r in self.inner.receivers.iter() {
            let err = tokio::time::timeout(Duration::from_secs(20), r.close(self)).await;

            if let Err(err) = err {
                error!("Close timeout on {}: {}", r.name(), err);
            }
        }
    }

    pub async fn flush_all(&self) {
        let fuse_count = 32i32;
        let mut breaked = false;
        let mut iters = 0usize;
        for _ in 0..fuse_count {
            iters += 1;
            let mut flushed = false;
            for r in self.inner.receivers.iter() {
                if r.need_flush() {
                    flushed = true;

                    r.flush(self).await;
                }
            }

            if !flushed {
                breaked = true;
                break;
            }
        }

        if !breaked {
            warn!(
                "!!! WARNING: unable to reach equilibrium in {} iterations !!!",
                fuse_count
            );
        } else {
            info!("flushed in {} iterations !!!", iters);
        }
    }

    pub async fn flush<M: Message>(&self) {
        let fuse_count = 32i32;
        let mut breaked = false;
        let mut iters = 0usize;

        for _ in 0..fuse_count {
            let receivers =
                self.select_receivers(M::type_tag_(), Default::default(), None, None, false);
            iters += 1;
            let mut flushed = false;
            for r in receivers {
                if r.need_flush() {
                    flushed = true;

                    r.flush(self).await;
                }
            }

            if !flushed {
                breaked = true;
                break;
            }
        }

        if !breaked {
            warn!(
                "!!! WARNING: unable to reach equilibrium in {} iterations !!!",
                fuse_count
            );
        } else {
            info!("flushed in {} iterations !!!", iters);
        }
    }

    pub async fn flush2<M1: Message, M2: Message>(&self) {
        let fuse_count = 32i32;
        let mut breaked = false;
        let mut iters = 0usize;
        for _ in 0..fuse_count {
            let receivers1 =
                self.select_receivers(M1::type_tag_(), Default::default(), None, None, false);

            let receivers2 =
                self.select_receivers(M2::type_tag_(), Default::default(), None, None, false);

            iters += 1;
            let mut flushed = false;
            for r in receivers1.chain(receivers2) {
                if r.need_flush() {
                    flushed = true;

                    r.flush(self).await;
                }
            }

            if !flushed {
                breaked = true;
                break;
            }
        }

        if !breaked {
            warn!(
                "!!! WARNING: unable to reach equilibrium in {} iterations !!!",
                fuse_count
            );
        } else {
            info!("flushed in {} iterations !!!", iters);
        }
    }

    pub async fn sync_all(&self) {
        let _handle = self.inner.maintain.lock().await;

        for r in self.inner.receivers.iter() {
            r.sync(self).await;
        }
    }

    pub async fn sync<M: Message>(&self) {
        let _handle = self.inner.maintain.lock().await;
        let receivers =
            self.select_receivers(M::type_tag_(), Default::default(), None, None, false);

        for r in receivers {
            r.sync(self).await;
        }
    }

    pub async fn sync2<M1: Message, M2: Message>(&self) {
        let _handle = self.inner.maintain.lock().await;

        let receivers1 =
            self.select_receivers(M1::type_tag_(), Default::default(), None, None, false);

        let receivers2 =
            self.select_receivers(M2::type_tag_(), Default::default(), None, None, false);

        for r in receivers1.chain(receivers2) {
            r.sync(self).await;
        }
    }

    #[inline]
    pub async fn flush_and_sync_all(&self) {
        self.flush_all().await;
        self.sync_all().await;
    }
    #[inline]
    pub async fn flush_and_sync<M: Message>(&self) {
        self.flush::<M>().await;
        self.sync::<M>().await;
    }
    #[inline]
    pub async fn flush_and_sync2<M1: Message, M2: Message>(&self) {
        self.flush2::<M1, M2>().await;
        self.sync2::<M1, M2>().await;
    }
    fn try_reserve(&self, tt: &TypeTag, rs: &[Receiver]) -> Option<SmallVec<[Permit; 32]>> {
        let mut permits = SmallVec::<[Permit; 32]>::new();

        for r in rs {
            if let Some(prmt) = r.try_reserve(tt) {
                permits.push(prmt);
            } else {
                return None;
            };
        }

        Some(permits)
    }

    #[inline]
    pub fn try_send<M: Message + Clone>(&self, msg: M) -> Result<(), Error<M>> {
        self.try_send_ext(msg, SendOptions::Broadcast)
    }

    pub fn try_send_ext<M: Message + Clone>(
        &self,
        msg: M,
        _options: SendOptions,
    ) -> core::result::Result<(), Error<M>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        if let Some(rs) = self.inner.lookup.get(&(msg.type_tag(), None, None)) {
            let permits = if let Some(x) = self.try_reserve(&tt, rs) {
                x
            } else {
                return Err(SendError::Full(msg).into());
            };

            let mut iter = permits.into_iter().zip(rs.iter());
            let mut counter = 1;
            let total = rs.len();

            while counter < total {
                let (p, r) = iter.next().unwrap();
                let _ = r.send(self, mid, msg.clone(), false, p);

                counter += 1;
            }

            if let Some((p, r)) = iter.next() {
                let _ = r.send(self, mid, msg, false, p);
                return Ok(());
            }
        }

        warn!(
            "Unhandled message {:?}: no receivers",
            core::any::type_name::<M>()
        );

        Ok(())
    }

    #[inline]
    pub fn send_blocking<M: Message + Clone>(&self, msg: M) -> Result<(), Error<M>> {
        self.send_blocking_ext(msg, SendOptions::Broadcast)
    }

    #[inline]
    pub fn send_blocking_ext<M: Message + Clone>(
        &self,
        msg: M,
        options: SendOptions,
    ) -> core::result::Result<(), Error<M>> {
        futures::executor::block_on(self.send_ext(msg, options))
    }

    #[inline]
    pub async fn send<M: Message + Clone>(&self, msg: M) -> core::result::Result<(), Error<M>> {
        Ok(self.send_ext(msg, SendOptions::Broadcast).await?)
    }

    pub async fn send_ext<M: Message + Clone>(
        &self,
        msg: M,
        _options: SendOptions,
    ) -> core::result::Result<(), Error<M>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        if let Some(rs) = self.inner.lookup.get(&(msg.type_tag(), None, None)) {
            if let Some((last, head)) = rs.split_last() {
                for r in head {
                    let _ = r.send(self, mid, msg.clone(), false, r.reserve(&tt).await);
                }

                let _ = last.send(self, mid, msg, false, last.reserve(&tt).await);

                return Ok(());
            }
        }

        warn!(
            "Unhandled message {:?}: no receivers",
            core::any::type_name::<M>()
        );

        Ok(())
    }

    #[inline]
    pub fn force_send<M: Message + Clone>(&self, msg: M) -> Result<(), Error<M>> {
        self.force_send_ext(msg, SendOptions::Broadcast)
    }

    pub fn force_send_ext<M: Message + Clone>(
        &self,
        msg: M,
        _options: SendOptions,
    ) -> core::result::Result<(), Error<M>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg).into());
        }

        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        if let Some(rs) = self.inner.lookup.get(&(msg.type_tag(), None, None)) {
            if let Some((last, head)) = rs.split_last() {
                for r in head {
                    let _ = r.force_send(self, mid, msg.clone(), false);
                }

                let _ = last.force_send(self, mid, msg, false);

                return Ok(());
            }
        }

        warn!(
            "Unhandled message {:?}: no receivers",
            core::any::type_name::<M>()
        );

        Ok(())
    }

    #[inline]
    pub fn try_send_one<M: Message>(&self, msg: M) -> Result<(), Error<M>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        if let Some(rs) = self
            .inner
            .lookup
            .get(&(msg.type_tag(), None, None))
            .and_then(|rs| rs.first())
        {
            let permits = if let Some(x) = rs.try_reserve(&tt) {
                x
            } else {
                return Err(SendError::Full(msg).into());
            };

            Ok(rs.send(self, mid, msg, false, permits)?)
        } else {
            Err(Error::NoReceivers)
        }
    }

    pub async fn send_one<M: Message>(&self, msg: M) -> Result<(), Error<M>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        if let Some(rs) = self
            .inner
            .lookup
            .get(&(msg.type_tag(), None, None))
            .and_then(|rs| rs.first())
        {
            Ok(rs.send(self, mid, msg, false, rs.reserve(&tt).await)?)
        } else {
            Err(Error::NoReceivers)
        }
    }

    #[inline]
    pub fn send_one_blocking<M: Message>(&self, msg: M) -> Result<(), Error<M>> {
        futures::executor::block_on(self.send_one(msg))
    }

    pub async fn request<M: Message, R: Message>(
        &self,
        req: M,
        options: SendOptions,
    ) -> Result<R, Error<M>> {
        let tid = M::type_tag_();
        let rid = R::type_tag_();

        let mut iter = self.select_receivers(tid.clone(), options, Some(rid), None, true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc
                .add_response_waiter::<R>()
                .map_err(|x| x.specify::<M>())?;

            let mid = mid | 1 << (u64::BITS - 1);

            rc.send(self, mid, req, true, rc.reserve(&tid).await)?;
            rx.await.map_err(|x| x.specify::<M>())
        } else {
            Err(Error::NoReceivers)
        }
    }

    pub async fn request_we<M, R, E>(&self, req: M, options: SendOptions) -> Result<R, Error<M, E>>
    where
        M: Message,
        R: Message,
        E: StdSyncSendError,
    {
        let tid = M::type_tag_();
        let rid = R::type_tag_();
        let eid = E::type_tag_();

        let mut iter = self.select_receivers(tid.clone(), options, Some(rid), Some(eid), true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc.add_response_waiter_we::<R, E>().map_err(|x| {
                x.map_err(|_| unimplemented!())
                    .map_msg(|_| unimplemented!())
            })?;

            rc.send(
                self,
                mid | 1 << (u64::BITS - 1),
                req,
                true,
                rc.reserve(&tid).await,
            )
            .map_err(|x| x.map_err(|_| unimplemented!()))?;

            rx.await.map_err(|x| x.specify::<M>())
        } else {
            Err(Error::NoReceivers)
        }
    }

    pub async fn send_boxed(
        &self,
        msg: Box<dyn Message>,
        options: SendOptions,
    ) -> Result<(), Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        let mut iter = self.select_receivers(tt.clone(), options, None, None, false);
        let first = iter.next();

        for r in iter {
            let _ = r.send_boxed(
                self,
                mid,
                msg.try_clone_boxed().unwrap(),
                false,
                r.reserve(&tt).await,
            );
        }

        if let Some(r) = first {
            let _ = r.send_boxed(
                self,
                mid,
                msg.try_clone_boxed().unwrap(),
                false,
                r.reserve(&tt).await,
            );
        } else {
            warn!("Unhandled message: no receivers");
        }

        Ok(())
    }

    pub async fn send_boxed_one(
        &self,
        msg: Box<dyn Message>,
        options: SendOptions,
    ) -> Result<(), Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        let mut iter = self.select_receivers(tt.clone(), options, None, None, false);
        if let Some(rs) = iter.next() {
            Ok(rs.send_boxed(self, mid, msg, false, rs.reserve(&tt).await)?)
        } else {
            Err(Error::NoReceivers)
        }
    }

    pub async fn request_boxed(
        &self,
        req: Box<dyn Message>,
        options: SendOptions,
    ) -> Result<Box<dyn Message>, Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(req).into());
        }

        let tt = req.type_tag();

        let mut iter = self.select_receivers(tt.clone(), options, None, None, true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc.add_response_waiter_boxed().map_err(|x| {
                x.map_err(|_| unimplemented!())
                    .map_msg(|_| unimplemented!())
            })?;

            rc.send_boxed(
                self,
                mid | 1 << (usize::BITS - 1),
                req,
                true,
                rc.reserve(&tt).await,
            )?;

            rx.await.map_err(|x| x.specify::<Box<dyn Message>>())
        } else {
            Err(Error::NoReceivers)
        }
    }

    pub async fn request_boxed_we<E: StdSyncSendError>(
        &self,
        req: Box<dyn Message>,
        options: SendOptions,
    ) -> Result<Box<dyn Message>, Error<Box<dyn Message>, E>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(req).into());
        }

        let tt = req.type_tag();
        let eid = E::type_tag_();

        let mut iter = self.select_receivers(tt.clone(), options, None, Some(eid), true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc.add_response_waiter_boxed_we().map_err(|x| {
                x.map_err(|_| unimplemented!())
                    .map_msg(|_| unimplemented!())
            })?;

            rc.send_boxed(
                self,
                mid | 1 << (usize::BITS - 1),
                req,
                true,
                rc.reserve(&tt).await,
            )
            .map_err(|x| x.map_err(|_| unimplemented!()))?;

            rx.await.map_err(|x| x.specify::<Box<dyn Message>>())
        } else {
            Err(Error::NoReceivers)
        }
    }

    pub async fn send_deserialize_one<'a, 'b: 'a, 'c: 'a>(
        &'a self,
        tt: TypeTag,
        de: &'b mut dyn erased_serde::Deserializer<'c>,
        _options: SendOptions,
    ) -> Result<(), Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            warn!("closed message bus");
            return Err(Error::NoResponse);
        }

        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        if let Some(rs) = self
            .inner
            .lookup
            .get(&(tt.clone(), None, None))
            .and_then(|rs| rs.first())
        {
            let msg = deserialize_shared_message(tt.clone(), de)?;

            Ok(rs.send_boxed(self, mid, msg.upcast_box(), false, rs.reserve(&tt).await)?)
        } else {
            Err(Error::NoReceivers)
        }
    }

    pub async fn request_deserialize<'a, 'b: 'a, 'c: 'a>(
        &'a self,
        tt: TypeTag,
        de: &'b mut dyn erased_serde::Deserializer<'c>,
        options: SendOptions,
    ) -> Result<Box<dyn Message>, Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            warn!("closed message bus");
            return Err(Error::NoResponse);
        }

        let mut iter = self.select_receivers(tt.clone(), options, None, None, true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc.add_response_waiter_boxed().unwrap();
            let msg = deserialize_shared_message(tt.clone(), de)?;

            rc.send_boxed(
                self,
                mid | 1 << (usize::BITS - 1),
                msg.upcast_box(),
                true,
                rc.reserve(&tt).await,
            )?;

            rx.await.map_err(|x| x.specify::<Box<dyn Message>>())
        } else {
            Err(Error::NoReceivers)
        }
    }

    pub fn stats(&self) -> impl Iterator<Item = Stats> + '_ {
        self.inner.receivers.iter().map(|x| x.stats())
    }

    #[inline]
    fn select_receivers(
        &self,
        tid: TypeTag,
        options: SendOptions,
        rid: Option<TypeTag>,
        eid: Option<TypeTag>,
        is_req: bool,
    ) -> impl Iterator<Item = &Receiver> + '_ {
        self.inner
            .lookup
            .get(&(tid.clone(), rid.clone(), eid.clone()))
            .into_iter()
            .flatten()
            .filter(move |r| r.accept(is_req, &tid, rid.as_ref(), eid.as_ref()))
            .filter(move |r| match options {
                SendOptions::Except(id) => id != r.id(),
                SendOptions::Direct(id) => id == r.id(),
                _ => true,
            })
    }
}
