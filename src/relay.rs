use std::{
    any::TypeId,
    borrow::Cow,
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

use sharded_slab::Slab;
use tokio::sync::oneshot::Sender;

use crate::{
    error::{Error, SendError},
    receiver::Permit,
    Bus, Message,
};

pub trait RelayTrait {
    type Context;
    // fn handle_message(&self, mid: u64, msg: &dyn SafeMessage, ctx: Self::Context, bus: &Bus);
    // fn handle_request(&self, mid: u64, msg: &dyn SafeMessage, ctx: Self::Context, bus: &Bus);
    fn start_relay(&self, bus: &Bus) -> Result<Self::Context, Error>;
    fn stop_relay(&self, ctx: Self::Context);
}

pub struct Relay {
    in_map: HashMap<Cow<'static, str>, TypeId>,
    out_map: HashMap<TypeId, Cow<'static, str>>,
    // waiters: Slab<Sender<Result<R, Error<(), E>>>>,
    queue_size: AtomicU64,
    limit: u64,
}

impl Relay {
    // pub async fn reserve(&self) -> Permit {
    //     loop {
    //         let count = self.queue_size.load(Ordering::Relaxed);
    //         if count < self.limit {
    //             let res = self.processing.compare_exchange(
    //                 count,
    //                 count + 1,
    //                 Ordering::SeqCst,
    //                 Ordering::SeqCst,
    //             );
    //             if res.is_ok() {
    //                 break Permit {
    //                     fuse: false,
    //                     inner: self.context.clone(),
    //                 };
    //             }

    //             // continue
    //         } else {
    //             self.response.notified().await
    //         }
    //     }
    // }

    // pub fn try_reserve(&self) -> Option<Permit> {
    //     loop {
    //         let count = self.processing.load(Ordering::Relaxed);

    //         if count < self.limit {
    //             let res = self.processing.compare_exchange(
    //                 count,
    //                 count + 1,
    //                 Ordering::SeqCst,
    //                 Ordering::SeqCst,
    //             );
    //             if res.is_ok() {
    //                 break Some(Permit {
    //                     fuse: false,
    //                     inner: self.context.clone(),
    //                 });
    //             }

    //             // continue
    //         } else {
    //             break None;
    //         }
    //     }
    // }

    // #[inline]
    // pub fn send<M: Message>(
    //     &self,
    //     mid: u64,
    //     msg: M,
    //     mut permit: Permit,
    // ) -> Result<(), SendError<M>> {
    //     unimplemented!()
    // }

    // #[inline]
    // pub fn force_send<M: Message + Clone>(&self, mid: u64, msg: M) -> Result<(), SendError<M>> {
    //     unimplemented!()
    // }

    // #[inline]
    // pub fn need_flush(&self) -> bool {
    //     self.context.need_flush.load(Ordering::SeqCst)
    // }

    // #[inline]
    // pub async fn close(&self) {
    //     let notified = self.context.closed.notified();
    //     if self.inner.close().is_ok() {
    //         notified.await;
    //     } else {
    //         warn!("close failed!");
    //     }
    // }

    // #[inline]
    // pub async fn sync(&self) {
    //     let notified = self.context.synchronized.notified();
    //     if self.inner.sync().is_ok() {
    //         notified.await
    //     } else {
    //         warn!("sync failed!");
    //     }
    // }

    // #[inline]
    // pub async fn flush(&self) {
    //     let notified = self.context.flushed.notified();
    //     if self.inner.flush().is_ok() {
    //         notified.await;
    //         self.context.need_flush.store(false, Ordering::SeqCst);
    //     } else {
    //         warn!("flush failed!");
    //     }
    // }
}
