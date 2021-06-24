use crate::{msgs, trait_object::TraitObject, Bus, Error, Message};
use core::{
    any::TypeId,
    fmt,
    marker::PhantomData,
    mem,
    pin::Pin,
    task::{Context, Poll},
};
use futures::future::poll_fn;
use futures::Future;
use std::{
    any::Any,
    borrow::Cow,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::{oneshot, Notify};

struct SlabCfg;
impl sharded_slab::Config for SlabCfg {
    const RESERVED_BITS: usize = 1;
}

type Slab<T> = sharded_slab::Slab<T, SlabCfg>;

pub trait SendUntypedReceiver: Send + Sync {
    fn send(&self, msg: Action) -> Result<(), SendError<Action>>;
}

pub trait SendTypedReceiver<M: Message>: Sync {
    fn send(&self, mid: u64, msg: M) -> Result<(), SendError<M>>;
}

pub trait ReciveTypedReceiver<M, E>: Sync
where
    M: Message,
    E: crate::Error,
{
    fn poll_events(&self, ctx: &mut Context<'_>) -> Poll<Event<M, E>>;
}

pub trait ReceiverTrait: Send + Sync {
    fn typed(&self) -> AnyReceiver<'_>;
    fn poller(&self) -> AnyPoller<'_>;
    fn name(&self) -> &str;
    fn stats(&self) -> Result<(), SendError<()>>;
    fn close(&self) -> Result<(), SendError<()>>;
    fn sync(&self) -> Result<(), SendError<()>>;
    fn flush(&self) -> Result<(), SendError<()>>;
}

pub trait ReceiverPollerBuilder {
    fn build(bus: Bus) -> Box<dyn Future<Output = ()>>;
}

pub trait PermitDrop {
    fn permit_drop(&self);
}

#[derive(Debug, Clone)]
pub struct Stats {
    pub has_queue: bool,
    pub queue_capacity: u64,
    pub queue_size: u64,

    pub has_parallel: bool,
    pub parallel_capacity: u64,
    pub parallel_size: u64,

    pub has_batch: bool,
    pub batch_capacity: u64,
    pub batch_size: u64,
}

#[non_exhaustive]
#[derive(Debug)]
pub enum Action {
    Flush,
    Sync,
    Close,
    Stats,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum Event<M, E> {
    Response(u64, Result<M, E>),
    Synchronized(Result<(), E>),
    Stats(Stats),
    Flushed,
    Exited,
}

struct ReceiverWrapper<M, R, E, S>
where
    M: Message,
    R: Message,
    E: Error,
    S: 'static,
{
    inner: S,
    _m: PhantomData<(M, R, E)>,
}

impl<M, R, E, S> ReceiverTrait for ReceiverWrapper<M, R, E, S>
where
    M: Message,
    R: Message,
    E: Error,
    S: SendUntypedReceiver + SendTypedReceiver<M> + ReciveTypedReceiver<R, E> + 'static,
{
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    fn typed(&self) -> AnyReceiver<'_> {
        AnyReceiver::new(&self.inner)
    }

    fn poller(&self) -> AnyPoller<'_> {
        AnyPoller::new(&self.inner)
    }

    fn stats(&self) -> Result<(), SendError<()>> {
        SendUntypedReceiver::send(&self.inner, Action::Stats).map_err(|_| SendError::Closed(()))
    }

    fn close(&self) -> Result<(), SendError<()>> {
        SendUntypedReceiver::send(&self.inner, Action::Close).map_err(|_| SendError::Closed(()))
    }

    fn sync(&self) -> Result<(), SendError<()>> {
        SendUntypedReceiver::send(&self.inner, Action::Sync).map_err(|_| SendError::Closed(()))
    }

    fn flush(&self) -> Result<(), SendError<()>> {
        SendUntypedReceiver::send(&self.inner, Action::Flush).map_err(|_| SendError::Closed(()))
    }
}

pub struct Permit {
    pub(crate) fuse: bool,
    pub(crate) inner: Arc<dyn PermitDrop>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        if !self.fuse {
            self.inner.permit_drop();
        }
    }
}

pub struct AnyReceiver<'a> {
    dyn_typed_receiver_trait_object: TraitObject,
    type_id: TypeId,
    _m: PhantomData<&'a usize>,
}

unsafe impl Send for AnyReceiver<'_> {}

impl<'a> AnyReceiver<'a> {
    pub fn new<M: Message, R: SendTypedReceiver<M> + 'static>(rcvr: &'a R) -> Self {
        let trcvr = rcvr as &(dyn SendTypedReceiver<M>);

        Self {
            dyn_typed_receiver_trait_object: unsafe { mem::transmute(trcvr) },
            type_id: TypeId::of::<dyn SendTypedReceiver<M>>(),
            _m: Default::default(),
        }
    }

    pub fn dyn_typed_receiver<M: Message>(&'a self) -> &'a dyn SendTypedReceiver<M> {
        assert_eq!(self.type_id, TypeId::of::<dyn SendTypedReceiver<M>>());

        unsafe { mem::transmute(self.dyn_typed_receiver_trait_object) }
    }
}

pub struct AnyPoller<'a> {
    dyn_typed_receiver_trait_object: TraitObject,
    type_id: TypeId,
    _m: PhantomData<&'a usize>,
}

unsafe impl Send for AnyPoller<'_> {}

impl<'a> AnyPoller<'a> {
    pub fn new<M, E, R>(rcvr: &'a R) -> Self
    where
        M: Message,
        E: crate::Error,
        R: ReciveTypedReceiver<M, E> + 'static,
    {
        let trcvr = rcvr as &(dyn ReciveTypedReceiver<M, E>);

        Self {
            dyn_typed_receiver_trait_object: unsafe { mem::transmute(trcvr) },
            type_id: TypeId::of::<dyn ReciveTypedReceiver<M, E>>(),
            _m: Default::default(),
        }
    }

    pub fn dyn_typed_receiver<M: Message, E: crate::Error>(
        &'a self,
    ) -> &'a dyn ReciveTypedReceiver<M, E> {
        assert_eq!(self.type_id, TypeId::of::<dyn ReciveTypedReceiver<M, E>>());

        unsafe { mem::transmute(self.dyn_typed_receiver_trait_object) }
    }
}

pub enum SendError<M> {
    Full(M),
    Closed(M),
}

impl<M: fmt::Debug> fmt::Debug for SendError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Full(m) => write!(f, "SendError::Full({:?})", m)?,
            SendError::Closed(m) => write!(f, "SendError::Closed({:?})", m)?,
        }

        Ok(())
    }
}

impl<M: fmt::Debug> fmt::Display for SendError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Full(m) => write!(f, "SendError::Full({:?})", m)?,
            SendError::Closed(m) => write!(f, "SendError::Closed({:?})", m)?,
        }

        Ok(())
    }
}

impl<M: fmt::Debug> std::error::Error for SendError<M> {}

#[derive(Debug, Clone)]
pub struct ReceiverStats {
    pub name: Cow<'static, str>,
    pub fields: Vec<(Cow<'static, str>, u64)>,
}

impl fmt::Display for ReceiverStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "-- {}: {{ ", self.name)?;

        for (idx, (k, v)) in self.fields.iter().enumerate() {
            if idx != 0 {
                write!(f, ", ")?;
            }

            write!(f, "{}: {}", k, v)?;
        }

        write!(f, " }}")?;
        Ok(())
    }
}

struct ReceiverContext {
    resp_type_id: TypeId,
    limit: u64,
    processing: AtomicU64,
    need_flush: AtomicBool,
    flushed: Notify,
    synchronized: Notify,
    closed: Notify,
    response: Notify,
}

impl PermitDrop for ReceiverContext {
    fn permit_drop(&self) {
        self.processing.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct Receiver {
    inner: Arc<dyn ReceiverTrait>,
    context: Arc<ReceiverContext>,
    waiters: Arc<dyn Any + Send + Sync>,
}

impl fmt::Debug for Receiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Receiver({:?})", self.inner.type_id())?;
        Ok(())
    }
}

impl core::cmp::PartialEq for Receiver {
    fn eq(&self, other: &Receiver) -> bool {
        self.inner.type_id() == other.inner.type_id()
    }
}

impl core::cmp::Eq for Receiver {}

impl Receiver {
    #[inline]
    pub(crate) fn new<M, R, E, S>(limit: u64, inner: S) -> Self
    where
        M: Message,
        R: Message,
        E: Error,
        S: SendUntypedReceiver + SendTypedReceiver<M> + ReciveTypedReceiver<R, E> + 'static,
    {
        Self {
            context: Arc::new(ReceiverContext {
                resp_type_id: TypeId::of::<R>(),
                limit,
                processing: AtomicU64::new(0),
                need_flush: AtomicBool::new(false),
                flushed: Notify::new(),
                synchronized: Notify::new(),
                closed: Notify::new(),
                response: Notify::new(),
            }),
            inner: Arc::new(ReceiverWrapper {
                inner,
                _m: Default::default(),
            }),
            waiters: Arc::new(sharded_slab::Slab::<oneshot::Sender<R>>::new_with_config::<
                SlabCfg,
            >()),
        }
    }

    #[inline]
    pub fn resp_type_id(&self) -> TypeId {
        self.context.resp_type_id
    }

    #[inline]
    pub fn need_flush(&self) -> bool {
        self.context.need_flush.load(Ordering::SeqCst)
    }

    pub async fn reserve(&self) -> Permit {
        loop {
            let count = self.context.processing.load(Ordering::Relaxed);
            if count < self.context.limit {
                let res = self.context.processing.compare_exchange(
                    count,
                    count + 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                if res.is_ok() {
                    break Permit {
                        fuse: false,
                        inner: self.context.clone(),
                    };
                }

                // continue
            } else {
                self.context.response.notified().await
            }
        }
    }

    pub fn try_reserve(&self) -> Option<Permit> {
        loop {
            let count = self.context.processing.load(Ordering::Relaxed);

            if count < self.context.limit {
                let res = self.context.processing.compare_exchange(
                    count,
                    count + 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                if res.is_ok() {
                    break Some(Permit {
                        fuse: false,
                        inner: self.context.clone(),
                    });
                }

                // continue
            } else {
                break None;
            }
        }
    }

    #[inline]
    pub fn send<M: Message>(
        &self,
        mid: u64,
        mut permit: Permit,
        msg: M,
    ) -> Result<(), SendError<M>> {
        let any_receiver = self.inner.typed();
        let receiver = any_receiver.dyn_typed_receiver::<M>();
        let res = receiver.send(mid, msg);
        permit.fuse = true;

        if !res.is_err() {
            self.context.need_flush.store(true, Ordering::SeqCst);
        }

        res
    }

    #[inline]
    pub fn force_send<M: Message>(&self, mid: u64, msg: M) -> Result<(), SendError<M>> {
        let any_receiver = self.inner.typed();
        let receiver = any_receiver.dyn_typed_receiver::<M>();
        let res = receiver.send(mid, msg);
        self.context.processing.fetch_add(1, Ordering::SeqCst);

        if !res.is_err() {
            self.context.need_flush.store(true, Ordering::SeqCst);
        }

        res
    }

    pub fn start_polling_events<R, E>(
        &self,
    ) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
    where
        R: Message,
        E: crate::Error,
    {
        let ctx_clone = self.context.clone();
        let inner_clone = self.inner.clone();
        let waiters = self
            .waiters
            .clone()
            .downcast::<Slab<oneshot::Sender<R>>>()
            .unwrap();

        Box::new(move |bus| {
            Box::pin(async move {
                let any_receiver = inner_clone.poller();
                let receiver = any_receiver.dyn_typed_receiver::<R, E>();

                loop {
                    let event = poll_fn(move |ctx| receiver.poll_events(ctx)).await;
                    match event {
                        Event::Exited => {
                            ctx_clone.closed.notify_waiters();
                            break;
                        }
                        Event::Flushed => ctx_clone.flushed.notify_waiters(),
                        Event::Synchronized(_res) => ctx_clone.synchronized.notify_waiters(),
                        Event::Response(mid, resp) => {
                            ctx_clone.processing.fetch_sub(1, Ordering::SeqCst);
                            ctx_clone.response.notify_one();

                            match resp {
                                Ok(msg) => {
                                    if let Some(waiter) = waiters.take(mid as usize) {
                                        if let Err(_msg) = waiter.send(msg) {
                                            error!("Response cannot be processed!");
                                        }
                                    } else if TypeId::of::<R>() != TypeId::of::<()>() {
                                        warn!("Non-void response has no listeners!");
                                    }
                                }
                                Err(err) => {
                                    bus.try_send(msgs::Error(Arc::new(err.into()))).ok();
                                }
                            }
                        }
                        _ => unimplemented!(),
                    }
                }
            })
        })
    }

    #[inline]
    pub(crate) fn add_response_waiter<R: Message>(
        &self,
        waiter: oneshot::Sender<R>,
    ) -> Option<usize> {
        let idx = self
            .waiters
            .downcast_ref::<Slab<oneshot::Sender<R>>>()
            .unwrap()
            .insert(waiter)?;

        Some(idx)
    }

    #[inline]
    pub async fn close(&self) {
        let notified = self.context.closed.notified();
        if self.inner.close().is_ok() {
            notified.await;
        } else {
            warn!("close failed!");
        }
    }

    #[inline]
    pub async fn sync(&self) {
        let notified = self.context.synchronized.notified();
        if self.inner.sync().is_ok() {
            notified.await
        } else {
            warn!("sync failed!");
        }
    }

    #[inline]
    pub async fn flush(&self) {
        let notified = self.context.flushed.notified();
        if self.inner.flush().is_ok() {
            notified.await;
            self.context.need_flush.store(false, Ordering::SeqCst);
        } else {
            warn!("flush failed!");
        }
    }
}
