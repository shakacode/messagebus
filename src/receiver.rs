use crate::{Bus, Message, msgs, receivers::{Event, Permit, PermitDrop}, trait_object::TraitObject};
use core::{any::TypeId, fmt, marker::PhantomData, mem, pin::Pin, task::{Context, Poll}};
use futures::future::poll_fn;
use tokio::sync::Notify;
use std::{borrow::Cow, sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}}};
use futures::Future;


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
            R: ReciveTypedReceiver<M, E> + 'static
    {
        let trcvr = rcvr as &(dyn ReciveTypedReceiver<M, E>);

        Self {
            dyn_typed_receiver_trait_object: unsafe { mem::transmute(trcvr) },
            type_id: TypeId::of::<dyn ReciveTypedReceiver<M, E>>(),
            _m: Default::default(),
        }
    }

    pub fn dyn_typed_receiver<M: Message, E: crate::Error>(&'a self) -> &'a dyn ReciveTypedReceiver<M, E> {
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

pub trait SendTypedReceiver<M: Message>: Sync {
    fn send(&self, mid: u64, msg: M) -> Result<(), SendError<M>>;
}

pub trait ReciveTypedReceiver<M, E>: Sync 
    where M: Message,
          E: crate::Error
{
    fn poll_events(&self, ctx: &mut Context<'_>) -> Poll<Event<M, E>>;
}

pub trait ReceiverTrait: Send + Sync {
    fn typed(&self) -> AnyReceiver<'_>;
    fn poller(&self) -> AnyPoller<'_>;
    fn type_id(&self) -> TypeId;
    fn stats(&self) -> Result<(), SendError<()>>;
    fn close(&self) -> Result<(), SendError<()>>;
    fn sync(&self) -> Result<(), SendError<()>>;
    fn flush(&self) -> Result<(), SendError<()>>;
}

pub trait ReceiverPollerBuilder {
    fn build(bus: Bus) -> Box<dyn Future<Output = ()>>;
}

struct ReceiverContext {
    limit: u64,
    processing: AtomicU64,
    need_flush: AtomicBool,
    flushed: Notify,
    synchronized: Notify,
    closed: Notify,
    response: Notify,
    statistics: Notify,
}

impl PermitDrop for ReceiverContext {
    fn permit_drop(&self) {
        self.processing.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct Receiver {
    inner: Arc<dyn ReceiverTrait>,
    context: Arc<ReceiverContext>,
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
    pub(crate) fn new(limit: u64, inner: Arc<dyn ReceiverTrait>) -> Self {
        let context = Arc::new(ReceiverContext {
            limit,
            processing: AtomicU64::new(0),
            need_flush: AtomicBool::new(false),
            flushed: Notify::new(),
            synchronized: Notify::new(),
            closed: Notify::new(),
            response: Notify::new(),
            statistics: Notify::new(),
        });

        Self { inner, context }
    }

    #[inline]
    pub fn type_id(&self) -> TypeId {
        self.inner.type_id()
    }

    #[inline]
    pub fn need_flush(&self) -> bool {
        self.context.need_flush.load(Ordering::SeqCst)
    }

    #[inline]
    pub async fn reserve(&self) -> Permit {
        loop {
            let count = self.context.processing.load(Ordering::Relaxed);
            if count < self.context.limit {
                let res = self.context.processing.compare_exchange(count, count + 1, Ordering::SeqCst, Ordering::SeqCst);
                if res.is_ok() {
                    break Permit {
                        fuse: false,
                        inner: self.context.clone(),
                    };
                }

                // continue
            } else {
                self.context.response.notified()
                    .await
            }
        }
    }

    #[inline]
    pub fn try_reserve(&self) -> Option<Permit> {
        loop {
            let count = self.context.processing.load(Ordering::Relaxed);
            
            if count < self.context.limit {
                let res = self.context.processing.compare_exchange(count, count + 1, Ordering::SeqCst, Ordering::SeqCst);
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
    pub fn send<M: Message>(&self, mid: u64, mut permit: Permit, msg: M) -> Result<(), SendError<M>> {
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

        if !res.is_err() {
            self.context.need_flush.store(true, Ordering::SeqCst);
        }

        res
    }

    pub fn start_polling_events<M, E>(&self) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>> 
        where 
            M: Message,
            E: crate::Error
    {
        let ctx_clone = self.context.clone();
        let inner_clone = self.inner.clone();

        Box::new(move |bus| Box::pin(async move {
            let any_receiver = inner_clone.poller();
            let receiver = any_receiver.dyn_typed_receiver::<M, E>();

            loop {
                let event = poll_fn(move |ctx| receiver.poll_events(ctx))
                    .await;

                match event {
                    Event::Exited => {
                        ctx_clone.closed.notify_waiters();
                        break;
                    },

                    Event::Flushed => ctx_clone.flushed.notify_waiters(),
                    Event::Synchronized => ctx_clone.synchronized.notify_waiters(),
                    Event::Response(_mid, resp) => {
                        ctx_clone.processing.fetch_sub(1, Ordering::SeqCst);
                        ctx_clone.response.notify_one();

                        match resp {
                            Ok(_msg) => (),
                            Err(err) => { bus.try_send(msgs::Error(Arc::new(err.into()))).ok(); }
                        }
                    },
                    _ => unimplemented!()
                }
            }
        }))
    }

    // #[inline]
    // pub fn stats(&self) -> ReceiverStats {
    //     if self.inner.stats().is_ok() {
    //         self.context.stats.notified()
    //             .await
    //     } else {
    //         warn!("close failed!");
    //     }
    // }

    #[inline]
    pub async fn close(&self) {
        if self.inner.close().is_ok() {
            self.context.closed.notified()
                .await
        } else {
            warn!("close failed!");
        }
    }

    #[inline]
    pub async fn sync(&self) {
        if self.inner.sync().is_ok() {
            self.context.synchronized.notified()
                .await
        } else {
            warn!("sync failed!");
        }
    }

    #[inline]
    pub async fn flush(&self) {
        if self.inner.flush().is_ok() {
            self.context.flushed.notified()
                .await;
                
            self.context.need_flush.store(false, Ordering::SeqCst);
        } else {
            warn!("flush failed!");
        }
    }
}
