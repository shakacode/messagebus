use crate::{trait_object::TraitObject, Bus, Message};
use core::{
    any::TypeId,
    fmt,
    future::Future,
    marker::PhantomData,
    mem,
    pin::Pin,
    task::{Context, Poll},
};
use futures::future::poll_fn;
use std::{borrow::Cow, sync::Arc};

pub struct AnyReceiver<'a> {
    dyn_typed_receiver_trait_object: TraitObject,
    type_id: TypeId,
    _m: PhantomData<&'a usize>,
}

impl<'a> AnyReceiver<'a> {
    pub fn new<M: Message, R: TypedReceiver<M> + 'static>(rcvr: &'a R) -> Self {
        let trcvr = rcvr as &(dyn TypedReceiver<M>);

        Self {
            dyn_typed_receiver_trait_object: unsafe { mem::transmute(trcvr) },
            type_id: TypeId::of::<dyn TypedReceiver<M>>(),
            _m: Default::default(),
        }
    }

    pub fn dyn_typed_receiver<M: Message>(&'a self) -> &'a dyn TypedReceiver<M> {
        assert_eq!(self.type_id, TypeId::of::<dyn TypedReceiver<M>>());

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

pub trait TypedReceiver<M: Message>: Sync {
    fn poll_ready(&self, ctx: &mut Context<'_>) -> Poll<()>;
    fn try_send(&self, msg: M) -> Result<(), SendError<M>>;
}

pub trait ReceiverTrait: Send + Sync {
    fn typed(&self) -> AnyReceiver<'_>;
    fn type_id(&self) -> TypeId;
    fn close(&self);
    fn stats(&self) -> ReceiverStats;
    fn sync(&self);
    fn poll_synchronized(&self, ctx: &mut Context<'_>) -> Poll<()>;
}

pub trait ReceiverPollerBuilder {
    fn build(bus: Bus) -> Box<dyn Future<Output = ()>>;
}

pub struct Receiver {
    inner: Arc<dyn ReceiverTrait>,
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

pub struct ReceiverPoller<'a, M: Message> {
    inner: &'a dyn ReceiverTrait,
    msg: Option<M>,
}

impl<'a, M: Message> Unpin for ReceiverPoller<'a, M> {}
impl<'a, M: Message> Future for ReceiverPoller<'a, M> {
    type Output = Result<(), SendError<M>>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut msg = if let Some(msg) = this.msg.take() {
            msg
        } else {
            return Poll::Ready(Ok(()));
        };

        let any_receiver = this.inner.typed();
        let receiver = any_receiver.dyn_typed_receiver::<M>();

        loop {
            match receiver.poll_ready(ctx) {
                Poll::Ready(_) => (),
                Poll::Pending => {
                    this.msg = Some(msg);
                    return Poll::Pending;
                }
            }

            msg = match receiver.try_send(msg) {
                Ok(_) => break Poll::Ready(Ok(())),
                Err(SendError::Full(m)) => m,
                Err(err) => break Poll::Ready(Err(err)),
            }
        }
    }
}

impl Receiver {
    #[inline]
    pub(crate) fn new(inner: Arc<dyn ReceiverTrait>) -> Self {
        Self { inner }
    }

    #[inline]
    pub fn type_id(&self) -> TypeId {
        self.inner.type_id()
    }

    #[inline]
    pub fn broadcast<M: Message>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<(), SendError<M>>> + '_ {
        ReceiverPoller {
            inner: self.inner.as_ref(),
            msg: Some(msg),
        }
    }

    #[inline]
    pub fn try_broadcast<M: Message>(&self, msg: M) -> Result<(), SendError<M>> {
        let any_receiver = self.inner.typed();
        let receiver = any_receiver.dyn_typed_receiver::<M>();

        receiver.try_send(msg)
    }

    #[inline]
    pub fn close(&self) {
        self.inner.close();
    }

    #[inline]
    pub fn stats(&self) -> ReceiverStats {
        self.inner.stats()
    }

    #[inline]
    pub fn sync(&self) -> impl Future<Output = ()> + '_ {
        self.inner.sync();

        poll_fn(move |ctx| self.inner.poll_synchronized(ctx))
    }
}
