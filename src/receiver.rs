use crate::{
    envelop::{IntoBoxedMessage, TypeTag},
    error::{SendError, StdSyncSendError},
    trait_object::TraitObject,
    Bus, Error, Message,
};
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
    E: StdSyncSendError,
{
    fn poll_events(&self, ctx: &mut Context<'_>) -> Poll<Event<M, E>>;
}

pub trait WrapperReturnTypeOnly<R: Message>: Send + Sync {
    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<R, Error>>,
    ) -> Result<usize, Error>;
}

pub trait WrapperReturnTypeAndError<R: Message, E: StdSyncSendError>: Send + Sync {
    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<R, Error<(), E>>>,
    ) -> Result<usize, Error>;
    fn response(&self, mid: u64, resp: Result<R, Error<(), E>>) -> Result<(), Error>;
}

pub trait ReceiverTrait: Send + Sync {
    fn typed(&self) -> AnyReceiver<'_>;
    fn wrapper(&self) -> AnyWrapper<'_>;
    fn name(&self) -> &str;
    fn send_boxed(&self, mid: u64, msg: Box<dyn Message>) -> Result<(), Error<Box<dyn Message>>>;
    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<Box<dyn Message>, Error>>,
    ) -> Result<usize, Error>;
    fn stats(&self) -> Result<(), Error<Action>>;
    fn close(&self) -> Result<(), Error<Action>>;
    fn sync(&self) -> Result<(), Error<Action>>;
    fn flush(&self) -> Result<(), Error<Action>>;
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
#[derive(Debug, Clone)]
pub enum Action {
    Flush,
    Sync,
    Close,
    Stats,
}

#[non_exhaustive]
#[derive(Debug)]
pub enum Event<M, E: StdSyncSendError> {
    Response(u64, Result<M, Error<(), E>>),
    Synchronized(Result<(), Error<(), E>>),
    Stats(Stats),
    Flushed,
    Exited,
}

struct ReceiverWrapper<M, R, E, S>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    S: 'static,
{
    inner: S,
    waiters: Slab<Waiter<R, E>>,
    _m: PhantomData<(M, R, E)>,
}

impl<M, R, E, S> WrapperReturnTypeAndError<R, E> for ReceiverWrapper<M, R, E, S>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    S: Send + Sync + 'static,
{
    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<R, Error<(), E>>>,
    ) -> Result<usize, Error> {
        Ok(self
            .waiters
            .insert(Waiter::WithErrorType(listener))
            .ok_or_else(|| Error::AddListenerError)?)
    }

    fn response(&self, mid: u64, resp: Result<R, Error<(), E>>) -> Result<(), Error> {
        if let Some(waiter) = self.waiters.take(mid as _) {
            match waiter {
                Waiter::WithErrorType(sender) => sender.send(resp).unwrap(),
                Waiter::WithoutErrorType(sender) => {
                    sender.send(resp.map_err(|e| e.into_dyn())).unwrap()
                }
                Waiter::Boxed(sender) => sender
                    .send(resp.map_err(|e| e.into_dyn()).map(|x| x.into_boxed()))
                    .unwrap(),
            }
        }

        Ok(())
    }
}

impl<M, R, E, S> WrapperReturnTypeOnly<R> for ReceiverWrapper<M, R, E, S>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    S: Send + Sync + 'static,
{
    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<R, Error>>,
    ) -> Result<usize, Error> {
        Ok(self
            .waiters
            .insert(Waiter::WithoutErrorType(listener))
            .ok_or_else(|| Error::AddListenerError)?)
    }
}

impl<M, R, E, S> ReceiverTrait for ReceiverWrapper<M, R, E, S>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    S: SendUntypedReceiver + SendTypedReceiver<M> + ReciveTypedReceiver<R, E> + 'static,
{
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    fn typed(&self) -> AnyReceiver<'_> {
        AnyReceiver::new(&self.inner)
    }

    fn wrapper(&self) -> AnyWrapper<'_> {
        AnyWrapper::new(self)
    }

    fn send_boxed(
        &self,
        mid: u64,
        boxed_msg: Box<dyn Message>,
    ) -> Result<(), Error<Box<dyn Message>>> {
        let boxed = boxed_msg
            .as_any_boxed()
            .downcast::<M>()
            .map_err(|_| Error::MessageCastError)?;

        Ok(SendTypedReceiver::send(&self.inner, mid, *boxed)
            .map_err(|err| Error::from(err.into_boxed()))?)
    }

    fn stats(&self) -> Result<(), Error<Action>> {
        Ok(SendUntypedReceiver::send(&self.inner, Action::Stats)?)
    }

    fn close(&self) -> Result<(), Error<Action>> {
        Ok(SendUntypedReceiver::send(&self.inner, Action::Close)?)
    }

    fn sync(&self) -> Result<(), Error<Action>> {
        Ok(SendUntypedReceiver::send(&self.inner, Action::Sync)?)
    }

    fn flush(&self) -> Result<(), Error<Action>> {
        Ok(SendUntypedReceiver::send(&self.inner, Action::Flush)?)
    }

    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<Box<dyn Message>, Error>>,
    ) -> Result<usize, Error> {
        Ok(self
            .waiters
            .insert(Waiter::Boxed(listener))
            .ok_or_else(|| Error::AddListenerError)?)
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
    data: *mut (),
    typed: (TypeId, *mut ()),
    poller: (TypeId, *mut ()),
    _m: PhantomData<&'a usize>,
}

impl<'a> AnyReceiver<'a> {
    pub fn new<M, R, E, S>(rcvr: &'a S) -> Self
    where
        M: Message,
        R: Message,
        E: StdSyncSendError,
        S: SendTypedReceiver<M> + ReciveTypedReceiver<R, E> + 'static,
    {
        let send_typed_receiver = rcvr as &(dyn SendTypedReceiver<M>);
        let recive_typed_receiver = rcvr as &(dyn ReciveTypedReceiver<R, E>);

        let send_typed_receiver: TraitObject = unsafe { mem::transmute(send_typed_receiver) };
        let recive_typed_receiver: TraitObject = unsafe { mem::transmute(recive_typed_receiver) };

        Self {
            data: send_typed_receiver.data,
            typed: (
                TypeId::of::<dyn SendTypedReceiver<M>>(),
                send_typed_receiver.vtable,
            ),
            poller: (
                TypeId::of::<dyn ReciveTypedReceiver<R, E>>(),
                recive_typed_receiver.vtable,
            ),
            _m: Default::default(),
        }
    }

    pub fn cast_send_typed<M: Message>(&'a self) -> &'a dyn SendTypedReceiver<M> {
        assert_eq!(self.typed.0, TypeId::of::<dyn SendTypedReceiver<M>>());

        unsafe {
            mem::transmute(TraitObject {
                data: self.data,
                vtable: self.typed.1,
            })
        }
    }

    pub fn cast_recive_typed<R: Message, E: StdSyncSendError>(
        &'a self,
    ) -> &'a dyn ReciveTypedReceiver<R, E> {
        assert_eq!(self.poller.0, TypeId::of::<dyn ReciveTypedReceiver<R, E>>());

        unsafe {
            mem::transmute(TraitObject {
                data: self.data,
                vtable: self.poller.1,
            })
        }
    }
}

unsafe impl Send for AnyReceiver<'_> {}

pub struct AnyWrapper<'a> {
    data: *mut (),
    wrapper_r: (TypeId, *mut ()),
    wrapper_re: (TypeId, *mut ()),
    _m: PhantomData<&'a usize>,
}

impl<'a> AnyWrapper<'a> {
    pub fn new<R, E, S>(rcvr: &'a S) -> Self
    where
        R: Message,
        E: StdSyncSendError,
        S: WrapperReturnTypeOnly<R> + WrapperReturnTypeAndError<R, E> + 'static,
    {
        let wrapper_r = rcvr as &(dyn WrapperReturnTypeOnly<R>);
        let wrapper_re = rcvr as &(dyn WrapperReturnTypeAndError<R, E>);

        let wrapper_r: TraitObject = unsafe { mem::transmute(wrapper_r) };
        let wrapper_re: TraitObject = unsafe { mem::transmute(wrapper_re) };

        Self {
            data: wrapper_r.data,
            wrapper_r: (
                TypeId::of::<dyn WrapperReturnTypeOnly<R>>(),
                wrapper_r.vtable,
            ),
            wrapper_re: (
                TypeId::of::<dyn WrapperReturnTypeAndError<R, E>>(),
                wrapper_re.vtable,
            ),
            _m: Default::default(),
        }
    }

    pub fn cast_ret_only<R: Message>(&'a self) -> &'a dyn WrapperReturnTypeOnly<R> {
        assert_eq!(
            self.wrapper_r.0,
            TypeId::of::<dyn WrapperReturnTypeOnly<R>>()
        );

        unsafe {
            mem::transmute(TraitObject {
                data: self.data,
                vtable: self.wrapper_r.1,
            })
        }
    }

    pub fn cast_ret_and_error<R: Message, E: StdSyncSendError>(
        &'a self,
    ) -> &'a dyn WrapperReturnTypeAndError<R, E> {
        assert_eq!(
            self.wrapper_re.0,
            TypeId::of::<dyn WrapperReturnTypeAndError<R, E>>()
        );

        unsafe {
            mem::transmute(TraitObject {
                data: self.data,
                vtable: self.wrapper_re.1,
            })
        }
    }
}

unsafe impl Send for AnyWrapper<'_> {}

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
    resp_type_tag: TypeTag,
    err_type_tag: TypeTag,
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

enum Waiter<R: Message, E: StdSyncSendError> {
    WithErrorType(oneshot::Sender<Result<R, Error<(), E>>>),
    WithoutErrorType(oneshot::Sender<Result<R, Error>>),
    Boxed(oneshot::Sender<Result<Box<dyn Message>, Error>>),
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
    pub(crate) fn new<M, R, E, S>(limit: u64, inner: S) -> Self
    where
        M: Message,
        R: Message,
        E: StdSyncSendError,
        S: SendUntypedReceiver + SendTypedReceiver<M> + ReciveTypedReceiver<R, E> + 'static,
    {
        Self {
            context: Arc::new(ReceiverContext {
                resp_type_tag: R::type_tag_(),
                err_type_tag: E::type_tag_(),
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
                waiters: sharded_slab::Slab::<Waiter<R, E>>::new_with_config::<SlabCfg>(),
                _m: Default::default(),
            }),
        }
    }

    #[inline]
    pub fn resp_type_tag(&self) -> &TypeTag {
        &self.context.resp_type_tag
    }

    #[inline]
    pub fn err_type_tag(&self) -> &TypeTag {
        &self.context.err_type_tag
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
        msg: M,
        mut permit: Permit,
    ) -> Result<(), SendError<M>> {
        let any_receiver = self.inner.typed();
        let receiver = any_receiver.cast_send_typed::<M>();
        let res = receiver.send(mid, msg);
        permit.fuse = true;

        if !res.is_err() {
            self.context.need_flush.store(true, Ordering::SeqCst);
        }

        res
    }

    #[inline]
    pub fn force_send<M: Message + Clone>(&self, mid: u64, msg: M) -> Result<(), SendError<M>> {
        let any_receiver = self.inner.typed();
        let receiver = any_receiver.cast_send_typed::<M>();
        let res = receiver.send(mid, msg);
        self.context.processing.fetch_add(1, Ordering::SeqCst);

        if !res.is_err() {
            self.context.need_flush.store(true, Ordering::SeqCst);
        }

        res
    }

    pub fn send_boxed(
        &self,
        mid: u64,
        msg: Box<dyn Message>,
        mut permit: Permit,
    ) -> Result<(), Error<Box<dyn Message>>> {
        self.context.processing.fetch_add(1, Ordering::SeqCst);
        let res = self.inner.send_boxed(mid, msg);
        permit.fuse = true;

        if !res.is_err() {
            self.context.need_flush.store(true, Ordering::SeqCst);
        }

        Ok(())
    }

    pub fn start_polling_events<R, E>(
        &self,
    ) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
    where
        R: Message,
        E: StdSyncSendError,
    {
        let ctx_clone = self.context.clone();
        let inner_clone = self.inner.clone();

        Box::new(move |_| {
            Box::pin(async move {
                let any_receiver = inner_clone.typed();
                let receiver = any_receiver.cast_recive_typed::<R, E>();

                let any_wrapper = inner_clone.wrapper();
                let wrapper = any_wrapper.cast_ret_and_error::<R, E>();

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

                            if let Err(err) = wrapper.response(mid, resp) {
                                error!("Response error: {}", err);
                            }
                        }

                        _ => unimplemented!(),
                    }
                }
            })
        })
    }

    #[inline]
    pub(crate) fn add_response_waiter_boxed(
        &self,
        waiter: oneshot::Sender<Result<Box<dyn Message>, Error>>,
    ) -> Result<usize, Error> {
        self.inner.add_response_listener(waiter)
    }

    #[inline]
    pub(crate) fn add_response_waiter<R: Message>(
        &self,
        waiter: oneshot::Sender<Result<R, Error>>,
    ) -> Result<usize, Error> {
        let any_receiver = self.inner.wrapper();
        let receiver = any_receiver.cast_ret_only::<R>();

        receiver.add_response_listener(waiter)
    }

    #[inline]
    pub(crate) fn add_response_waiter_we<R: Message, E: StdSyncSendError>(
        &self,
        waiter: oneshot::Sender<Result<R, Error<(), E>>>,
    ) -> Result<usize, Error> {
        let any_receiver = self.inner.wrapper();
        let receiver = any_receiver.cast_ret_and_error::<R, E>();

        receiver.add_response_listener(waiter)
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
