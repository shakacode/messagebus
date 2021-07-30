use crate::relay::RelayWrapper;
use crate::{
    envelop::{IntoBoxedMessage, TypeTag},
    error::{GenericError, SendError, StdSyncSendError},
    trait_object::TraitObject,
    Bus, Error, Message, Relay,
};
use core::{
    any::TypeId,
    fmt,
    marker::PhantomData,
    mem,
    pin::Pin,
    task::{Context, Poll},
};
use futures::Future;
use futures::{future::poll_fn, FutureExt};
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
    fn send_msg(
        &self,
        _mid: u64,
        _msg: Box<dyn Message>,
    ) -> Result<(), SendError<Box<dyn Message>>> {
        unimplemented!()
    }
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

pub trait ReciveUnypedReceiver: Sync {
    fn poll_events(&self, ctx: &mut Context<'_>) -> Poll<Event<Box<dyn Message>, GenericError>>;
}

pub trait WrapperReturnTypeOnly<R: Message>: Send + Sync {
    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<R, Error>>,
    ) -> Result<u64, Error>;
}

pub trait WrapperReturnTypeAndError<R: Message, E: StdSyncSendError>: Send + Sync {
    fn start_polling_events(
        self: Arc<Self>,
    ) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>;
    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<R, Error<(), E>>>,
    ) -> Result<u64, Error>;
    fn response(&self, mid: u64, resp: Result<R, Error<(), E>>) -> Result<(), Error>;
}

pub trait TypeTagAccept {
    fn accept(&self, msg: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool;
    fn iter_types(&self, cb: &mut dyn FnMut(&TypeTag, &TypeTag, &TypeTag) -> bool);
}

pub trait ReceiverTrait: TypeTagAccept + Send + Sync {
    fn name(&self) -> &str;
    fn typed(&self) -> Option<AnyReceiver<'_>>;
    fn wrapper(&self) -> Option<AnyWrapperRef<'_>>;
    fn wrapper_arc(self: Arc<Self>) -> Option<AnyWrapperArc>;

    fn send_boxed(&self, mid: u64, msg: Box<dyn Message>) -> Result<(), Error<Box<dyn Message>>>;
    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<Box<dyn Message>, Error>>,
    ) -> Result<u64, Error>;

    fn stats(&self) -> Result<Stats, Error<Action>>;

    fn close(&self) -> Result<(), Error<Action>>;
    fn close_notify(&self) -> &Notify;

    fn sync(&self) -> Result<(), Error<Action>>;
    fn sync_notify(&self) -> &Notify;

    fn flush(&self) -> Result<(), Error<Action>>;
    fn flush_notify(&self) -> &Notify;

    fn need_flush(&self) -> bool;

    fn try_reserve(&self) -> Option<Permit>;
    fn reserve_notify(&self) -> &Notify;

    fn start_polling(
        self: Arc<Self>,
    ) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>;
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
    S: ReciveTypedReceiver<R, E> + 'static,
{
    inner: S,
    waiters: Slab<Waiter<R, E>>,
    context: Arc<ReceiverContext>,
    _m: PhantomData<(M, R, E)>,
}

impl<M, R, E, S> WrapperReturnTypeAndError<R, E> for ReceiverWrapper<M, R, E, S>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    S: ReciveTypedReceiver<R, E> + Send + Sync + 'static,
{
    fn start_polling_events(
        self: Arc<Self>,
    ) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>> {
        Box::new(move |_| {
            Box::pin(async move {
                loop {
                    let this = self.clone();
                    let event = poll_fn(move |ctx| this.inner.poll_events(ctx)).await;

                    match event {
                        Event::Exited => {
                            self.context.closed.notify_waiters();
                            break;
                        }
                        Event::Flushed => self.context.flushed.notify_waiters(),
                        Event::Synchronized(_res) => self.context.synchronized.notify_waiters(),
                        Event::Response(mid, resp) => {
                            self.context.processing.fetch_sub(1, Ordering::SeqCst);
                            self.context.response.notify_one();

                            if let Err(err) = self.response(mid, resp) {
                                error!("Response error: {}", err);
                            }
                        }

                        _ => unimplemented!(),
                    }
                }
            })
        })
    }

    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<R, Error<(), E>>>,
    ) -> Result<u64, Error> {
        Ok(self
            .waiters
            .insert(Waiter::WithErrorType(listener))
            .ok_or_else(|| Error::AddListenerError)? as _)
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
    S: ReciveTypedReceiver<R, E> + Send + Sync + 'static,
{
    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<R, Error>>,
    ) -> Result<u64, Error> {
        Ok(self
            .waiters
            .insert(Waiter::WithoutErrorType(listener))
            .ok_or_else(|| Error::AddListenerError)? as _)
    }
}

impl<M, R, E, S> TypeTagAccept for ReceiverWrapper<M, R, E, S>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    S: ReciveTypedReceiver<R, E> + Send + Sync + 'static,
{
    fn iter_types(&self, cb: &mut dyn FnMut(&TypeTag, &TypeTag, &TypeTag) -> bool) {
        let _ = cb(&M::type_tag_(), &R::type_tag_(), &E::type_tag_());
    }

    fn accept(&self, msg: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
        if let Some(resp) = resp {
            if resp.as_ref() != R::type_tag_().as_ref() {
                return false;
            }
        }

        if let Some(err) = err {
            if err.as_ref() != E::type_tag_().as_ref() {
                return false;
            }
        }

        msg.as_ref() == M::type_tag_().as_ref()
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

    fn typed(&self) -> Option<AnyReceiver<'_>> {
        Some(AnyReceiver::new(&self.inner))
    }

    fn wrapper(&self) -> Option<AnyWrapperRef<'_>> {
        Some(AnyWrapperRef::new(self))
    }

    fn wrapper_arc(self: Arc<Self>) -> Option<AnyWrapperArc> {
        Some(AnyWrapperArc::new(self))
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

    fn stats(&self) -> Result<Stats, Error<Action>> {
        unimplemented!()
    }

    fn close(&self) -> Result<(), Error<Action>> {
        Ok(SendUntypedReceiver::send(&self.inner, Action::Close)?)
    }

    fn close_notify(&self) -> &Notify {
        &self.context.closed
    }

    fn sync(&self) -> Result<(), Error<Action>> {
        Ok(SendUntypedReceiver::send(&self.inner, Action::Sync)?)
    }

    fn sync_notify(&self) -> &Notify {
        &self.context.synchronized
    }

    fn flush(&self) -> Result<(), Error<Action>> {
        Ok(SendUntypedReceiver::send(&self.inner, Action::Flush)?)
    }

    fn flush_notify(&self) -> &Notify {
        &self.context.flushed
    }

    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<Box<dyn Message>, Error>>,
    ) -> Result<u64, Error> {
        Ok(self
            .waiters
            .insert(Waiter::Boxed(listener))
            .ok_or_else(|| Error::AddListenerError)? as _)
    }

    fn need_flush(&self) -> bool {
        self.context.need_flush.load(Ordering::SeqCst)
    }

    fn try_reserve(&self) -> Option<Permit> {
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

    fn reserve_notify(&self) -> &Notify {
        &self.context.response
    }

    fn start_polling(
        self: Arc<Self>,
    ) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>> {
        self.start_polling_events()
    }
}

pub struct Permit {
    pub(crate) fuse: bool,
    pub(crate) inner: Arc<dyn PermitDrop + Send + Sync>,
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
    _m: PhantomData<&'a dyn Any>,
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
        let send_typed_receiver: TraitObject = unsafe { mem::transmute(send_typed_receiver) };

        Self {
            data: send_typed_receiver.data,
            typed: (
                TypeId::of::<dyn SendTypedReceiver<M>>(),
                send_typed_receiver.vtable,
            ),
            _m: Default::default(),
        }
    }

    #[inline]
    pub fn cast_send_typed<M: Message>(&'a self) -> Option<&'a dyn SendTypedReceiver<M>> {
        if self.typed.0 != TypeId::of::<dyn SendTypedReceiver<M>>() {
            return None;
        }

        Some(unsafe {
            mem::transmute(TraitObject {
                data: self.data,
                vtable: self.typed.1,
            })
        })
    }
}

unsafe impl Send for AnyReceiver<'_> {}

pub struct AnyWrapperRef<'a> {
    data: *mut (),
    wrapper_r: (TypeId, *mut ()),
    wrapper_re: (TypeId, *mut ()),
    _m: PhantomData<&'a usize>,
}

impl<'a> AnyWrapperRef<'a> {
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

    #[inline]
    pub fn cast_ret_only<R: Message>(&'a self) -> Option<&'a dyn WrapperReturnTypeOnly<R>> {
        if self.wrapper_r.0 != TypeId::of::<dyn WrapperReturnTypeOnly<R>>() {
            return None;
        }

        Some(unsafe {
            mem::transmute(TraitObject {
                data: self.data,
                vtable: self.wrapper_r.1,
            })
        })
    }

    #[inline]
    pub fn cast_ret_and_error<R: Message, E: StdSyncSendError>(
        &'a self,
    ) -> Option<&'a dyn WrapperReturnTypeAndError<R, E>> {
        if self.wrapper_re.0 != TypeId::of::<dyn WrapperReturnTypeAndError<R, E>>() {
            return None;
        }

        Some(unsafe {
            mem::transmute(TraitObject {
                data: self.data,
                vtable: self.wrapper_re.1,
            })
        })
    }
}

unsafe impl Send for AnyWrapperRef<'_> {}

pub struct AnyWrapperArc {
    wrapper_re: Box<dyn Any>,
}

impl AnyWrapperArc {
    pub fn new<R, E, S>(rcvr: Arc<S>) -> Self
    where
        R: Message,
        E: StdSyncSendError,
        S: WrapperReturnTypeAndError<R, E> + 'static,
    {
        let wrapper_re = Box::new(rcvr as Arc<dyn WrapperReturnTypeAndError<R, E>>);

        Self { wrapper_re }
    }

    #[inline]
    pub fn cast_ret_and_error<R: Message, E: StdSyncSendError>(
        &self,
    ) -> Option<Arc<dyn WrapperReturnTypeAndError<R, E>>> {
        Some(
            self.wrapper_re
                .downcast_ref::<Arc<dyn WrapperReturnTypeAndError<R, E>>>()?
                .clone(),
        )
    }
}

unsafe impl Send for AnyWrapperArc {}

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

#[derive(Clone)]
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
            inner: Arc::new(ReceiverWrapper {
                inner,
                waiters: sharded_slab::Slab::new_with_config::<SlabCfg>(),
                context: Arc::new(ReceiverContext {
                    limit,
                    processing: AtomicU64::new(0),
                    need_flush: AtomicBool::new(false),
                    flushed: Notify::new(),
                    synchronized: Notify::new(),
                    closed: Notify::new(),
                    response: Notify::new(),
                }),
                _m: Default::default(),
            }),
        }
    }

    #[inline]
    pub(crate) fn new_relay<S>(limit: u64, inner: S) -> Self
    where
        S: Relay + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(RelayWrapper::new(inner, limit)),
        }
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    #[inline]
    pub fn accept(&self, msg: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
        self.inner.accept(msg, resp, err)
    }

    #[inline]
    pub fn need_flush(&self) -> bool {
        self.inner.need_flush()
    }

    #[inline]
    pub async fn reserve(&self) -> Permit {
        loop {
            if let Some(p) = self.inner.try_reserve() {
                return p;
            } else {
                self.inner.reserve_notify().notified().await
            }
        }
    }

    #[inline]
    pub fn try_reserve(&self) -> Option<Permit> {
        self.inner.try_reserve()
    }

    #[inline]
    pub fn send<M: Message>(&self, mid: u64, msg: M, mut permit: Permit) -> Result<(), Error<M>> {
        let res = if let Some(any_receiver) = self.inner.typed() {
            any_receiver
                .cast_send_typed::<M>()
                .unwrap()
                .send(mid, msg)
                .map_err(Into::into)
        } else {
            self.inner
                .send_boxed(mid, msg.into_boxed())
                .map_err(|err| err.map_msg(|b| *b.as_any_boxed().downcast::<M>().unwrap()))
                .map(|_| ())
        };

        permit.fuse = true;

        res
    }

    #[inline]
    pub fn force_send<M: Message + Clone>(&self, mid: u64, msg: M) -> Result<(), Error<M>> {
        let res = if let Some(any_receiver) = self.inner.typed() {
            any_receiver
                .cast_send_typed::<M>()
                .unwrap()
                .send(mid, msg)
                .map_err(Into::into)
        } else {
            self.inner
                .send_boxed(mid, msg.into_boxed())
                .map_err(|err| err.map_msg(|b| *b.as_any_boxed().downcast::<M>().unwrap()))
                .map(|_| ())
        };

        res
    }

    #[inline]
    pub fn send_boxed(
        &self,
        mid: u64,
        msg: Box<dyn Message>,
        mut permit: Permit,
    ) -> Result<(), Error<Box<dyn Message>>> {
        let res = self.inner.send_boxed(mid, msg);
        permit.fuse = true;
        Ok(())
    }

    #[inline]
    pub fn start_polling(
        &self,
    ) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>> {
        self.inner.clone().start_polling()
    }

    #[inline]
    pub(crate) fn add_response_waiter_boxed(
        &self,
    ) -> Result<(u64, impl Future<Output = Result<Box<dyn Message>, Error>>), Error> {
        let (tx, rx) = oneshot::channel();
        let mid = self.inner.add_response_listener(tx)?;

        Ok((mid, async move {
            match rx.await {
                Ok(x) => x,
                Err(err) => Err(Error::from(err)),
            }
        }))
    }

    #[inline]
    pub(crate) fn add_response_waiter<R: Message>(
        &self,
    ) -> Result<(u64, impl Future<Output = Result<R, Error>>), Error> {
        if let Some(any_receiver) = self.inner.wrapper() {
            let (tx, rx) = oneshot::channel();
            let mid = any_receiver
                .cast_ret_only::<R>()
                .unwrap()
                .add_response_listener(tx)?;

            Ok((
                mid,
                async move {
                    match rx.await {
                        Ok(x) => x,
                        Err(err) => Err(Error::from(err)),
                    }
                }
                .left_future(),
            ))
        } else {
            let (tx, rx) = oneshot::channel();
            let mid = self.inner.add_response_listener(tx)?;

            Ok((
                mid,
                async move {
                    match rx.await {
                        Ok(Ok(x)) => Ok(*x.as_any_boxed().downcast::<R>().unwrap()),
                        Ok(Err(x)) => Err(x),
                        Err(err) => Err(Error::from(err)),
                    }
                }
                .right_future(),
            ))
        }
    }

    #[inline]
    pub(crate) fn add_response_waiter_we<R: Message, E: StdSyncSendError>(
        &self,
    ) -> Result<(u64, impl Future<Output = Result<R, Error<(), E>>>), Error> {
        if let Some(any_wrapper) = self.inner.wrapper() {
            let (tx, rx) = oneshot::channel();
            let mid = any_wrapper
                .cast_ret_and_error::<R, E>()
                .unwrap()
                .add_response_listener(tx)?;

            Ok((mid, async move {
                match rx.await {
                    Ok(x) => x,
                    Err(err) => Err(Error::from(err)),
                }
            }))
        } else {
            unimplemented!()
        }
    }

    #[inline]
    pub async fn close(&self) {
        let notify = self.inner.close_notify().notified();

        if self.inner.close().is_ok() {
            notify.await;
        } else {
            warn!("close failed!");
        }
    }

    #[inline]
    pub async fn sync(&self) {
        let notify = self.inner.sync_notify().notified();

        if self.inner.sync().is_ok() {
            notify.await;
        } else {
            warn!("sync failed!");
        }
    }

    #[inline]
    pub async fn flush(&self) {
        let notify = self.inner.flush_notify().notified();

        if self.inner.flush().is_ok() {
            notify.await;
        } else {
            warn!("flush failed!");
        }
    }

    #[inline]
    pub fn iter_types(&self, cb: &mut dyn FnMut(&TypeTag, &TypeTag, &TypeTag) -> bool) {
        self.inner.iter_types(cb)
    }
}
