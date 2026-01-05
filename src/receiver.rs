use crate::relay::RelayWrapper;
use crate::stats::Stats;
use crate::Untyped;
use crate::{
    envelop::{IntoBoxedMessage, TypeTag},
    error::{GenericError, StdSyncSendError},
    trait_object::TraitObject,
    Bus, Error, Message, Relay,
};
use core::{
    any::{Any, TypeId},
    fmt,
    marker::PhantomData,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicI64, Ordering},
};
use futures::{pin_mut, Stream};
use futures::{Future, FutureExt, StreamExt};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::{oneshot, Notify};

pub type BusPollerCallback = Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>;
pub type UntypedPollerCallback = Box<dyn FnOnce(Untyped) -> BusPollerCallback>;

struct SlabCfg;
impl sharded_slab::Config for SlabCfg {
    const RESERVED_BITS: usize = 1;
}

type Slab<T> = sharded_slab::Slab<T, SlabCfg>;

pub trait SendUntypedReceiver: Send + Sync {
    fn send(&self, msg: Action, bus: &Bus) -> Result<(), Error<Action>>;
    fn send_msg(
        &self,
        _mid: u64,
        _msg: Box<dyn Message>,
        _req: bool,
        _bus: &Bus,
    ) -> Result<(), Error<Box<dyn Message>>> {
        unimplemented!()
    }
}

pub trait SendTypedReceiver<M: Message>: Sync {
    fn send(&self, mid: u64, msg: M, req: bool, bus: &Bus) -> Result<(), Error<M>>;
}

pub trait ReciveTypedReceiver<M, E>: Sync
where
    M: Message,
    E: StdSyncSendError,
{
    type Stream: Stream<Item = Event<M, E>> + Send;

    fn event_stream(&self, bus: Bus) -> Self::Stream;
}

pub trait ReciveUntypedReceiver: Sync {
    type Stream: Stream<Item = Event<Box<dyn Message>, GenericError>> + Send;

    fn event_stream(&self, bus: Bus) -> Self::Stream;
}

pub trait WrapperReturnTypeOnly<R: Message>: Send + Sync {
    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<R, Error>>,
    ) -> Result<u64, Error>;
}

pub trait WrapperErrorTypeOnly<E: StdSyncSendError>: Send + Sync {
    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<Box<dyn Message>, Error<(), E>>>,
    ) -> Result<u64, Error>;
}

pub trait WrapperReturnTypeAndError<R: Message, E: StdSyncSendError>: Send + Sync {
    fn start_polling_events(self: Arc<Self>) -> BusPollerCallback;
    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<R, Error<(), E>>>,
    ) -> Result<u64, Error>;
    fn response(&self, mid: u64, resp: Result<R, Error<(), E>>) -> Result<Option<R>, Error>;
}

pub type TypeTagAcceptItem = (TypeTag, Option<(TypeTag, TypeTag)>);
pub trait TypeTagAccept {
    fn iter_types(&self) -> Box<dyn Iterator<Item = TypeTagAcceptItem> + '_>;
    fn accept_msg(&self, msg: &TypeTag) -> bool;
    fn accept_req(&self, req: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool;
}

pub trait ReceiverTrait: TypeTagAccept + Send + Sync {
    fn name(&self) -> &str;
    fn typed(&self) -> Option<AnyReceiver<'_>>;
    fn wrapper(&self) -> Option<AnyWrapperRef<'_>>;

    fn id(&self) -> u64;

    fn send_boxed(
        &self,
        mid: u64,
        msg: Box<dyn Message>,
        req: bool,
        bus: &Bus,
    ) -> Result<(), Error<Box<dyn Message>>>;
    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<Box<dyn Message>, Error>>,
    ) -> Result<u64, Error>;

    fn stats(&self) -> Stats;

    fn send_action(&self, bus: &Bus, action: Action) -> Result<(), Error<Action>>;
    fn close_notify(&self) -> &Notify;
    fn sync_notify(&self) -> &Notify;
    fn flush_notify(&self) -> &Notify;
    fn ready_notify(&self) -> &Notify;
    fn idle_notify(&self) -> &Notify;

    fn is_init_sent(&self) -> bool;
    fn is_ready(&self) -> bool;
    fn is_idling(&self) -> bool;
    fn need_flush(&self) -> bool;
    fn set_need_flush(&self);

    fn try_reserve(&self, tt: &TypeTag) -> Option<Permit>;
    fn reserve_notify(&self, tt: &TypeTag) -> Arc<Notify>;
    fn increment_processing(&self, tt: &TypeTag);

    fn start_polling(self: Arc<Self>) -> BusPollerCallback;
}

#[allow(dead_code)]
pub trait ReceiverPollerBuilder {
    fn build(bus: Bus) -> Box<dyn Future<Output = ()>>;
}

pub trait PermitDrop {
    fn permit_drop(&self);
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum Action {
    Init(u64),
    Flush,
    Sync,
    Close,
    Stats,
}

pub type EventBoxed<E> = Event<Box<dyn Message>, E>;

#[non_exhaustive]
#[derive(Debug)]
pub enum Event<M, E: StdSyncSendError> {
    Response(u64, Result<M, Error<(), E>>),
    Synchronized(Result<(), Error<(), E>>),
    BatchComplete(TypeTag, u64),
    Error(Error<(), E>),
    InitFailed(Error<(), E>),
    Stats(Stats),
    Flushed,
    Exited,
    Ready,
    Pause,
}

impl<M, E: StdSyncSendError> Event<M, E> {
    pub fn map_msg<U, F: FnOnce(M) -> U>(self, f: F) -> Event<U, E> {
        match self {
            Event::Response(mid, res) => Event::Response(mid, res.map(f)),
            Event::Synchronized(res) => Event::Synchronized(res),
            Event::BatchComplete(tt, cnt) => Event::BatchComplete(tt, cnt),
            Event::Error(err) => Event::Error(err),
            Event::InitFailed(err) => Event::InitFailed(err),
            Event::Stats(st) => Event::Stats(st),
            Event::Flushed => Event::Flushed,
            Event::Exited => Event::Exited,
            Event::Ready => Event::Ready,
            Event::Pause => Event::Pause,
        }
    }
}

struct ReceiverWrapper<M, R, E, S>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    S: ReciveTypedReceiver<R, E> + 'static,
{
    inner: S,
    id: u64,
    waiters: Slab<Waiter<R, E>>,
    context: Arc<ReceiverContext>,
    _m: PhantomData<(M, R, E)>,
}

impl<M, R, E, S> WrapperReturnTypeAndError<R, E> for ReceiverWrapper<M, R, E, S>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    S: SendUntypedReceiver + ReciveTypedReceiver<R, E> + Send + Sync + 'static,
{
    fn start_polling_events(self: Arc<Self>) -> BusPollerCallback {
        Box::new(move |bus| {
            Box::pin(async move {
                let this = self.clone();
                let events = this.inner.event_stream(bus);
                pin_mut!(events);

                loop {
                    let event = if let Some(event) = events.next().await {
                        event
                    } else {
                        self.context.closed.notify_waiters();
                        break;
                    };

                    match event {
                        Event::Error(err) => error!("Batch Error: {}", err),
                        Event::Pause => self.context.ready_flag.store(false, Ordering::SeqCst),
                        Event::Ready => {
                            self.context.ready_flag.store(true, Ordering::SeqCst);
                            self.context.ready.notify_waiters();
                        }
                        Event::InitFailed(err) => {
                            error!("Receiver init failed: {}", err);

                            self.context.ready_flag.store(false, Ordering::SeqCst);
                            self.context.ready.notify_waiters();
                        }
                        Event::Exited => {
                            self.context.closed.notify_waiters();
                            break;
                        }
                        Event::Flushed => {
                            self.context.need_flush.store(false, Ordering::SeqCst);
                            self.context.flushed.notify_one();
                        }
                        Event::Synchronized(_res) => self.context.synchronized.notify_waiters(),
                        Event::Response(mid, resp) => {
                            let prev_value = self.context.processing.fetch_sub(1, Ordering::SeqCst);
                            if prev_value == 1 {
                                // last task completes
                                self.context.idle.notify_waiters();
                            }

                            self.context.response.notify_one();

                            match self.response(mid, resp) {
                                Ok(Some(_resp)) => {
                                    if self.context.resend_unused_resp {
                                        // TODO
                                    }
                                }

                                Ok(None) => (),
                                Err(err) => error!("Response Error: {}", err),
                            }
                        }

                        Event::BatchComplete(_, n) => {
                            self.context.processing.fetch_sub(n as _, Ordering::SeqCst);

                            if n > 1 {
                                self.context.response.notify_waiters();
                            } else {
                                self.context.response.notify_one();
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
            .ok_or(Error::AddListenerError)? as _)
    }

    fn response(&self, mid: u64, resp: Result<R, Error<(), E>>) -> Result<Option<R>, Error> {
        Ok(if let Some(waiter) = self.waiters.take(mid as _) {
            match waiter {
                Waiter::WithErrorType(sender) => sender.send(resp).unwrap(),
                Waiter::WithoutErrorType(sender) => {
                    sender.send(resp.map_err(|e| e.into_dyn())).unwrap()
                }
                Waiter::Boxed(sender) => sender
                    .send(resp.map_err(|e| e.into_dyn()).map(|x| x.into_boxed()))
                    .unwrap(),

                Waiter::BoxedWithError(sender) => {
                    sender.send(resp.map(|x| x.into_boxed())).unwrap()
                }
            }
            None
        } else {
            resp.ok()
        })
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
            .ok_or(Error::AddListenerError)? as _)
    }
}

impl<M, R, E, S> WrapperErrorTypeOnly<E> for ReceiverWrapper<M, R, E, S>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    S: ReciveTypedReceiver<R, E> + Send + Sync + 'static,
{
    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<Box<dyn Message>, Error<(), E>>>,
    ) -> Result<u64, Error> {
        Ok(self
            .waiters
            .insert(Waiter::BoxedWithError(listener))
            .ok_or(Error::AddListenerError)? as _)
    }
}

impl<M, R, E, S> TypeTagAccept for ReceiverWrapper<M, R, E, S>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    S: ReciveTypedReceiver<R, E> + Send + Sync + 'static,
{
    fn iter_types(&self) -> Box<dyn Iterator<Item = TypeTagAcceptItem> + '_> {
        Box::new(std::iter::once((
            M::type_tag_(),
            Some((R::type_tag_(), E::type_tag_())),
        )))
    }

    fn accept_req(&self, req: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
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

        req.as_ref() == M::type_tag_().as_ref()
    }

    fn accept_msg(&self, msg: &TypeTag) -> bool {
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
        std::any::type_name::<S>()
    }

    fn typed(&self) -> Option<AnyReceiver<'_>> {
        Some(AnyReceiver::new(&self.inner))
    }

    fn wrapper(&self) -> Option<AnyWrapperRef<'_>> {
        Some(AnyWrapperRef::new(self))
    }

    fn id(&self) -> u64 {
        self.id
    }

    fn send_boxed(
        &self,
        mid: u64,
        boxed_msg: Box<dyn Message>,
        req: bool,
        bus: &Bus,
    ) -> Result<(), Error<Box<dyn Message>>> {
        let boxed = boxed_msg
            .as_any_boxed()
            .downcast::<M>()
            .map_err(|_| Error::MessageCastError)?;

        SendTypedReceiver::send(&self.inner, mid, *boxed, req, bus)
            .map_err(|err| err.map_msg(|m| m.into_boxed()))
    }

    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<Box<dyn Message>, Error>>,
    ) -> Result<u64, Error> {
        Ok(self
            .waiters
            .insert(Waiter::Boxed(listener))
            .ok_or(Error::AddListenerError)? as _)
    }

    fn stats(&self) -> Stats {
        Stats {
            msg_type_tag: M::type_tag_(),
            resp_type_tag: Some(R::type_tag_()),
            err_type_tag: Some(E::type_tag_()),

            has_queue: true,
            queue_capacity: self.context.limit as _,
            queue_size: self.context.processing.load(Ordering::Relaxed) as _,

            ..Default::default()
        }
    }

    fn send_action(&self, bus: &Bus, action: Action) -> Result<(), Error<Action>> {
        SendUntypedReceiver::send(&self.inner, action, bus)
    }

    fn close_notify(&self) -> &Notify {
        &self.context.closed
    }

    fn sync_notify(&self) -> &Notify {
        &self.context.synchronized
    }

    fn flush_notify(&self) -> &Notify {
        &self.context.flushed
    }

    fn ready_notify(&self) -> &Notify {
        &self.context.ready
    }

    fn idle_notify(&self) -> &Notify {
        &self.context.idle
    }

    fn is_init_sent(&self) -> bool {
        self.context.init_sent.load(Ordering::SeqCst)
    }

    fn is_ready(&self) -> bool {
        self.context.ready_flag.load(Ordering::SeqCst)
    }

    fn is_idling(&self) -> bool {
        self.context.processing.load(Ordering::SeqCst) == 0
    }

    fn need_flush(&self) -> bool {
        self.context.need_flush.load(Ordering::SeqCst)
    }

    fn set_need_flush(&self) {
        self.context.need_flush.store(true, Ordering::SeqCst);
    }

    fn try_reserve(&self, _: &TypeTag) -> Option<Permit> {
        loop {
            let count = self.context.processing.load(Ordering::Relaxed);

            if count < self.context.limit as _ {
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

    fn reserve_notify(&self, _: &TypeTag) -> Arc<Notify> {
        self.context.response.clone()
    }

    fn increment_processing(&self, _tt: &TypeTag) {
        self.context.processing.fetch_add(1, Ordering::SeqCst);
    }

    fn start_polling(self: Arc<Self>) -> BusPollerCallback {
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
    pub fn new<M, S>(rcvr: &'a S) -> Self
    where
        M: Message,
        S: SendTypedReceiver<M> + 'static,
    {
        let dyn_ref: &dyn SendTypedReceiver<M> = rcvr;
        let raw = TraitObject::from_dyn(dyn_ref);

        Self {
            data: raw.data,
            typed: (TypeId::of::<dyn SendTypedReceiver<M>>(), raw.vtable),
            _m: Default::default(),
        }
    }

    #[inline]
    pub fn cast_send_typed<M: Message>(&'a self) -> Option<&'a dyn SendTypedReceiver<M>> {
        if self.typed.0 != TypeId::of::<dyn SendTypedReceiver<M>>() {
            return None;
        }

        // SAFETY: TypeId check above guarantees this is the correct trait type.
        // Lifetime 'a is preserved from the original reference stored in new().
        let raw = unsafe { TraitObject::from_raw_parts(self.data, self.typed.1) };
        Some(unsafe { raw.as_dyn_ref() })
    }
}

// SAFETY: AnyReceiver contains raw pointers derived from a reference to a type S
// that implements `SendTypedReceiver<M>`, which requires `Send + Sync`. The pointers
// are only used to reconstruct trait objects with the same lifetime 'a, and the
// PhantomData<&'a dyn Any> ensures proper lifetime tracking.
unsafe impl Send for AnyReceiver<'_> {}

pub struct AnyWrapperRef<'a> {
    data: *mut (),
    wrapper_r: (TypeId, *mut ()),
    wrapper_e: (TypeId, *mut ()),
    wrapper_re: (TypeId, *mut ()),
    _m: PhantomData<&'a usize>,
}

impl<'a> AnyWrapperRef<'a> {
    pub fn new<R, E, S>(rcvr: &'a S) -> Self
    where
        R: Message,
        E: StdSyncSendError,
        S: WrapperReturnTypeOnly<R>
            + WrapperErrorTypeOnly<E>
            + WrapperReturnTypeAndError<R, E>
            + 'static,
    {
        let dyn_r: &dyn WrapperReturnTypeOnly<R> = rcvr;
        let dyn_e: &dyn WrapperErrorTypeOnly<E> = rcvr;
        let dyn_re: &dyn WrapperReturnTypeAndError<R, E> = rcvr;

        let wrapper_r = TraitObject::from_dyn(dyn_r);
        let wrapper_e = TraitObject::from_dyn(dyn_e);
        let wrapper_re = TraitObject::from_dyn(dyn_re);

        Self {
            data: wrapper_r.data,
            wrapper_r: (
                TypeId::of::<dyn WrapperReturnTypeOnly<R>>(),
                wrapper_r.vtable,
            ),
            wrapper_e: (
                TypeId::of::<dyn WrapperErrorTypeOnly<E>>(),
                wrapper_e.vtable,
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

        // SAFETY: TypeId check above guarantees this is the correct trait type.
        // Lifetime 'a is preserved from the original reference stored in new().
        let raw = unsafe { TraitObject::from_raw_parts(self.data, self.wrapper_r.1) };
        Some(unsafe { raw.as_dyn_ref() })
    }

    #[inline]
    pub fn cast_error_only<E: StdSyncSendError>(
        &'a self,
    ) -> Option<&'a dyn WrapperErrorTypeOnly<E>> {
        if self.wrapper_e.0 != TypeId::of::<dyn WrapperErrorTypeOnly<E>>() {
            return None;
        }

        // SAFETY: TypeId check above guarantees this is the correct trait type.
        // Lifetime 'a is preserved from the original reference stored in new().
        let raw = unsafe { TraitObject::from_raw_parts(self.data, self.wrapper_e.1) };
        Some(unsafe { raw.as_dyn_ref() })
    }

    #[inline]
    pub fn cast_ret_and_error<R: Message, E: StdSyncSendError>(
        &'a self,
    ) -> Option<&'a dyn WrapperReturnTypeAndError<R, E>> {
        if self.wrapper_re.0 != TypeId::of::<dyn WrapperReturnTypeAndError<R, E>>() {
            return None;
        }

        // SAFETY: TypeId check above guarantees this is the correct trait type.
        // Lifetime 'a is preserved from the original reference stored in new().
        let raw = unsafe { TraitObject::from_raw_parts(self.data, self.wrapper_re.1) };
        Some(unsafe { raw.as_dyn_ref() })
    }
}

// SAFETY: AnyWrapperRef contains raw pointers derived from a reference to a type S
// that implements `WrapperReturnTypeOnly<R> + WrapperErrorTypeOnly<E> + WrapperReturnTypeAndError<R, E>`,
// which all require `Send + Sync`. The pointers are only used to reconstruct trait objects
// with the same lifetime 'a, and the PhantomData<&'a usize> ensures proper lifetime tracking.
unsafe impl Send for AnyWrapperRef<'_> {}

struct ReceiverContext {
    limit: u64,
    processing: AtomicI64,
    need_flush: AtomicBool,
    ready_flag: AtomicBool,
    flushed: Notify,
    synchronized: Notify,
    closed: Notify,
    ready: Notify,
    idle: Notify,
    response: Arc<Notify>,
    init_sent: AtomicBool,
    resend_unused_resp: bool,
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
    BoxedWithError(oneshot::Sender<Result<Box<dyn Message>, Error<(), E>>>),
}

#[derive(Clone)]
pub struct Receiver {
    inner: Arc<dyn ReceiverTrait>,
}

impl Hash for Receiver {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.id().hash(state);
    }
}

impl fmt::Debug for Receiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Receiver({:?})", self.inner.type_id())?;
        Ok(())
    }
}

impl core::cmp::PartialEq for Receiver {
    fn eq(&self, other: &Receiver) -> bool {
        self.inner.id() == other.inner.id()
    }
}

impl core::cmp::Eq for Receiver {}

impl Receiver {
    #[inline]
    pub(crate) fn new<M, R, E, S>(id: u64, limit: u64, resend: bool, inner: S) -> Self
    where
        M: Message,
        R: Message,
        E: StdSyncSendError,
        S: SendUntypedReceiver + SendTypedReceiver<M> + ReciveTypedReceiver<R, E> + 'static,
    {
        Self {
            inner: Arc::new(ReceiverWrapper {
                id,
                inner,
                waiters: sharded_slab::Slab::new_with_config::<SlabCfg>(),
                context: Arc::new(ReceiverContext {
                    limit,
                    processing: AtomicI64::new(0),
                    need_flush: AtomicBool::new(false),
                    ready_flag: AtomicBool::new(false),
                    init_sent: AtomicBool::new(false),
                    flushed: Notify::new(),
                    synchronized: Notify::new(),
                    closed: Notify::new(),
                    ready: Notify::new(),
                    idle: Notify::new(),
                    response: Arc::new(Notify::new()),
                    resend_unused_resp: resend,
                }),
                _m: Default::default(),
            }),
        }
    }

    #[inline]
    pub(crate) fn new_relay<S>(id: u64, inner: S) -> Self
    where
        S: Relay + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(RelayWrapper::new(id, inner)),
        }
    }

    #[inline]
    pub fn id(&self) -> u64 {
        self.inner.id()
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    #[inline]
    pub fn stats(&self) -> Stats {
        self.inner.stats()
    }

    #[inline]
    pub fn accept(
        &self,
        is_req: bool,
        msg: &TypeTag,
        resp: Option<&TypeTag>,
        err: Option<&TypeTag>,
    ) -> bool {
        if is_req {
            self.inner.accept_req(msg, resp, err)
        } else {
            self.inner.accept_msg(msg)
        }
    }

    #[inline]
    pub fn need_flush(&self) -> bool {
        self.inner.need_flush()
    }

    #[inline]
    pub fn is_idling(&self) -> bool {
        self.inner.is_idling()
    }

    #[inline]
    pub async fn reserve(&self, tt: &TypeTag) -> Permit {
        loop {
            if let Some(p) = self.inner.try_reserve(tt) {
                return p;
            } else {
                self.inner.reserve_notify(tt).notified().await
            }
        }
    }

    #[inline]
    pub fn try_reserve(&self, tt: &TypeTag) -> Option<Permit> {
        self.inner.try_reserve(tt)
    }

    #[inline]
    pub fn send<M: Message>(
        &self,
        bus: &Bus,
        mid: u64,
        msg: M,
        req: bool,
        mut permit: Permit,
    ) -> Result<(), Error<M>> {
        // Set need_flush BEFORE sending to ensure flush_all() sees the flag
        // even if it checks between flag set and message enqueue
        self.inner.set_need_flush();
        permit.fuse = true;

        if let Some(any_receiver) = self.inner.typed() {
            any_receiver
                .cast_send_typed::<M>()
                .unwrap()
                .send(mid, msg, req, bus)
        } else {
            self.inner
                .send_boxed(mid, msg.into_boxed(), req, bus)
                .map_err(|err| err.map_msg(|b| *b.as_any_boxed().downcast::<M>().unwrap()))
                .map(|_| ())
        }
    }

    #[inline]
    pub fn force_send<M: Message + Clone>(
        &self,
        bus: &Bus,
        mid: u64,
        msg: M,
        req: bool,
    ) -> Result<(), Error<M>> {
        // Set need_flush BEFORE sending to ensure flush_all() sees the flag
        // even if it checks between flag set and message enqueue
        self.inner.set_need_flush();
        self.inner.increment_processing(&M::type_tag_());

        if let Some(any_receiver) = self.inner.typed() {
            any_receiver
                .cast_send_typed::<M>()
                .unwrap()
                .send(mid, msg, req, bus)
        } else {
            self.inner
                .send_boxed(mid, msg.into_boxed(), req, bus)
                .map_err(|err| err.map_msg(|b| *b.as_any_boxed().downcast::<M>().unwrap()))
                .map(|_| ())
        }
    }

    #[inline]
    pub fn send_boxed(
        &self,
        bus: &Bus,
        mid: u64,
        msg: Box<dyn Message>,
        req: bool,
        mut permit: Permit,
    ) -> Result<(), Error<Box<dyn Message>>> {
        // Set need_flush BEFORE sending to ensure flush_all() sees the flag
        // even if it checks between flag set and message enqueue
        self.inner.set_need_flush();
        permit.fuse = true;
        self.inner.send_boxed(mid, msg, req, bus)
    }

    #[inline]
    pub fn start_polling(&self) -> BusPollerCallback {
        self.inner.clone().start_polling()
    }

    #[inline]
    #[allow(clippy::type_complexity)]
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
    #[allow(clippy::type_complexity)]
    pub(crate) fn add_response_waiter_boxed_we<E: StdSyncSendError>(
        &self,
    ) -> Result<
        (
            u64,
            impl Future<Output = Result<Box<dyn Message>, Error<(), E>>>,
        ),
        Error,
    > {
        if let Some(any_wrapper) = self.inner.wrapper() {
            let (tx, rx) = oneshot::channel();
            let mid = any_wrapper
                .cast_error_only::<E>()
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
    #[allow(clippy::type_complexity)]
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
    pub fn init(&self, bus: &Bus) -> Result<(), Error<Action>> {
        if !self.inner.is_init_sent() {
            self.inner.send_action(bus, Action::Init(self.inner.id()))
        } else {
            Ok(())
        }
    }

    #[inline]
    pub async fn idle(&self) {
        let notify = self.inner.idle_notify().notified();
        if !self.inner.is_idling() {
            notify.await;
        }
    }

    #[inline]
    pub async fn ready(&self) {
        let notify = self.inner.ready_notify().notified();
        if !self.inner.is_ready() {
            notify.await;
        }
    }

    #[inline]
    pub async fn close(&self, bus: &Bus) {
        let notify = self.inner.close_notify().notified();

        if self.inner.send_action(bus, Action::Close).is_ok() {
            notify.await;
        } else {
            warn!("close failed!");
        }
    }

    #[inline]
    pub async fn sync(&self, bus: &Bus) {
        let notify = self.inner.sync_notify().notified();

        if self.inner.send_action(bus, Action::Sync).is_ok() {
            notify.await;
        } else {
            warn!("sync failed!");
        }
    }

    #[inline]
    pub async fn flush(&self, bus: &Bus) {
        let notify = self.inner.flush_notify().notified();

        if self.inner.send_action(bus, Action::Flush).is_ok() {
            notify.await;
        } else {
            warn!("flush failed!");
        }
    }

    #[inline]
    pub fn iter_types(&self) -> impl Iterator<Item = (TypeTag, Option<(TypeTag, TypeTag)>)> + '_ {
        self.inner.iter_types()
    }
}
