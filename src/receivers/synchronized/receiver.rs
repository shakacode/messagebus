use std::{marker::PhantomData, pin::Pin};

use futures::{Future, Stream};
use parking_lot::Mutex as ParkingLotMutex;
use tokio::sync::{mpsc, Mutex};

use super::{
    execution::{AsyncExecution, SyncExecution, SynchronizedExecutionMode},
    SynchronizedConfig,
};
use crate::{
    builder::ReceiverSubscriberBuilder,
    error::{Error, StdSyncSendError},
    receiver::{
        Action, Event, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver,
        UntypedPollerCallback,
    },
    receivers::Request,
    AsyncSynchronizedHandler, Bus, Message, SynchronizedHandler, Untyped,
};

/// Generic synchronized receiver that works with both sync and async handlers.
///
/// The `Mode` parameter determines whether this receiver uses sync or async execution:
/// - `SyncExecution`: Uses `spawn_blocking` for sync handlers
/// - `AsyncExecution`: Uses `tokio::spawn` for async handlers
pub struct Synchronized<M, R, E, Mode>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    tx: mpsc::UnboundedSender<Request<M>>,
    srx: ParkingLotMutex<Option<mpsc::UnboundedReceiver<Event<R, E>>>>,
    _mode: PhantomData<Mode>,
}

// Type aliases for backward compatibility
pub type SynchronizedSync<M, R, E> = Synchronized<M, R, E, SyncExecution>;
pub type SynchronizedAsync<M, R, E> = Synchronized<M, R, E, AsyncExecution>;

/// The poller function that processes messages.
async fn synchronized_poller<T, M, R, E, Mode>(
    mut rx: mpsc::UnboundedReceiver<Request<M>>,
    bus: Bus,
    ut: Untyped,
    stx: mpsc::UnboundedSender<Event<R, E>>,
) where
    T: Send + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: SynchronizedExecutionMode<T, M, R, E>,
{
    let handler = ut
        .downcast::<Mutex<T>>()
        .expect("handler type mismatch - this is a bug");

    while let Some(msg) = rx.recv().await {
        match msg {
            Request::Request(mid, msg, _req) => {
                Mode::spawn_handler(handler.clone(), msg, bus.clone(), stx.clone(), mid);
            }

            Request::Action(Action::Init(..)) => {
                let _ = stx.send(Event::Ready);
            }

            Request::Action(Action::Close) => {
                rx.close();
            }

            Request::Action(Action::Flush) => {
                let _ = stx.send(Event::Flushed);
            }

            Request::Action(Action::Sync) => {
                let resp = Mode::call_sync(handler.clone(), bus.clone()).await;
                let _ = stx.send(Event::Synchronized(resp.map_err(Error::Other)));
            }

            _ => unimplemented!(),
        }
    }
}

/// Shared build logic for both sync and async variants.
fn build_receiver<T, M, R, E, Mode>(
    _cfg: SynchronizedConfig,
) -> (Synchronized<M, R, E, Mode>, UntypedPollerCallback)
where
    T: Send + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: SynchronizedExecutionMode<T, M, R, E>,
{
    let (stx, srx) = mpsc::unbounded_channel();
    let (tx, rx) = mpsc::unbounded_channel();

    let poller = Box::new(move |ut| {
        Box::new(move |bus| {
            Box::pin(synchronized_poller::<T, M, R, E, Mode>(rx, bus, ut, stx))
                as Pin<Box<dyn Future<Output = ()> + Send>>
        }) as Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
    });

    (
        Synchronized {
            tx,
            srx: ParkingLotMutex::new(Some(srx)),
            _mode: PhantomData,
        },
        poller,
    )
}

// ReceiverSubscriberBuilder for sync handlers
impl<T, M, R, E> ReceiverSubscriberBuilder<T, M, R, E> for SynchronizedSync<M, R, E>
where
    T: SynchronizedHandler<M, Response = R, Error = E> + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    type Config = SynchronizedConfig;

    fn build(cfg: Self::Config) -> (Self, UntypedPollerCallback) {
        build_receiver::<T, M, R, E, SyncExecution>(cfg)
    }
}

// ReceiverSubscriberBuilder for async handlers
impl<T, M, R, E> ReceiverSubscriberBuilder<T, M, R, E> for SynchronizedAsync<M, R, E>
where
    T: AsyncSynchronizedHandler<M, Response = R, Error = E> + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    type Config = SynchronizedConfig;

    fn build(cfg: Self::Config) -> (Self, UntypedPollerCallback) {
        build_receiver::<T, M, R, E, AsyncExecution>(cfg)
    }
}

// SendUntypedReceiver - generic over Mode, written once
impl<M, R, E, Mode> SendUntypedReceiver for Synchronized<M, R, E, Mode>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: Send + Sync + 'static,
{
    fn send(&self, msg: Action, _bus: &Bus) -> Result<(), Error<Action>> {
        match self.tx.send(Request::Action(msg)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Action(msg))) => Err(Error::send_closed(msg)),
            _ => unimplemented!(),
        }
    }
}

// SendTypedReceiver - generic over Mode, written once
impl<M, R, E, Mode> SendTypedReceiver<M> for Synchronized<M, R, E, Mode>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: Send + Sync + 'static,
{
    fn send(&self, mid: u64, m: M, req: bool, _bus: &Bus) -> Result<(), Error<M>> {
        match self.tx.send(Request::Request(mid, m, req)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Request(_, msg, _))) => {
                Err(Error::send_closed(msg))
            }
            _ => unimplemented!(),
        }
    }
}

// ReciveTypedReceiver - generic over Mode, written once
impl<M, R, E, Mode> ReciveTypedReceiver<R, E> for Synchronized<M, R, E, Mode>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: Send + Sync + 'static,
{
    type Stream = Pin<Box<dyn Stream<Item = Event<R, E>> + Send>>;

    fn event_stream(&self, _: Bus) -> Self::Stream {
        let mut rx = self.srx.lock().take().expect("event_stream called twice");
        Box::pin(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
    }
}
