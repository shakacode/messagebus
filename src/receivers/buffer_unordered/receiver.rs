use std::{marker::PhantomData, pin::Pin};

use futures::{Future, Stream};
use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::{
    execution::{AsyncExecution, ExecutionMode, SyncExecution},
    BufferUnorderedConfig,
};
use crate::{
    error::{Error, StdSyncSendError},
    receiver::{
        Action, Event, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver,
        UntypedPollerCallback,
    },
    receivers::{
        common::{create_event_stream, send_typed_message, send_untyped_action},
        Request,
    },
    Bus, Message, Untyped,
};

/// Generic buffer unordered receiver that works with both sync and async handlers.
///
/// The `Mode` parameter determines whether this receiver uses sync or async execution:
/// - `SyncExecution`: Uses `spawn_blocking` for sync handlers
/// - `AsyncExecution`: Uses `tokio::spawn` for async handlers
pub struct BufferUnordered<M, R, E, Mode>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    tx: mpsc::UnboundedSender<Request<M>>,
    srx: Mutex<Option<mpsc::UnboundedReceiver<Event<R, E>>>>,
    _mode: PhantomData<Mode>,
}

// Type aliases for backward compatibility
pub type BufferUnorderedSync<M, R, E> = BufferUnordered<M, R, E, SyncExecution>;
pub type BufferUnorderedAsync<M, R, E> = BufferUnordered<M, R, E, AsyncExecution>;

/// The poller function that processes messages.
///
/// This is generic over the execution mode, so the same logic works for both
/// sync and async handlers.
async fn buffer_unordered_poller<T, M, R, E, Mode>(
    mut rx: mpsc::UnboundedReceiver<Request<M>>,
    bus: Bus,
    ut: Untyped,
    cfg: BufferUnorderedConfig,
    stx: mpsc::UnboundedSender<Event<R, E>>,
) where
    T: Send + Sync + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: ExecutionMode<T, M, R, E>,
{
    assert!(cfg.max_parallel > 0, "max_parallel must be > 0");

    let handler = ut
        .downcast::<T>()
        .expect("handler type mismatch - this is a bug");
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(cfg.max_parallel));

    while let Some(msg) = rx.recv().await {
        match msg {
            Request::Request(mid, msg, _req) => {
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("semaphore closed unexpectedly");

                Mode::spawn_handler(handler.clone(), msg, bus.clone(), stx.clone(), mid, permit);
            }

            Request::Action(Action::Init(..)) => {
                let _ = stx.send(Event::Ready);
            }

            Request::Action(Action::Close) => {
                rx.close();
            }

            Request::Action(Action::Flush) => {
                // Wait for all in-flight tasks to complete
                let _ = semaphore.acquire_many(cfg.max_parallel as _).await;
                let _ = stx.send(Event::Flushed);
            }

            Request::Action(Action::Sync) => {
                // Wait for all in-flight tasks, then call sync
                let lock = semaphore.acquire_many(cfg.max_parallel as _).await;

                let resp = Mode::call_sync(handler.clone(), bus.clone()).await;
                drop(lock);

                let _ = stx.send(Event::Synchronized(resp.map_err(Error::Other)));
            }

            _ => unimplemented!(),
        }
    }
}

/// Shared build logic for both sync and async variants.
fn build_receiver<T, M, R, E, Mode>(
    cfg: BufferUnorderedConfig,
) -> (BufferUnordered<M, R, E, Mode>, UntypedPollerCallback)
where
    T: Send + Sync + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: ExecutionMode<T, M, R, E>,
{
    let (stx, srx) = mpsc::unbounded_channel();
    let (tx, rx) = mpsc::unbounded_channel();

    let poller = Box::new(move |ut| {
        Box::new(move |bus| {
            Box::pin(buffer_unordered_poller::<T, M, R, E, Mode>(
                rx, bus, ut, cfg, stx,
            )) as Pin<Box<dyn Future<Output = ()> + Send>>
        }) as Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
    });

    (
        BufferUnordered {
            tx,
            srx: Mutex::new(Some(srx)),
            _mode: PhantomData,
        },
        poller,
    )
}

// ReceiverSubscriberBuilder implementations via macro
crate::impl_receiver_subscriber_builder!(
    BufferUnorderedSync<M, R, E>,
    Handler,
    BufferUnorderedConfig,
    SyncExecution
);

crate::impl_receiver_subscriber_builder!(
    BufferUnorderedAsync<M, R, E>,
    AsyncHandler,
    BufferUnorderedConfig,
    AsyncExecution
);

// SendUntypedReceiver - generic over Mode, written once
impl<M, R, E, Mode> SendUntypedReceiver for BufferUnordered<M, R, E, Mode>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: Send + Sync + 'static,
{
    fn send(&self, msg: Action, _bus: &Bus) -> Result<(), Error<Action>> {
        send_untyped_action(&self.tx, msg)
    }
}

// SendTypedReceiver - generic over Mode, written once
impl<M, R, E, Mode> SendTypedReceiver<M> for BufferUnordered<M, R, E, Mode>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: Send + Sync + 'static,
{
    fn send(&self, mid: u64, m: M, req: bool, _bus: &Bus) -> Result<(), Error<M>> {
        send_typed_message(&self.tx, mid, m, req)
    }
}

// ReciveTypedReceiver - generic over Mode, written once
impl<M, R, E, Mode> ReciveTypedReceiver<R, E> for BufferUnordered<M, R, E, Mode>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: Send + Sync + 'static,
{
    type Stream = Pin<Box<dyn Stream<Item = Event<R, E>> + Send>>;

    fn event_stream(&self, _: Bus) -> Self::Stream {
        create_event_stream(&self.srx)
    }
}
