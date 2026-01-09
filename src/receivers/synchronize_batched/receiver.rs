use std::{marker::PhantomData, mem, pin::Pin};

use futures::{Future, Stream};
use parking_lot::Mutex as ParkingLotMutex;
use tokio::sync::{mpsc, Mutex};

use super::{
    execution::{AsyncExecution, BatchSynchronizedExecutionMode, SyncExecution},
    SynchronizedBatchedConfig,
};
use crate::{
    builder::ReceiverSubscriberBuilder,
    error::{Error, StdSyncSendError},
    receiver::{
        Action, Event, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver,
        UntypedPollerCallback,
    },
    receivers::{
        common::{create_event_stream, send_typed_message, send_untyped_action, NoStats},
        Request,
    },
    AsyncBatchSynchronizedHandler, BatchSynchronizedHandler, Bus, Message, Untyped,
};

/// Generic synchronized batched receiver that works with both sync and async handlers.
pub struct SynchronizedBatched<M, R, E, Mode>
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
pub type SynchronizedBatchedSync<M, R, E> = SynchronizedBatched<M, R, E, SyncExecution>;
pub type SynchronizedBatchedAsync<M, R, E> = SynchronizedBatched<M, R, E, AsyncExecution>;

/// The poller function that processes messages in batches.
async fn batch_synchronized_poller<T, M, R, E, Mode>(
    mut rx: mpsc::UnboundedReceiver<Request<M>>,
    bus: Bus,
    ut: Untyped,
    cfg: SynchronizedBatchedConfig,
    stx: mpsc::UnboundedSender<Event<R, E>>,
) where
    T: Send + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError + Clone,
    Mode: BatchSynchronizedExecutionMode<T, M, R, E>,
{
    let handler = ut
        .downcast::<Mutex<T>>()
        .expect("handler type mismatch - this is a bug");

    let mut buffer_mid = Vec::with_capacity(cfg.batch_size);
    let mut buffer = Vec::with_capacity(cfg.batch_size);
    let mut pending_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            Request::Request(mid, msg, req) => {
                buffer_mid.push((mid, req));
                buffer.push(msg);

                if buffer_mid.len() >= cfg.batch_size {
                    let batch_mids = mem::take(&mut buffer_mid);
                    let batch_msgs = mem::take(&mut buffer);

                    let handle = Mode::spawn_batch_handler(
                        handler.clone(),
                        batch_msgs,
                        batch_mids,
                        bus.clone(),
                        stx.clone(),
                    );
                    pending_tasks.push(handle);
                }
            }

            Request::Action(Action::Init(..)) => {
                let _ = stx.send(Event::Ready);
            }

            Request::Action(Action::Close) => {
                // Wait for all pending tasks before closing
                for handle in pending_tasks.drain(..) {
                    let _ = handle.await;
                }
                rx.close();
            }

            Request::Action(Action::Flush) => {
                // Process remaining messages in buffer
                if !buffer_mid.is_empty() {
                    let batch_mids = mem::take(&mut buffer_mid);
                    let batch_msgs = mem::take(&mut buffer);

                    let handle = Mode::spawn_batch_handler(
                        handler.clone(),
                        batch_msgs,
                        batch_mids,
                        bus.clone(),
                        stx.clone(),
                    );
                    pending_tasks.push(handle);
                }

                // Wait for all pending batch tasks to complete
                for handle in pending_tasks.drain(..) {
                    let _ = handle.await;
                }

                let _ = stx.send(Event::Flushed);
            }

            Request::Action(Action::Sync) => {
                // Wait for pending tasks before sync
                for handle in pending_tasks.drain(..) {
                    let _ = handle.await;
                }

                let resp = Mode::call_sync(handler.clone(), bus.clone()).await;
                let _ = stx.send(Event::Synchronized(resp.map_err(Error::Other)));
            }

            _ => unimplemented!(),
        }
    }
}

/// Shared build logic for both sync and async variants.
fn build_receiver<T, M, R, E, Mode>(
    cfg: SynchronizedBatchedConfig,
) -> (SynchronizedBatched<M, R, E, Mode>, UntypedPollerCallback)
where
    T: Send + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError + Clone,
    Mode: BatchSynchronizedExecutionMode<T, M, R, E>,
{
    let (stx, srx) = mpsc::unbounded_channel();
    let (tx, rx) = mpsc::unbounded_channel();

    let poller = Box::new(move |ut| {
        Box::new(move |bus| {
            Box::pin(batch_synchronized_poller::<T, M, R, E, Mode>(
                rx, bus, ut, cfg, stx,
            )) as Pin<Box<dyn Future<Output = ()> + Send>>
        }) as Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
    });

    (
        SynchronizedBatched {
            tx,
            srx: ParkingLotMutex::new(Some(srx)),
            _mode: PhantomData,
        },
        poller,
    )
}

// ReceiverSubscriberBuilder for sync batch handlers
impl<T, M, R> ReceiverSubscriberBuilder<T, M, R, T::Error>
    for SynchronizedBatchedSync<M, R, T::Error>
where
    T: BatchSynchronizedHandler<M, Response = R> + 'static,
    T::Error: StdSyncSendError + Clone,
    M: Message,
    R: Message,
{
    type Config = SynchronizedBatchedConfig;

    fn build(cfg: Self::Config) -> (Self, UntypedPollerCallback) {
        build_receiver::<T, M, R, T::Error, SyncExecution>(cfg)
    }
}

// ReceiverSubscriberBuilder for async batch handlers
impl<T, M, R> ReceiverSubscriberBuilder<T, M, R, T::Error>
    for SynchronizedBatchedAsync<M, R, T::Error>
where
    T: AsyncBatchSynchronizedHandler<M, Response = R> + 'static,
    T::Error: StdSyncSendError + Clone,
    M: Message,
    R: Message,
{
    type Config = SynchronizedBatchedConfig;

    fn build(cfg: Self::Config) -> (Self, UntypedPollerCallback) {
        build_receiver::<T, M, R, T::Error, AsyncExecution>(cfg)
    }
}

// SendUntypedReceiver - generic over Mode, written once
impl<M, R, E, Mode> SendUntypedReceiver for SynchronizedBatched<M, R, E, Mode>
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
impl<M, R, E, Mode> SendTypedReceiver<M> for SynchronizedBatched<M, R, E, Mode>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: Send + Sync + 'static,
{
    fn send(&self, mid: u64, m: M, req: bool, _bus: &Bus) -> Result<(), Error<M>> {
        send_typed_message(&self.tx, &NoStats, mid, m, req)
    }
}

// ReciveTypedReceiver - generic over Mode, written once
impl<M, R, E, Mode> ReciveTypedReceiver<R, E> for SynchronizedBatched<M, R, E, Mode>
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
