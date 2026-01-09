use std::{
    marker::PhantomData,
    mem,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use futures::{Future, Stream};
use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::{
    execution::{AsyncExecution, BatchExecutionMode, SyncExecution},
    BufferUnorderedBatchedConfig, BufferUnorderedBatchedStats,
};
use crate::{
    builder::ReceiverSubscriberBuilder,
    error::{Error, StdSyncSendError},
    receiver::{
        Action, Event, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver,
        UntypedPollerCallback,
    },
    receivers::{
        common::{create_event_stream, send_typed_message, send_untyped_action, MaybeSendStats},
        Request,
    },
    AsyncBatchHandler, BatchHandler, Bus, Message, Untyped,
};

impl MaybeSendStats for Arc<BufferUnorderedBatchedStats> {
    fn on_send_success(&self) {
        self.buffer.fetch_add(1, Ordering::Relaxed);
    }
}

/// Generic buffer unordered batched receiver that works with both sync and async handlers.
pub struct BufferUnorderedBatched<M, R, E, Mode>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    tx: mpsc::UnboundedSender<Request<M>>,
    stats: Arc<BufferUnorderedBatchedStats>,
    srx: Mutex<Option<mpsc::UnboundedReceiver<Event<R, E>>>>,
    _mode: PhantomData<Mode>,
}

// Type aliases for backward compatibility
pub type BufferUnorderedBatchedSync<M, R, E> = BufferUnorderedBatched<M, R, E, SyncExecution>;
pub type BufferUnorderedBatchedAsync<M, R, E> = BufferUnorderedBatched<M, R, E, AsyncExecution>;

/// The poller function that processes messages in batches.
async fn buffer_unordered_batch_poller<T, M, R, E, Mode>(
    mut rx: mpsc::UnboundedReceiver<Request<M>>,
    bus: Bus,
    ut: Untyped,
    _stats: Arc<BufferUnorderedBatchedStats>,
    cfg: BufferUnorderedBatchedConfig,
    stx: mpsc::UnboundedSender<Event<R, E>>,
) where
    T: Send + Sync + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: BatchExecutionMode<T, M, R, E>,
{
    let handler = ut
        .downcast::<T>()
        .expect("handler type mismatch - this is a bug");
    let semaphore = Arc::new(tokio::sync::Semaphore::new(cfg.max_parallel));

    let mut buffer_mid = Vec::with_capacity(cfg.batch_size);
    let mut buffer = Vec::with_capacity(cfg.batch_size);

    while let Some(msg) = rx.recv().await {
        match msg {
            Request::Request(mid, msg, req) => {
                buffer_mid.push((mid, req));
                buffer.push(msg);

                if buffer_mid.len() >= cfg.batch_size {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("semaphore closed unexpectedly");

                    let batch_mids = mem::take(&mut buffer_mid);
                    let batch_msgs = mem::take(&mut buffer);

                    Mode::spawn_batch_handler(
                        handler.clone(),
                        batch_msgs,
                        batch_mids,
                        bus.clone(),
                        stx.clone(),
                        permit,
                    );
                }
            }

            Request::Action(Action::Init(..)) => {
                let _ = stx.send(Event::Ready);
            }

            Request::Action(Action::Close) => {
                rx.close();
            }

            Request::Action(Action::Flush) => {
                // Process remaining messages in buffer
                if !buffer_mid.is_empty() {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("semaphore closed unexpectedly");

                    let batch_mids = mem::take(&mut buffer_mid);
                    let batch_msgs = mem::take(&mut buffer);

                    Mode::spawn_batch_handler(
                        handler.clone(),
                        batch_msgs,
                        batch_mids,
                        bus.clone(),
                        stx.clone(),
                        permit,
                    );
                }

                // Wait for all in-flight tasks to complete
                let _ = semaphore.acquire_many(cfg.max_parallel as _).await;
                let _ = stx.send(Event::Flushed);
            }

            Request::Action(Action::Sync) => {
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
    cfg: BufferUnorderedBatchedConfig,
) -> (BufferUnorderedBatched<M, R, E, Mode>, UntypedPollerCallback)
where
    T: Send + Sync + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: BatchExecutionMode<T, M, R, E>,
{
    let stats = Arc::new(BufferUnorderedBatchedStats {
        buffer: AtomicU64::new(0),
        buffer_total: AtomicU64::new(cfg.buffer_size as _),
        parallel: AtomicU64::new(0),
        parallel_total: AtomicU64::new(cfg.max_parallel as _),
        batch: AtomicU64::new(0),
        batch_size: AtomicU64::new(cfg.batch_size as _),
    });

    let (stx, srx) = mpsc::unbounded_channel();
    let (tx, rx) = mpsc::unbounded_channel();
    let stats_clone = stats.clone();

    let poller = Box::new(move |ut| {
        Box::new(move |bus| {
            Box::pin(buffer_unordered_batch_poller::<T, M, R, E, Mode>(
                rx,
                bus,
                ut,
                stats_clone,
                cfg,
                stx,
            )) as Pin<Box<dyn Future<Output = ()> + Send>>
        }) as Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
    });

    (
        BufferUnorderedBatched {
            tx,
            stats,
            srx: Mutex::new(Some(srx)),
            _mode: PhantomData,
        },
        poller,
    )
}

// ReceiverSubscriberBuilder for sync batch handlers
impl<T, M, R> ReceiverSubscriberBuilder<T, M, R, T::Error>
    for BufferUnorderedBatchedSync<M, R, T::Error>
where
    T: BatchHandler<M, Response = R> + 'static,
    T::Error: StdSyncSendError,
    M: Message,
    R: Message,
{
    type Config = BufferUnorderedBatchedConfig;

    fn build(cfg: Self::Config) -> (Self, UntypedPollerCallback) {
        build_receiver::<T, M, R, T::Error, SyncExecution>(cfg)
    }
}

// ReceiverSubscriberBuilder for async batch handlers
impl<T, M, R> ReceiverSubscriberBuilder<T, M, R, T::Error>
    for BufferUnorderedBatchedAsync<M, R, T::Error>
where
    T: AsyncBatchHandler<M, Response = R> + 'static,
    T::Error: StdSyncSendError + Clone,
    M: Message,
    R: Message,
{
    type Config = BufferUnorderedBatchedConfig;

    fn build(cfg: Self::Config) -> (Self, UntypedPollerCallback) {
        build_receiver::<T, M, R, T::Error, AsyncExecution>(cfg)
    }
}

// SendUntypedReceiver - generic over Mode, written once
impl<M, R, E, Mode> SendUntypedReceiver for BufferUnorderedBatched<M, R, E, Mode>
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
impl<M, R, E, Mode> SendTypedReceiver<M> for BufferUnorderedBatched<M, R, E, Mode>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: Send + Sync + 'static,
{
    fn send(&self, mid: u64, m: M, req: bool, _bus: &Bus) -> Result<(), Error<M>> {
        send_typed_message(&self.tx, &self.stats, mid, m, req)
    }
}

// ReciveTypedReceiver - generic over Mode, written once
impl<M, R, E, Mode> ReciveTypedReceiver<R, E> for BufferUnorderedBatched<M, R, E, Mode>
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
