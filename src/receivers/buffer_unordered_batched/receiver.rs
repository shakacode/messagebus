use std::{collections::HashMap, marker::PhantomData, pin::Pin, sync::Arc};

use futures::{Future, Stream};
use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::{
    execution::{AsyncExecution, BatchExecutionMode, SyncExecution},
    BufferUnorderedBatchedConfig,
};
use crate::{
    error::{Error, StdSyncSendError},
    group::GroupId,
    receiver::{
        Action, Event, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver,
        UntypedPollerCallback,
    },
    receivers::{
        common::{create_event_stream, send_typed_message, send_untyped_action, GroupBuffer},
        Request,
    },
    Bus, Message, Untyped,
};

/// Generic buffer unordered batched receiver that works with both sync and async handlers.
pub struct BufferUnorderedBatched<M, R, E, Mode>
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
pub type BufferUnorderedBatchedSync<M, R, E> = BufferUnorderedBatched<M, R, E, SyncExecution>;
pub type BufferUnorderedBatchedAsync<M, R, E> = BufferUnorderedBatched<M, R, E, AsyncExecution>;

/// The poller function that processes messages in batches.
///
/// Messages are buffered per group_id, ensuring each batch contains only
/// messages from the same group for proper group context propagation.
async fn buffer_unordered_batch_poller<T, M, R, E, Mode>(
    mut rx: mpsc::UnboundedReceiver<Request<M>>,
    bus: Bus,
    ut: Untyped,
    cfg: BufferUnorderedBatchedConfig,
    stx: mpsc::UnboundedSender<Event<R, E>>,
) where
    T: Send + Sync + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
    Mode: BatchExecutionMode<T, M, R, E>,
{
    assert!(cfg.max_parallel > 0, "max_parallel must be > 0");

    let handler = ut
        .downcast::<T>()
        .expect("handler type mismatch - this is a bug");
    let semaphore = Arc::new(tokio::sync::Semaphore::new(cfg.max_parallel));

    // Buffer messages by group_id so each batch contains only one group
    let mut buffers: HashMap<Option<GroupId>, GroupBuffer<M>> = HashMap::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            Request::Request(mid, msg, req, group_id) => {
                let buffer = buffers
                    .entry(group_id)
                    .or_insert_with(|| GroupBuffer::new(cfg.batch_size));
                buffer.push(mid, msg, req);

                if buffer.len() >= cfg.batch_size {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("semaphore closed unexpectedly");

                    let (batch_msgs, batch_mids) = buffer.take();
                    let batch_size = batch_msgs.len();

                    Mode::spawn_batch_handler(
                        handler.clone(),
                        batch_msgs,
                        batch_mids,
                        bus.clone(),
                        stx.clone(),
                        permit,
                        group_id,
                        batch_size,
                    );
                }
            }

            Request::Action(Action::Init(..)) => {
                let _ = stx.send(Event::Ready);
            }

            Request::Action(Action::Close) => {
                // Process remaining buffered messages before shutdown
                for (group_id, mut buffer) in buffers.drain() {
                    if !buffer.is_empty() {
                        let permit = semaphore
                            .clone()
                            .acquire_owned()
                            .await
                            .expect("semaphore closed unexpectedly");

                        let (batch_msgs, batch_mids) = buffer.take();
                        let batch_size = batch_msgs.len();

                        Mode::spawn_batch_handler(
                            handler.clone(),
                            batch_msgs,
                            batch_mids,
                            bus.clone(),
                            stx.clone(),
                            permit,
                            group_id,
                            batch_size,
                        );
                    }
                }

                // Wait for all in-flight tasks to complete
                let _ = semaphore.acquire_many(cfg.max_parallel as _).await;
                rx.close();
            }

            Request::Action(Action::Flush) => {
                // Process remaining messages in all buffers
                for (group_id, mut buffer) in buffers.drain() {
                    if !buffer.is_empty() {
                        let permit = semaphore
                            .clone()
                            .acquire_owned()
                            .await
                            .expect("semaphore closed unexpectedly");

                        let (batch_msgs, batch_mids) = buffer.take();
                        let batch_size = batch_msgs.len();

                        Mode::spawn_batch_handler(
                            handler.clone(),
                            batch_msgs,
                            batch_mids,
                            bus.clone(),
                            stx.clone(),
                            permit,
                            group_id,
                            batch_size,
                        );
                    }
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
    let (stx, srx) = mpsc::unbounded_channel();
    let (tx, rx) = mpsc::unbounded_channel();

    let poller = Box::new(move |ut| {
        Box::new(move |bus| {
            Box::pin(buffer_unordered_batch_poller::<T, M, R, E, Mode>(
                rx, bus, ut, cfg, stx,
            )) as Pin<Box<dyn Future<Output = ()> + Send>>
        }) as Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
    });

    (
        BufferUnorderedBatched {
            tx,
            srx: Mutex::new(Some(srx)),
            _mode: PhantomData,
        },
        poller,
    )
}

// ReceiverSubscriberBuilder implementations via macro
crate::impl_receiver_subscriber_builder!(
    BufferUnorderedBatchedSync<M, R, T::Error>,
    BatchHandler,
    BufferUnorderedBatchedConfig,
    SyncExecution,
    batched
);

crate::impl_receiver_subscriber_builder!(
    BufferUnorderedBatchedAsync<M, R, T::Error>,
    AsyncBatchHandler,
    BufferUnorderedBatchedConfig,
    AsyncExecution,
    batched_clone
);

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
    fn send(
        &self,
        mid: u64,
        m: M,
        req: bool,
        _bus: &Bus,
        group_id: Option<crate::group::GroupId>,
    ) -> Result<(), Error<M>> {
        send_typed_message(&self.tx, mid, m, req, group_id)
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
