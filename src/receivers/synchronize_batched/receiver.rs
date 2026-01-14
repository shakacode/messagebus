use std::{collections::HashMap, marker::PhantomData, pin::Pin};

use futures::{Future, Stream};
use parking_lot::Mutex as ParkingLotMutex;
use tokio::sync::{mpsc, Mutex};

use super::{
    execution::{AsyncExecution, BatchSynchronizedExecutionMode, SyncExecution},
    SynchronizedBatchedConfig,
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
///
/// Messages are buffered per group_id, ensuring each batch contains only
/// messages from the same group for proper group context propagation.
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

    // Buffer messages by group_id so each batch contains only one group
    let mut buffers: HashMap<Option<GroupId>, GroupBuffer<M>> = HashMap::new();
    let mut pending_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            Request::Request(mid, msg, req, group_id) => {
                let buffer = buffers
                    .entry(group_id)
                    .or_insert_with(|| GroupBuffer::new(cfg.batch_size));
                buffer.push(mid, msg, req);

                if buffer.len() >= cfg.batch_size {
                    let (batch_msgs, batch_mids) = buffer.take();
                    let batch_size = batch_msgs.len();

                    let handle = Mode::spawn_batch_handler(
                        handler.clone(),
                        batch_msgs,
                        batch_mids,
                        bus.clone(),
                        stx.clone(),
                        group_id,
                        batch_size,
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
                // Process remaining messages in all buffers
                for (group_id, mut buffer) in buffers.drain() {
                    if !buffer.is_empty() {
                        let (batch_msgs, batch_mids) = buffer.take();
                        let batch_size = batch_msgs.len();

                        let handle = Mode::spawn_batch_handler(
                            handler.clone(),
                            batch_msgs,
                            batch_mids,
                            bus.clone(),
                            stx.clone(),
                            group_id,
                            batch_size,
                        );
                        pending_tasks.push(handle);
                    }
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

// ReceiverSubscriberBuilder implementations via macro
crate::impl_receiver_subscriber_builder!(
    SynchronizedBatchedSync<M, R, T::Error>,
    BatchSynchronizedHandler,
    SynchronizedBatchedConfig,
    SyncExecution,
    batched_clone
);

crate::impl_receiver_subscriber_builder!(
    SynchronizedBatchedAsync<M, R, T::Error>,
    AsyncBatchSynchronizedHandler,
    SynchronizedBatchedConfig,
    AsyncExecution,
    batched_clone
);

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
