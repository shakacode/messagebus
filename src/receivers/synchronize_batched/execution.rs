use std::sync::Arc;

use futures::executor::block_on;
use tokio::sync::{mpsc::UnboundedSender, Mutex};

use crate::{
    error::{Error, StdSyncSendError},
    group::GroupId,
    receiver::Event,
    AsyncBatchSynchronizedHandler, BatchSynchronizedHandler, Bus, Message,
};

/// Trait that abstracts over sync vs async execution modes for synchronized batch handlers.
///
/// Batches are collected per group_id, so all messages in a batch belong to
/// the same group. This enables proper group context propagation and tracking.
pub trait BatchSynchronizedExecutionMode<T, M, R, E>: Send + Sync + 'static
where
    T: Send + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError + Clone,
{
    /// Spawn a task to handle a batch of messages.
    ///
    /// All messages in the batch belong to the same group (identified by `group_id`).
    /// The batch size is passed to properly decrement the group counter after completion.
    fn spawn_batch_handler(
        handler: Arc<Mutex<T>>,
        msgs: Vec<M>,
        mids: Vec<(u64, bool)>,
        bus: Bus,
        response_tx: UnboundedSender<Event<R, E>>,
        group_id: Option<GroupId>,
        batch_size: usize,
    ) -> tokio::task::JoinHandle<()>;

    /// Call the handler's sync method.
    fn call_sync(
        handler: Arc<Mutex<T>>,
        bus: Bus,
    ) -> impl std::future::Future<Output = Result<(), E>> + Send;
}

/// Sync execution mode - spawns blocking tasks via `spawn_blocking`.
pub struct SyncExecution;

/// Async execution mode - spawns async tasks via `tokio::spawn`.
pub struct AsyncExecution;

impl<T, M, R> BatchSynchronizedExecutionMode<T, M, R, T::Error> for SyncExecution
where
    T: BatchSynchronizedHandler<M, Response = R> + 'static,
    T::Error: StdSyncSendError + Clone,
    M: Message,
    R: Message,
{
    fn spawn_batch_handler(
        handler: Arc<Mutex<T>>,
        msgs: Vec<M>,
        mids: Vec<(u64, bool)>,
        bus: Bus,
        response_tx: UnboundedSender<Event<R, T::Error>>,
        group_id: Option<GroupId>,
        batch_size: usize,
    ) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn_blocking(move || {
            let batch: T::InBatch = msgs.into_iter().collect();
            // Propagate group_id via task-local for any nested sends
            let resp = if let Some(gid) = group_id {
                Bus::with_group_context(gid, || block_on(handler.lock()).handle(batch, &bus))
            } else {
                block_on(handler.lock()).handle(batch, &bus)
            };
            if let Err(err) = &resp {
                log::error!("BatchSynchronizedHandler error: {err}");
            }

            // Decrement group counter for all messages in the batch
            if let Some(gid) = group_id {
                bus.group_registry().decrement_by(gid, batch_size as u64);
            }

            crate::process_batch_result!(resp, mids, response_tx);
        })
    }

    async fn call_sync(handler: Arc<Mutex<T>>, bus: Bus) -> Result<(), T::Error> {
        tokio::task::spawn_blocking(move || block_on(handler.lock()).sync(&bus))
            .await
            .expect("sync task panicked")
    }
}

impl<T, M, R> BatchSynchronizedExecutionMode<T, M, R, T::Error> for AsyncExecution
where
    T: AsyncBatchSynchronizedHandler<M, Response = R> + 'static,
    T::Error: StdSyncSendError + Clone,
    M: Message,
    R: Message,
{
    fn spawn_batch_handler(
        handler: Arc<Mutex<T>>,
        msgs: Vec<M>,
        mids: Vec<(u64, bool)>,
        bus: Bus,
        response_tx: UnboundedSender<Event<R, T::Error>>,
        group_id: Option<GroupId>,
        batch_size: usize,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let batch: T::InBatch = msgs.into_iter().collect();
            // Propagate group_id via task-local for any nested sends
            let resp = Bus::with_group_context_async(group_id, async {
                handler.lock().await.handle(batch, &bus).await
            })
            .await;
            if let Err(err) = &resp {
                log::error!("AsyncBatchSynchronizedHandler error: {err}");
            }

            // Decrement group counter for all messages in the batch
            if let Some(gid) = group_id {
                bus.group_registry().decrement_by(gid, batch_size as u64);
            }

            crate::process_batch_result!(resp, mids, response_tx);
        })
    }

    async fn call_sync(handler: Arc<Mutex<T>>, bus: Bus) -> Result<(), T::Error> {
        handler.lock().await.sync(&bus).await
    }
}
