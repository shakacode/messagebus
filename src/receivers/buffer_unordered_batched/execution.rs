use std::sync::Arc;

use tokio::sync::{mpsc::UnboundedSender, OwnedSemaphorePermit};

use crate::{
    error::{Error, StdSyncSendError},
    receiver::Event,
    AsyncBatchHandler, BatchHandler, Bus, Message,
};

/// Trait that abstracts over sync vs async execution modes for batch handlers.
pub trait BatchExecutionMode<T, M, R, E>: Send + Sync + 'static
where
    T: Send + Sync + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    /// Spawn a task to handle a batch of messages.
    fn spawn_batch_handler(
        handler: Arc<T>,
        msgs: Vec<M>,
        mids: Vec<(u64, bool)>,
        bus: Bus,
        response_tx: UnboundedSender<Event<R, E>>,
        permit: OwnedSemaphorePermit,
    );

    /// Call the handler's sync method.
    fn call_sync(
        handler: Arc<T>,
        bus: Bus,
    ) -> impl std::future::Future<Output = Result<(), E>> + Send;
}

/// Sync execution mode - spawns blocking tasks via `spawn_blocking`.
pub struct SyncExecution;

/// Async execution mode - spawns async tasks via `tokio::spawn`.
pub struct AsyncExecution;

impl<T, M, R> BatchExecutionMode<T, M, R, T::Error> for SyncExecution
where
    T: BatchHandler<M, Response = R> + 'static,
    T::Error: StdSyncSendError,
    M: Message,
    R: Message,
{
    fn spawn_batch_handler(
        handler: Arc<T>,
        msgs: Vec<M>,
        mids: Vec<(u64, bool)>,
        bus: Bus,
        response_tx: UnboundedSender<Event<R, T::Error>>,
        permit: OwnedSemaphorePermit,
    ) {
        tokio::task::spawn_blocking(move || {
            let batch: T::InBatch = msgs.into_iter().collect();
            let resp = handler.handle(batch, &bus);
            if let Err(err) = &resp {
                log::error!("BatchHandler error: {err}");
            }
            drop(permit);

            crate::process_batch_result!(resp, mids, response_tx);
        });
    }

    async fn call_sync(handler: Arc<T>, bus: Bus) -> Result<(), T::Error> {
        tokio::task::spawn_blocking(move || handler.sync(&bus))
            .await
            .expect("sync task panicked")
    }
}

impl<T, M, R> BatchExecutionMode<T, M, R, T::Error> for AsyncExecution
where
    T: AsyncBatchHandler<M, Response = R> + 'static,
    T::Error: StdSyncSendError,
    M: Message,
    R: Message,
{
    fn spawn_batch_handler(
        handler: Arc<T>,
        msgs: Vec<M>,
        mids: Vec<(u64, bool)>,
        bus: Bus,
        response_tx: UnboundedSender<Event<R, T::Error>>,
        permit: OwnedSemaphorePermit,
    ) {
        tokio::spawn(async move {
            let batch: T::InBatch = msgs.into_iter().collect();
            let resp = handler.handle(batch, &bus).await;
            if let Err(err) = &resp {
                log::error!("AsyncBatchHandler error: {err}");
            }
            drop(permit);

            crate::process_batch_result!(resp, mids, response_tx);
        });
    }

    async fn call_sync(handler: Arc<T>, bus: Bus) -> Result<(), T::Error> {
        handler.sync(&bus).await
    }
}
