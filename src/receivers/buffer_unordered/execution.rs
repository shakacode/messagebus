use std::sync::Arc;

use tokio::sync::{mpsc::UnboundedSender, OwnedSemaphorePermit};

use crate::{
    error::{Error, StdSyncSendError},
    receiver::Event,
    AsyncHandler, Bus, Handler, Message,
};

/// Trait that abstracts over sync vs async execution modes.
///
/// This allows the receiver implementation to be generic over how tasks
/// are spawned and how handlers are invoked, eliminating code duplication
/// between sync and async variants.
pub trait ExecutionMode<T, M, R, E>: Send + Sync + 'static
where
    T: Send + Sync + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    /// Spawn a task to handle the message.
    ///
    /// The implementation should:
    /// 1. Invoke the handler with the message
    /// 2. Send the response back via `response_tx`
    /// 3. Drop the permit when done to release the semaphore
    fn spawn_handler(
        handler: Arc<T>,
        msg: M,
        bus: Bus,
        response_tx: UnboundedSender<Event<R, E>>,
        mid: u64,
        permit: OwnedSemaphorePermit,
    );

    /// Call the handler's sync method.
    ///
    /// Returns a future that completes when sync is done.
    fn call_sync(
        handler: Arc<T>,
        bus: Bus,
    ) -> impl std::future::Future<Output = Result<(), E>> + Send;
}

/// Sync execution mode - spawns blocking tasks via `spawn_blocking`.
pub struct SyncExecution;

/// Async execution mode - spawns async tasks via `tokio::spawn`.
pub struct AsyncExecution;

impl<T, M, R, E> ExecutionMode<T, M, R, E> for SyncExecution
where
    T: Handler<M, Response = R, Error = E> + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    fn spawn_handler(
        handler: Arc<T>,
        msg: M,
        bus: Bus,
        response_tx: UnboundedSender<Event<R, E>>,
        mid: u64,
        permit: OwnedSemaphorePermit,
    ) {
        tokio::task::spawn_blocking(move || {
            let resp = handler.handle(msg, &bus);
            if let Err(err) = &resp {
                log::error!("Handler error: {err}");
            }
            drop(permit);

            if response_tx
                .send(Event::Response(mid, resp.map_err(Error::Other)))
                .is_err()
            {
                log::trace!("failed to send response - channel closed during shutdown");
            }
        });
    }

    async fn call_sync(handler: Arc<T>, bus: Bus) -> Result<(), E> {
        tokio::task::spawn_blocking(move || handler.sync(&bus))
            .await
            .expect("sync task panicked")
    }
}

impl<T, M, R, E> ExecutionMode<T, M, R, E> for AsyncExecution
where
    T: AsyncHandler<M, Response = R, Error = E> + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    fn spawn_handler(
        handler: Arc<T>,
        msg: M,
        bus: Bus,
        response_tx: UnboundedSender<Event<R, E>>,
        mid: u64,
        permit: OwnedSemaphorePermit,
    ) {
        tokio::spawn(async move {
            let resp = handler.handle(msg, &bus).await;
            if let Err(err) = &resp {
                log::error!("AsyncHandler error: {err}");
            }
            drop(permit);

            if response_tx
                .send(Event::Response(mid, resp.map_err(Error::Other)))
                .is_err()
            {
                log::trace!("failed to send response - channel closed during shutdown");
            }
        });
    }

    async fn call_sync(handler: Arc<T>, bus: Bus) -> Result<(), E> {
        handler.sync(&bus).await
    }
}
