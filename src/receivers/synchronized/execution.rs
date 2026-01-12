use std::sync::Arc;

use futures::executor::block_on;
use tokio::{
    sync::{mpsc::UnboundedSender, Mutex},
    task::JoinHandle,
};

use crate::{
    error::{Error, StdSyncSendError},
    receiver::Event,
    AsyncSynchronizedHandler, Bus, Message, SynchronizedHandler,
};

/// Trait that abstracts over sync vs async execution modes for synchronized handlers.
///
/// Synchronized handlers use `Arc<Mutex<T>>` to allow mutable access to the handler.
pub trait SynchronizedExecutionMode<T, M, R, E>: Send + Sync + 'static
where
    T: Send + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    /// Spawn a task to handle the message. Returns a handle to await completion.
    fn spawn_handler(
        handler: Arc<Mutex<T>>,
        msg: M,
        bus: Bus,
        response_tx: UnboundedSender<Event<R, E>>,
        mid: u64,
    ) -> JoinHandle<()>;

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

impl<T, M, R, E> SynchronizedExecutionMode<T, M, R, E> for SyncExecution
where
    T: SynchronizedHandler<M, Response = R, Error = E> + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    fn spawn_handler(
        handler: Arc<Mutex<T>>,
        msg: M,
        bus: Bus,
        response_tx: UnboundedSender<Event<R, E>>,
        mid: u64,
    ) -> JoinHandle<()> {
        tokio::task::spawn_blocking(move || {
            let resp = block_on(handler.lock()).handle(msg, &bus);
            if let Err(err) = &resp {
                log::error!("SynchronizedHandler error: {err}");
            }

            if response_tx
                .send(Event::Response(mid, resp.map_err(Error::Other)))
                .is_err()
            {
                log::trace!("failed to send response - channel closed during shutdown");
            }
        })
    }

    async fn call_sync(handler: Arc<Mutex<T>>, bus: Bus) -> Result<(), E> {
        tokio::task::spawn_blocking(move || block_on(handler.lock()).sync(&bus))
            .await
            .expect("sync task panicked")
    }
}

impl<T, M, R, E> SynchronizedExecutionMode<T, M, R, E> for AsyncExecution
where
    T: AsyncSynchronizedHandler<M, Response = R, Error = E> + 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    fn spawn_handler(
        handler: Arc<Mutex<T>>,
        msg: M,
        bus: Bus,
        response_tx: UnboundedSender<Event<R, E>>,
        mid: u64,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let resp = handler.lock().await.handle(msg, &bus).await;
            if let Err(err) = &resp {
                log::error!("AsyncSynchronizedHandler error: {err}");
            }

            if response_tx
                .send(Event::Response(mid, resp.map_err(Error::Other)))
                .is_err()
            {
                log::trace!("failed to send response - channel closed during shutdown");
            }
        })
    }

    async fn call_sync(handler: Arc<Mutex<T>>, bus: Bus) -> Result<(), E> {
        handler.lock().await.sync(&bus).await
    }
}
