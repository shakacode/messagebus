mod execution;
mod receiver;

pub use receiver::{SynchronizedBatchedAsync, SynchronizedBatchedSync};
use serde::{Deserialize, Serialize};

/// Configuration for synchronized (sequential) batched receivers.
///
/// Used with [`BatchSynchronizedHandler`](crate::BatchSynchronizedHandler) and
/// [`AsyncBatchSynchronizedHandler`](crate::AsyncBatchSynchronizedHandler)
/// when subscribing with `subscribe_batch_sync` or `subscribe_batch_async` on
/// handlers registered with `register_unsync`.
///
/// # Important: Avoiding Deadlocks
///
/// Do not call [`Bus::flush_all()`](crate::Bus::flush_all) or
/// [`Bus::flush::<M>()`](crate::Bus::flush) from within a handler where `M` is
/// the same message type being handled. This creates a circular dependency that
/// will deadlock.
///
/// # Example
///
/// ```rust,no_run
/// use messagebus::{Bus, AsyncBatchSynchronizedHandler};
/// use messagebus::derive::{Message, Error as MbError};
/// use messagebus::receivers::SynchronizedBatchedConfig;
/// use async_trait::async_trait;
/// use thiserror::Error;
///
/// #[derive(Debug, Clone, Error, MbError)]
/// enum BatchError {
///     #[error("Batch processing failed")]
///     Failed,
/// }
///
/// #[derive(Debug, Clone, Message)]
/// #[message(clone)]
/// struct MyMessage(String);
///
/// struct MyBatchStatefulHandler {
///     count: u64,
/// }
///
/// impl MyBatchStatefulHandler {
///     fn new() -> Self { Self { count: 0 } }
/// }
///
/// #[async_trait]
/// impl AsyncBatchSynchronizedHandler<MyMessage> for MyBatchStatefulHandler {
///     type Error = BatchError;
///     type Response = ();
///     type InBatch = Vec<MyMessage>;
///     type OutBatch = Vec<()>;
///
///     async fn handle(&mut self, msgs: Vec<MyMessage>, _bus: &Bus) -> Result<Vec<()>, Self::Error> {
///         self.count += msgs.len() as u64;
///         Ok(vec![(); msgs.len()])
///     }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     let (bus, poller) = Bus::build()
///         .register_unsync(MyBatchStatefulHandler::new())
///         .subscribe_batch_async::<MyMessage>(32, SynchronizedBatchedConfig {
///             batch_size: 50,
///             ..Default::default()
///         })
///         .done()
///         .build();
///     tokio::spawn(poller);
/// }
/// ```
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct SynchronizedBatchedConfig {
    /// Size of the internal message buffer.
    ///
    /// Higher values allow more messages to be queued. Default: 4
    pub buffer_size: usize,

    /// Number of messages per batch.
    ///
    /// Messages are collected until this count is reached, then processed together.
    /// Default: 8
    pub batch_size: usize,

    /// If true, process partial batches immediately when available.
    ///
    /// Default: false (wait for full batches except on flush)
    pub when_ready: bool,
}

impl Default for SynchronizedBatchedConfig {
    fn default() -> Self {
        Self {
            buffer_size: 4,
            batch_size: 8,
            when_ready: false,
        }
    }
}
