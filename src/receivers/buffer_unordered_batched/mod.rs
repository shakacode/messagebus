mod execution;
mod receiver;

pub use receiver::{BufferUnorderedBatchedAsync, BufferUnorderedBatchedSync};
use serde::{Deserialize, Serialize};

/// Configuration for concurrent batched receivers.
///
/// Used with [`BatchHandler`](crate::BatchHandler) and [`AsyncBatchHandler`](crate::AsyncBatchHandler)
/// when subscribing with `subscribe_batch_sync` or `subscribe_batch_async`.
///
/// # Example
///
/// ```rust,no_run
/// use messagebus::{Bus, AsyncBatchHandler};
/// use messagebus::derive::{Message, Error as MbError};
/// use messagebus::receivers::BufferUnorderedBatchedConfig;
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
/// struct MyBatchHandler;
///
/// #[async_trait]
/// impl AsyncBatchHandler<MyMessage> for MyBatchHandler {
///     type Error = BatchError;
///     type Response = ();
///     type InBatch = Vec<MyMessage>;
///     type OutBatch = Vec<()>;
///
///     async fn handle(&self, msgs: Vec<MyMessage>, _bus: &Bus) -> Result<Vec<()>, Self::Error> {
///         Ok(vec![(); msgs.len()])
///     }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     let (bus, poller) = Bus::build()
///         .register(MyBatchHandler)
///         .subscribe_batch_async::<MyMessage>(64, BufferUnorderedBatchedConfig {
///             batch_size: 100,
///             max_parallel: 4,
///             ..Default::default()
///         })
///         .done()
///         .build();
///     tokio::spawn(poller);
/// }
/// ```
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct BufferUnorderedBatchedConfig {
    /// Size of the internal message buffer.
    ///
    /// Higher values allow more messages to be queued. Default: 8
    pub buffer_size: usize,

    /// Maximum number of batches to process concurrently.
    ///
    /// Controls parallelism. Must be > 0 (panics at runtime if 0). Default: 2
    pub max_parallel: usize,

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

impl Default for BufferUnorderedBatchedConfig {
    fn default() -> Self {
        Self {
            buffer_size: 8,
            max_parallel: 2,
            batch_size: 8,
            when_ready: false,
        }
    }
}
