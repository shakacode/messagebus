mod execution;
mod receiver;

pub use receiver::{BufferUnorderedAsync, BufferUnorderedSync};
use serde::{Deserialize, Serialize};

/// Configuration for concurrent (buffer unordered) receivers.
///
/// Used with [`Handler`](crate::Handler) and [`AsyncHandler`](crate::AsyncHandler)
/// when subscribing with `subscribe_sync` or `subscribe_async`.
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
/// use messagebus::{Bus, AsyncHandler, error};
/// use messagebus::derive::Message;
/// use messagebus::receivers::BufferUnorderedConfig;
/// use async_trait::async_trait;
///
/// #[derive(Debug, Clone, Message)]
/// #[message(clone)]
/// struct MyMessage(String);
///
/// struct MyHandler;
///
/// #[async_trait]
/// impl AsyncHandler<MyMessage> for MyHandler {
///     type Error = error::GenericError;
///     type Response = ();
///     async fn handle(&self, _msg: MyMessage, _bus: &Bus) -> Result<(), Self::Error> { Ok(()) }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     let (bus, poller) = Bus::build()
///         .register(MyHandler)
///         .subscribe_async::<MyMessage>(8, BufferUnorderedConfig {
///             buffer_size: 16,
///             max_parallel: 4,
///         })
///         .done()
///         .build();
///     tokio::spawn(poller);
/// }
/// ```
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct BufferUnorderedConfig {
    /// Size of the internal message buffer.
    ///
    /// Higher values allow more messages to be queued before backpressure.
    /// Default: 8
    pub buffer_size: usize,

    /// Maximum number of messages to process concurrently.
    ///
    /// Controls parallelism for async handlers. Default: 8
    pub max_parallel: usize,
}

impl Default for BufferUnorderedConfig {
    fn default() -> Self {
        Self {
            buffer_size: 8,
            max_parallel: 8,
        }
    }
}
