mod execution;
mod receiver;

pub use receiver::{SynchronizedAsync, SynchronizedSync};
use serde::{Deserialize, Serialize};

/// Configuration for synchronized (sequential) receivers.
///
/// Used with [`SynchronizedHandler`](crate::SynchronizedHandler) and
/// [`AsyncSynchronizedHandler`](crate::AsyncSynchronizedHandler)
/// when subscribing with `subscribe_sync` or `subscribe_async` on
/// handlers registered with `register_unsync`.
///
/// # Example
///
/// ```rust,no_run
/// use messagebus::{Bus, AsyncSynchronizedHandler, error};
/// use messagebus::derive::Message;
/// use messagebus::receivers::SynchronizedConfig;
/// use async_trait::async_trait;
///
/// #[derive(Debug, Clone, Message)]
/// #[message(clone)]
/// struct MyMessage(String);
///
/// struct MyStatefulHandler {
///     count: u64,
/// }
///
/// impl MyStatefulHandler {
///     fn new() -> Self { Self { count: 0 } }
/// }
///
/// #[async_trait]
/// impl AsyncSynchronizedHandler<MyMessage> for MyStatefulHandler {
///     type Error = error::GenericError;
///     type Response = ();
///     async fn handle(&mut self, _msg: MyMessage, _bus: &Bus) -> Result<(), Self::Error> {
///         self.count += 1;
///         Ok(())
///     }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     let (bus, poller) = Bus::build()
///         .register_unsync(MyStatefulHandler::new())
///         .subscribe_async::<MyMessage>(8, SynchronizedConfig {
///             buffer_size: 4,
///         })
///         .done()
///         .build();
///     tokio::spawn(poller);
/// }
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SynchronizedConfig {
    /// Size of the internal message buffer.
    ///
    /// Since messages are processed sequentially, this determines how many
    /// can be queued while waiting. Default: 1
    pub buffer_size: usize,
}

impl Default for SynchronizedConfig {
    fn default() -> Self {
        Self { buffer_size: 1 }
    }
}
