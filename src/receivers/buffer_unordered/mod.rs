mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use r#async::BufferUnorderedAsync;
use serde_derive::{Deserialize, Serialize};
pub use sync::BufferUnorderedSync;

#[allow(dead_code)]
#[derive(Debug)]
pub struct BufferUnorderedStats {
    pub buffer: AtomicU64,
    pub buffer_total: AtomicU64,
    pub parallel: AtomicU64,
    pub parallel_total: AtomicU64,
}

/// Configuration for concurrent (buffer unordered) receivers.
///
/// Used with [`Handler`](crate::Handler) and [`AsyncHandler`](crate::AsyncHandler)
/// when subscribing with `subscribe_sync` or `subscribe_async`.
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

#[macro_export]
macro_rules! buffer_unordered_poller_macro {
    ($t: tt, $h: tt, $st1: expr, $st2: expr) => {
        async fn buffer_unordered_poller<$t, M, R, E>(
            mut rx: mpsc::UnboundedReceiver<Request<M>>,
            bus: Bus,
            ut: Untyped,
            _stats: Arc<BufferUnorderedStats>,
            cfg: BufferUnorderedConfig,
            stx: mpsc::UnboundedSender<Event<R, E>>,
        ) where
            $t: $h<M, Response = R, Error = E> + 'static,
            M: Message,
            R: Message,
            E: StdSyncSendError,
        {
            let ut = ut.downcast::<$t>().unwrap();
            let semaphore = Arc::new(tokio::sync::Semaphore::new(cfg.max_parallel));

            while let Some(msg) = rx.recv().await {
                match msg {
                    Request::Request(mid, msg, _req) => {
                        #[allow(clippy::redundant_closure_call, clippy::let_underscore_future)]
                        let _ = ($st1)(
                            mid,
                            msg,
                            bus.clone(),
                            ut.clone(),
                            stx.clone(),
                            semaphore.clone().acquire_owned().await,
                        );
                    }

                    Request::Action(Action::Init(..)) => stx.send(Event::Ready).unwrap(),
                    Request::Action(Action::Close) => rx.close(),

                    Request::Action(Action::Flush) => {
                        let _ = semaphore.acquire_many(cfg.max_parallel as _).await;
                        stx.send(Event::Flushed).unwrap();
                    }

                    Request::Action(Action::Sync) => {
                        let lock = semaphore.acquire_many(cfg.max_parallel as _).await;

                        #[allow(clippy::redundant_closure_call)]
                        let resp = ($st2)(bus.clone(), ut.clone()).await;
                        drop(lock);

                        stx.send(Event::Synchronized(resp.map_err(Error::Other)))
                            .unwrap();
                    }

                    _ => unimplemented!(),
                }
            }
        }
    };
}
