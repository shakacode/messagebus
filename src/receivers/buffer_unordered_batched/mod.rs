mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use r#async::BufferUnorderedBatchedAsync;
use serde_derive::{Deserialize, Serialize};
pub use sync::BufferUnorderedBatchedSync;

#[allow(dead_code)]
#[derive(Debug)]
pub struct BufferUnorderedBatchedStats {
    pub buffer: AtomicU64,
    pub buffer_total: AtomicU64,
    pub parallel: AtomicU64,
    pub parallel_total: AtomicU64,
    pub batch: AtomicU64,
    pub batch_size: AtomicU64,
}

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
    /// Controls parallelism. Default: 2
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

#[macro_export]
macro_rules! buffer_unordered_batch_poller_macro {
    ($t: tt, $h: tt, $st1: expr, $st2: expr) => {
        async fn buffer_unordered_batch_poller<$t, M, R>(
            mut rx: mpsc::UnboundedReceiver<Request<M>>,
            bus: Bus,
            ut: Untyped,
            _stats: Arc<BufferUnorderedBatchedStats>,
            cfg: BufferUnorderedBatchedConfig,
            stx: mpsc::UnboundedSender<Event<R, $t::Error>>,
        ) where
            $t: $h<M, Response = R> + 'static,
            M: Message,
            R: Message,
        {
            let ut = ut.downcast::<$t>().unwrap();
            let semaphore = Arc::new(tokio::sync::Semaphore::new(cfg.max_parallel));

            let mut buffer_mid = Vec::with_capacity(cfg.batch_size);
            let mut buffer = Vec::with_capacity(cfg.batch_size);

            while let Some(msg) = rx.recv().await {
                let bus = bus.clone();
                let ut = ut.clone();
                let semaphore = semaphore.clone();
                let stx = stx.clone();

                match msg {
                    Request::Request(mid, msg, req) => {
                        buffer_mid.push((mid, req));
                        buffer.push(msg);

                        if buffer_mid.len() >= cfg.batch_size {
                            let task_permit = semaphore.acquire_owned().await;

                            let buffer_mid_clone = buffer_mid.drain(..).collect::<Vec<_>>();
                            let buffer_clone = buffer.drain(..).collect();

                            #[allow(clippy::redundant_closure_call, clippy::let_underscore_future)]
                            let _ =
                                ($st1)(buffer_mid_clone, buffer_clone, bus, ut, task_permit, stx);
                        }
                    }
                    Request::Action(Action::Init(..)) => {
                        stx.send(Event::Ready).unwrap();
                    }
                    Request::Action(Action::Close) => {
                        rx.close();
                    }
                    Request::Action(Action::Flush) => {
                        let stx_clone = stx.clone();

                        if !buffer_mid.is_empty() {
                            let buffer_mid_clone = buffer_mid.drain(..).collect::<Vec<_>>();
                            let buffer_clone = buffer.drain(..).collect();
                            let task_permit = semaphore.clone().acquire_owned().await;

                            #[allow(clippy::redundant_closure_call, clippy::let_underscore_future)]
                            let _ =
                                ($st1)(buffer_mid_clone, buffer_clone, bus, ut, task_permit, stx);
                        }

                        let _ = semaphore.acquire_many(cfg.max_parallel as _).await;
                        stx_clone.send(Event::Flushed).unwrap();
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
