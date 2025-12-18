mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use r#async::SynchronizedBatchedAsync;
use serde_derive::{Deserialize, Serialize};
pub use sync::SynchronizedBatchedSync;

#[allow(dead_code)]
#[derive(Debug)]
pub struct SynchronizedBatchedStats {
    pub buffer: AtomicU64,
    pub buffer_total: AtomicU64,
    pub batch: AtomicU64,
    pub batch_size: AtomicU64,
}

/// Configuration for synchronized (sequential) batched receivers.
///
/// Used with [`BatchSynchronizedHandler`](crate::BatchSynchronizedHandler) and
/// [`AsyncBatchSynchronizedHandler`](crate::AsyncBatchSynchronizedHandler)
/// when subscribing with `subscribe_batch_sync` or `subscribe_batch_async` on
/// handlers registered with `register_unsync`.
///
/// # Example
///
/// ```rust,ignore
/// Bus::build()
///     .register_unsync(MyBatchStatefulHandler::new())
///     .subscribe_batch_async::<MyMessage>(32, SynchronizedBatchedConfig {
///         batch_size: 50,
///         ..Default::default()
///     })
///     .done()
///     .build();
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

#[macro_export]
macro_rules! batch_synchronized_poller_macro {
    ($t: tt, $h: tt, $st1: expr, $st2: expr) => {
        async fn batch_synchronized_poller<$t, M, R>(
            mut rx: mpsc::UnboundedReceiver<Request<M>>,
            bus: Bus,
            ut: Untyped,
            // stats: Arc<SynchronizedBatchedStats>,
            cfg: SynchronizedBatchedConfig,
            stx: mpsc::UnboundedSender<Event<R, $t::Error>>,
        ) where
            $t: $h<M, Response = R> + 'static,
            $t::Error: StdSyncSendError + Clone,
            M: Message,
            R: Message,
        {
            let ut = ut.downcast::<Mutex<T>>().unwrap();

            let mut buffer_mid = Vec::with_capacity(cfg.batch_size);
            let mut buffer = Vec::with_capacity(cfg.batch_size);

            while let Some(msg) = rx.recv().await {
                let bus = bus.clone();
                let ut = ut.clone();
                let stx = stx.clone();

                match msg {
                    Request::Request(mid, msg, req) => {
                        buffer_mid.push((mid, req));
                        buffer.push(msg);

                        if buffer_mid.len() >= cfg.batch_size {
                            let buffer_mid_clone = buffer_mid.drain(..).collect::<Vec<_>>();
                            let buffer_clone = buffer.drain(..).collect();

                            #[allow(clippy::redundant_closure_call, clippy::let_underscore_future)]
                            let _ = ($st1)(buffer_mid_clone, buffer_clone, bus, ut, stx);
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

                            #[allow(clippy::redundant_closure_call, clippy::let_underscore_future)]
                            let _ = ($st1)(buffer_mid_clone, buffer_clone, bus, ut, stx);
                        }

                        stx_clone.send(Event::Flushed).unwrap();
                    }

                    Request::Action(Action::Sync) => {
                        #[allow(clippy::redundant_closure_call)]
                        let resp = ($st2)(bus.clone(), ut.clone()).await;
                        stx.send(Event::Synchronized(resp.map_err(Error::Other)))
                            .unwrap();
                    }

                    _ => unimplemented!(),
                }
            }
        }
    };
}
