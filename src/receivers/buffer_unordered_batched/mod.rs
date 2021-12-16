mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use r#async::BufferUnorderedBatchedAsync;
use serde_derive::{Deserialize, Serialize};
pub use sync::BufferUnorderedBatchedSync;

#[derive(Debug)]
pub struct BufferUnorderedBatchedStats {
    pub buffer: AtomicU64,
    pub buffer_total: AtomicU64,
    pub parallel: AtomicU64,
    pub parallel_total: AtomicU64,
    pub batch: AtomicU64,
    pub batch_size: AtomicU64,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct BufferUnorderedBatchedConfig {
    pub buffer_size: usize,
    pub max_parallel: usize,
    pub batch_size: usize,
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

                            #[allow(clippy::redundant_closure_call)]
                            let _ = ($st1)(
                                buffer_mid_clone,
                                buffer_clone,
                                bus,
                                ut,
                                task_permit,
                                stx,
                            );
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

                            #[allow(clippy::redundant_closure_call)]
                            let _ = ($st1)(
                                buffer_mid_clone,
                                buffer_clone,
                                bus,
                                ut,
                                task_permit,
                                stx,
                            );
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
