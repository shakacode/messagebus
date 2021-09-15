mod r#async;
mod sync;

use std::sync::{Arc, atomic::AtomicU64};

pub use r#async::BufferUnorderedAsync;
use serde_derive::{Deserialize, Serialize};
pub use sync::BufferUnorderedSync;
use tokio::sync::{RwLock, Semaphore};

#[derive(Debug)]
pub struct BufferUnorderedStats {
    pub buffer: AtomicU64,
    pub buffer_total: AtomicU64,
    pub parallel: AtomicU64,
    pub parallel_total: AtomicU64,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct BufferUnorderedConfig {
    pub buffer_size: usize,
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

#[derive(Clone)]
struct ConcurrentState {
    flush_lock: Arc<RwLock<()>>,
    semaphore: Arc<Semaphore>,
}

#[macro_export]
macro_rules! buffer_unordered_poller_macro {
    ($t: tt, $h: tt, $st1: expr, $st2: expr) => {
        fn buffer_unordered_poller<$t, M, R, E>(
            mut rx: mpsc::UnboundedReceiver<Request<M>>,
            bus: Bus,
            ut: Untyped,
            _stats: Arc<BufferUnorderedStats>,
            cfg: BufferUnorderedConfig,
            stx: mpsc::UnboundedSender<Event<R, E>>,
        ) -> impl Future<Output = ()>
        where
            $t: $h<M, Response = R, Error = E> + 'static,
            M: Message,
            R: Message,
            E: StdSyncSendError,
        {
            let ut = ut.downcast::<$t>().unwrap();

            let state = super::ConcurrentState {
                flush_lock: Arc::new(tokio::sync::RwLock::new(())),
                semaphore: Arc::new(tokio::sync::Semaphore::new(cfg.max_parallel)),
            };

            async move {
                while let Some(msg) = rx.recv().await {
                    match msg {
                        Request::Request(mid, msg, _req) => {
                            let bus = bus.clone();
                            let ut = ut.clone();
                            let state = state.clone();
                            let stx = stx.clone();

                            let task_permit = state.semaphore.acquire_owned().await;
                            let flush_permit = state.flush_lock.read_owned().await;

                            let _ = tokio::spawn(async move {
                                let resp = ($st1)(msg, bus, ut)
                                    .await;
                                
                                drop(task_permit);
                                drop(flush_permit);

                                stx.send(Event::Response(mid, resp.map_err(Error::Other)))
                                    .unwrap();
                            });
                        }
                        Request::Action(Action::Init)  => { stx.send(Event::Ready).unwrap(); }
                        Request::Action(Action::Close) => { rx.close(); }
                        Request::Action(Action::Flush) => { 
                            state.flush_lock.write().await; 
                            stx.send(Event::Flushed).unwrap();
                        }

                        Request::Action(Action::Sync) => {
                            let lock = state.flush_lock.write().await; 
                            let resp = ($st2)(bus.clone(), ut.clone()).await;
                            drop(lock);
                            stx.send(Event::Synchronized(resp.map_err(Error::Other)))
                                .unwrap();
                        }

                        _ => unimplemented!(),
                    }
                }
            }
        }
    };
}
