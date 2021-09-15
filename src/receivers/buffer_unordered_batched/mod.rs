mod r#async;
mod sync;

use std::sync::{Arc, atomic::AtomicU64};

pub use r#async::BufferUnorderedBatchedAsync;
use serde_derive::{Deserialize, Serialize};
pub use sync::BufferUnorderedBatchedSync;
use tokio::sync::{RwLock, Semaphore};

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

#[derive(Clone)]
struct ConcurrentState {
    flush_lock: Arc<RwLock<()>>,
    semaphore: Arc<Semaphore>,
}

#[macro_export]
macro_rules! buffer_unordered_batch_poller_macro {
    ($t: tt, $h: tt, $st1: expr, $st2: expr) => {
        fn buffer_unordered_batch_poller<$t, M, R>(
            mut rx: mpsc::UnboundedReceiver<Request<M>>,
            bus: Bus,
            ut: Untyped,
            _stats: Arc<BufferUnorderedBatchedStats>,
            cfg: BufferUnorderedBatchedConfig,
            stx: mpsc::UnboundedSender<Event<R, $t::Error>>,
        ) -> impl Future<Output = ()>
        where
            $t: $h<M, Response = R> + 'static,
            M: Message,
            R: Message,
        {
            let ut = ut.downcast::<$t>().unwrap();

            let state = super::ConcurrentState {
                flush_lock: Arc::new(tokio::sync::RwLock::new(())),
                semaphore: Arc::new(tokio::sync::Semaphore::new(cfg.max_parallel)),
            };

            async move {
                let mut buffer_mid = Vec::with_capacity(cfg.batch_size);
                let mut buffer = Vec::with_capacity(cfg.batch_size);

                while let Some(msg) = rx.recv().await {
                    let bus = bus.clone();
                    let ut = ut.clone();
                    let state = state.clone();
                    let stx = stx.clone();
                    
                    match msg {
                        Request::Request(mid, msg, req) => {
                            buffer_mid.push((mid, req));
                            buffer.push(msg);

                            if buffer_mid.len() >= cfg.batch_size {
                                let task_permit = state.semaphore.acquire_owned().await;
                                let flush_permit = state.flush_lock.read_owned().await;

                                let buffer_mid_clone = buffer_mid.drain(..).collect::<Vec<_>>();
                                let buffer_clone = buffer.drain(..).collect();

                                let _ = tokio::spawn(async move {
                                    let resp = ($st1)(buffer_clone, bus, ut).await;
                                    
                                    drop(task_permit);
                                    drop(flush_permit);

                                    let mids = buffer_mid_clone.into_iter();
                                    
                                    match resp {
                                        Ok(re) => {
                                            let mut mids = mids.into_iter();
                                            let mut re = re.into_iter();

                                            while let Some((mid, _req)) = mids.next() {
                                                if let Some(r) = re.next() {
                                                    stx.send(Event::Response(mid, Ok(r)))
                                                        .unwrap();
                                                } else {
                                                    stx.send(Event::Response(mid, Err(Error::NoResponse)))
                                                        .unwrap();
                                                }
                                            }
                                        }
                                        Err(er) => {
                                            for (mid, _req) in mids {
                                                stx.send(Event::Response(
                                                    mid,
                                                    Err(Error::Other(er.clone())),
                                                )).unwrap();
                                            }

                                            stx.send(Event::Error(er)).unwrap();
                                        }
                                    }
                                });
                            }
                        }
                        Request::Action(Action::Init)  => { stx.send(Event::Ready).unwrap(); }
                        Request::Action(Action::Close) => { rx.close(); }
                        Request::Action(Action::Flush) => {
                            let stx_clone = stx.clone();

                            if !buffer_mid.is_empty() {
                                let task_permit = state.semaphore.acquire_owned().await;
                                let flush_permit = state.flush_lock.clone().read_owned().await;

                                let buffer_mid_clone = buffer_mid.drain(..).collect::<Vec<_>>();
                                let buffer_clone = buffer.drain(..).collect();

                                let _ = tokio::spawn(async move {
                                    let resp = ($st1)(buffer_clone, bus, ut).await;
                                    
                                    drop(task_permit);
                                    drop(flush_permit);

                                    let mids = buffer_mid_clone.into_iter();
                                    
                                    match resp {
                                        Ok(re) => {
                                            let mut mids = mids.into_iter();
                                            let mut re = re.into_iter();

                                            while let Some((mid, _req)) = mids.next() {
                                                if let Some(r) = re.next() {
                                                    stx.send(Event::Response(mid, Ok(r)))
                                                        .unwrap();
                                                } else {
                                                    stx.send(Event::Response(mid, Err(Error::NoResponse)))
                                                        .unwrap();
                                                }
                                            }
                                        }
                                        Err(er) => {
                                            for (mid, _req) in mids {
                                                stx.send(Event::Response(
                                                    mid,
                                                    Err(Error::Other(er.clone())),
                                                )).unwrap();
                                            }

                                            stx.send(Event::Error(er)).unwrap();
                                        }
                                    }
                                });
                            }

                            state.flush_lock.write().await; 
                            stx_clone.send(Event::Flushed).unwrap();
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
