mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use r#async::SynchronizedBatchedAsync;
pub use sync::SynchronizedBatchedSync;
use serde_derive::{Serialize, Deserialize};


#[derive(Debug)]
pub struct SynchronizedBatchedStats {
    pub buffer: AtomicU64,
    pub buffer_total: AtomicU64,
    pub batch: AtomicU64,
    pub batch_size: AtomicU64,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct SynchronizedBatchedConfig {
    pub buffer_size: usize,
    pub batch_size: usize,
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
        fn batch_synchronized_poller<$t, M, R>(
            mut rx: mpsc::UnboundedReceiver<Request<M>>,
            bus: Bus,
            ut: Untyped,
            // stats: Arc<SynchronizedBatchedStats>,
            cfg: SynchronizedBatchedConfig,
            stx: mpsc::UnboundedSender<Event<R, $t::Error>>,
        ) -> impl Future<Output = ()>
        where
            $t: $h<M, Response = R> + 'static,
            $t::Error: StdSyncSendError + Clone,
            M: Message,
            R: Message,
        {
            let ut = ut.downcast::<Mutex<T>>().unwrap();
            let mut buffer_mid = Vec::with_capacity(cfg.batch_size);
            let mut buffer = Vec::with_capacity(cfg.batch_size);
            let mut handle_future = None;
            let mut sync_future = None;
            let mut need_sync = false;
            let mut rx_closed = false;
            let mut need_flush = false;

            futures::future::poll_fn(move |cx| loop {
                if let Some(fut) = handle_future.as_mut() {
                    // SAFETY: safe bacause pinnet to async generator `stack` which should be pinned
                    match unsafe { fix_type(fut) }.poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready((mids, res)) => match res {
                            Ok(re) => {
                                let mids: Vec<u64> = mids;
                                let re = fix_into_iter::<R, _>(re);

                                let mut mids = mids.into_iter();
                                let mut re = re.into_iter();

                                while let Some(mid) = mids.next() {
                                    if let Some(r) = re.next() {
                                        stx.send(Event::Response(mid, Ok(r))).ok();
                                    } else {
                                        stx.send(Event::Response(mid, Err(Error::NoResponse))).ok();
                                    }
                                }
                            }

                            Err(er) => {
                                let er: $t::Error = er;
                                for mid in mids {
                                    stx.send(Event::Response(mid, Err(Error::Other(er.clone()))))
                                        .ok();
                                }
                            }
                        },
                    }
                }
                handle_future = None;

                while !rx_closed && !need_sync && !need_flush {
                    match rx.poll_recv(cx) {
                        Poll::Ready(Some(a)) => match a {
                            Request::Request(mid, msg) => {
                                // stats.buffer.fetch_sub(1, Ordering::Relaxed);
                                // stats.batch.fetch_add(1, Ordering::Relaxed);

                                buffer_mid.push(mid);
                                buffer.push(msg);
                            }
                            Request::Action(Action::Flush) => need_flush = true,
                            Request::Action(Action::Sync) => need_sync = true,
                            Request::Action(Action::Close) => rx.close(),
                            _ => unimplemented!(),
                        },

                        Poll::Ready(None) => {
                            need_sync = true;
                            rx_closed = true;
                        }

                        Poll::Pending => {
                            if cfg.when_ready {
                                break;
                            } else {
                                return Poll::Pending;
                            }
                        }
                    }

                    if buffer_mid.len() >= cfg.batch_size {
                        break;
                    }
                }

                if !buffer.is_empty() {
                    // stats.batch.store(0, Ordering::Relaxed);

                    let buffer_mid_clone = buffer_mid.drain(..).collect::<Vec<_>>();
                    let buffer_clone = buffer.drain(..).collect();

                    handle_future.replace(($st1)(
                        buffer_mid_clone,
                        buffer_clone,
                        bus.clone(),
                        ut.clone(),
                    ));

                    continue;
                }

                if need_flush {
                    need_flush = false;
                    stx.send(Event::Flushed).ok();
                    continue;
                }

                if need_sync {
                    if let Some(fut) = sync_future.as_mut() {
                        // SAFETY: safe bacause pinnet to async generator `stack` which should be pinned
                        match unsafe { fix_type(fut) }.poll(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(resp) => {
                                need_sync = false;
                                let resp: Result<_, $t::Error> = resp;
                                stx.send(Event::Synchronized(resp.map_err(Error::Other)))
                                    .ok();
                            }
                        }
                        sync_future = None;
                    } else {
                        sync_future.replace(($st2)(bus.clone(), ut.clone()));
                    }
                }

                return if rx_closed {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                };
            })
        }
    };
}
