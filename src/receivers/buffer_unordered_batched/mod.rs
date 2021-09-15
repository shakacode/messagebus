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
        fn buffer_unordered_batch_poller<$t, M, R>(
            mut rx: mpsc::UnboundedReceiver<Request<M>>,
            bus: Bus,
            ut: Untyped,
            stats: Arc<BufferUnorderedBatchedStats>,
            cfg: BufferUnorderedBatchedConfig,
            stx: mpsc::UnboundedSender<Event<R, $t::Error>>,
        ) -> impl Future<Output = ()>
        where
            $t: $h<M, Response = R> + 'static,
            M: Message,
            R: Message,
        {
            let ut = ut.downcast::<$t>().unwrap();
            let mut buffer_mid = Vec::with_capacity(cfg.batch_size);
            let mut buffer = Vec::with_capacity(cfg.batch_size);
            let mut queue = FuturesUnordered::new();
            let mut sync_future = None;
            let mut need_sync = false;
            let mut rx_closed = false;
            let mut need_flush = false;

            futures::future::poll_fn(move |cx| 'main: loop {
                if !rx_closed && !need_flush && !need_sync {
                    while queue.len() < cfg.max_parallel {
                        let mut do_break = false;
                        let mut drained = false;

                        match rx.poll_recv(cx) {
                            Poll::Ready(Some(a)) => match a {
                                Request::Request(mid, msg, req) => {
                                    stats.buffer.fetch_sub(1, Ordering::Relaxed);
                                    stats.batch.fetch_add(1, Ordering::Relaxed);

                                    buffer_mid.push((mid, req));
                                    buffer.push(msg);
                                }
                                Request::Action(Action::Init) => {
                                    stx.send(Event::Ready).ok();
                                }
                                Request::Action(Action::Flush) => need_flush = true,
                                Request::Action(Action::Close) => rx.close(),
                                Request::Action(Action::Sync) => {
                                    need_sync = true;
                                    do_break = true;
                                }
                                _ => unimplemented!(),
                            },
                            Poll::Ready(None) => {
                                need_sync = true;
                                rx_closed = true;
                                do_break = true;
                            }
                            Poll::Pending => {
                                drained = true;
                                do_break = true;
                            }
                        }

                        if !buffer.is_empty()
                            && (rx_closed
                                || need_flush
                                || need_sync
                                || (drained && cfg.when_ready)
                                || buffer.len() >= cfg.batch_size)
                        {
                            stats.batch.store(0, Ordering::Relaxed);
                            stats.parallel.fetch_add(1, Ordering::Relaxed);

                            let bus = bus.clone();
                            let ut = ut.clone();
                            let buffer_mid_clone = buffer_mid.drain(..).collect::<Vec<_>>();
                            let buffer_clone = buffer.drain(..).collect();
                            let stats = stats.clone();

                            queue.push(($st1)(buffer_clone, bus, ut, stats, buffer_mid_clone));
                        }

                        if do_break {
                            break;
                        }
                    }
                }

                let queue_len = queue.len();

                loop {
                    if queue_len != 0 {
                        let mut finished = 0;
                        loop {
                            match queue.poll_next_unpin(cx) {
                                Poll::Ready(Some((mids, res))) => match res {
                                    Ok(re) => {
                                        let mut mids = mids.into_iter();
                                        let mut re = re.into_iter();

                                        while let Some((mid, req)) = mids.next() {
                                            if req {
                                                if let Some(r) = re.next() {
                                                    stx.send(Event::Response(mid, Ok(r))).ok();
                                                } else {
                                                    stx.send(Event::Response(
                                                        mid,
                                                        Err(Error::NoResponse),
                                                    ))
                                                    .ok();
                                                }
                                            } else {
                                                finished += 1;
                                            }
                                        }
                                    }
                                    Err(er) => {
                                        for (mid, req) in mids {
                                            if req {
                                                stx.send(Event::Response(
                                                    mid,
                                                    Err(Error::Other(er.clone())),
                                                ))
                                                .ok();
                                            } else {
                                                finished += 1
                                            }
                                        }
                                        stx.send(Event::Error(er)).ok();
                                    }
                                },
                                Poll::Ready(None) => break,
                                Poll::Pending => {
                                    if finished > 0 {
                                        stx.send(Event::Finished(finished)).ok();
                                    }

                                    return Poll::Pending
                                },
                            }
                        }
                        
                        if finished > 0 {
                            stx.send(Event::Finished(finished)).ok();
                        }
                    }

                    if need_flush {
                        need_flush = false;
                        stx.send(Event::Flushed).ok();
                    }

                    if need_sync {
                        if let Some(fut) = sync_future.as_mut() {
                            match unsafe { fix_type(fut) }.poll(cx) {
                                Poll::Pending => return Poll::Pending,
                                Poll::Ready(resp) => {
                                    let resp: Result<_, $t::Error> = resp;
                                    stx.send(Event::Synchronized(resp.map_err(Error::Other)))
                                        .ok();
                                }
                            }
                            need_sync = false;
                            sync_future = None;
                            continue 'main;
                        } else {
                            sync_future.replace(($st2)(bus.clone(), ut.clone()));
                        }
                    } else {
                        break;
                    }
                }

                if queue_len == queue.len() {
                    return if rx_closed {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    };
                }
            })
        }
    };
}
