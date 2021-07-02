mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use r#async::BufferUnorderedAsync;
pub use sync::BufferUnorderedSync;
use serde_derive::{Serialize, Deserialize};

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

#[macro_export]
macro_rules! buffer_unordered_poller_macro {
    ($t: tt, $h: tt, $st1: expr, $st2: expr) => {
        fn buffer_unordered_poller<$t, M, R, E>(
            mut rx: mpsc::UnboundedReceiver<Request<M>>,
            bus: Bus,
            ut: Untyped,
            stats: Arc<BufferUnorderedStats>,
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
            let mut queue = FuturesUnordered::new();
            let mut sync_future = None;
            let mut need_sync = false;
            let mut rx_closed = false;
            let mut need_flush = false;

            futures::future::poll_fn(move |cx| loop {
                if !rx_closed && !need_flush && !need_sync {
                    while queue.len() < cfg.max_parallel {
                        match rx.poll_recv(cx) {
                            Poll::Ready(Some(a)) => match a {
                                Request::Request(mid, msg) => {
                                    stats.buffer.fetch_sub(1, Ordering::Relaxed);
                                    stats.parallel.fetch_add(1, Ordering::Relaxed);
                                    queue.push(($st1)(
                                        mid,
                                        msg,
                                        bus.clone(),
                                        ut.clone(),
                                        stats.clone(),
                                    ));
                                }
                                Request::Action(Action::Flush) => need_flush = true,
                                Request::Action(Action::Close) => rx.close(),
                                Request::Action(Action::Sync) => {
                                    need_sync = true;
                                    break;
                                }
                                _ => unimplemented!(),
                            },
                            Poll::Ready(None) => {
                                need_sync = true;
                                rx_closed = true;
                                break;
                            }
                            Poll::Pending => break,
                        }
                    }
                }

                let queue_len = queue.len();

                loop {
                    if queue_len != 0 {
                        loop {
                            match queue.poll_next_unpin(cx) {
                                Poll::Pending => return Poll::Pending,
                                Poll::Ready(Some((mid, resp))) => {
                                    let resp: Result<_, $t::Error> = resp;
                                    stx.send(Event::Response(mid, resp.map_err(Error::Other)))
                                        .ok();
                                }
                                Poll::Ready(None) => break,
                            }
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
                                    let resp: Result<_, E> = resp;
                                    stx.send(Event::Synchronized(resp.map_err(Error::Other)))
                                        .ok();
                                }
                            }
                            need_sync = false;
                            sync_future = None;
                            continue;
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
