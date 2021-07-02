mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use r#async::SynchronizedAsync;
pub use sync::SynchronizedSync;
use serde_derive::{Serialize, Deserialize};

#[derive(Debug)]
pub struct SynchronizedStats {
    pub buffer: AtomicU64,
    pub buffer_total: AtomicU64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SynchronizedConfig {
    pub buffer_size: usize,
}

impl Default for SynchronizedConfig {
    fn default() -> Self {
        Self { buffer_size: 1 }
    }
}

#[macro_export]
macro_rules! synchronized_poller_macro {
    ($t: tt, $h: tt, $st1: expr, $st2: expr) => {
        fn synchronized_poller<$t, M, R>(
            mut rx: mpsc::UnboundedReceiver<Request<M>>,
            bus: Bus,
            ut: Untyped,
            stx: mpsc::UnboundedSender<Event<R, $t::Error>>,
        ) -> impl Future<Output = ()>
        where
            $t: $h<M, Response = R> + 'static,
            $t::Error: StdSyncSendError,
            M: Message,
            R: Message,
        {
            let ut = ut.downcast::<Mutex<T>>().unwrap();
            let mut handle_future = None;
            let mut sync_future = None;
            let mut need_sync = false;
            let mut rx_closed = false;

            futures::future::poll_fn(move |cx| loop {
                if let Some(fut) = handle_future.as_mut() {
                    // SAFETY: safe bacause pinnet to async generator `stack` which should be pinned
                    match unsafe { fix_type(fut) }.poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready((mid, resp)) => {
                            let resp: Result<_, $t::Error> = resp;
                            stx.send(Event::Response(mid, resp.map_err(Error::Other)))
                                .ok();
                        }
                    }
                }
                handle_future = None;

                if !rx_closed && !need_sync {
                    match rx.poll_recv(cx) {
                        Poll::Ready(Some(a)) => match a {
                            Request::Request(mid, msg) => {
                                handle_future.replace(($st1)(mid, msg, bus.clone(), ut.clone()));
                                continue;
                            }
                            Request::Action(Action::Flush) => {
                                stx.send(Event::Flushed).ok();
                                continue;
                            }
                            Request::Action(Action::Sync) => need_sync = true,
                            Request::Action(Action::Close) => {
                                rx.close();
                                continue;
                            }
                            _ => unimplemented!(),
                        },
                        Poll::Ready(None) => {
                            need_sync = true;
                            rx_closed = true;
                        }
                        Poll::Pending => {}
                    }
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
