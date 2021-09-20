mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use r#async::BufferUnorderedAsync;
use serde_derive::{Deserialize, Serialize};
pub use sync::BufferUnorderedSync;

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
            use futures::StreamExt;
    
            #[inline(always)]
            unsafe fn fix_type<'a, F, T>(x: &'a mut Option<F>) -> Option<Pin<&'a mut (impl Future<Output = T> + Send)>>
            where
                F: Future<Output = T> + Send,
            {
                Some(Pin::new_unchecked(x.as_mut()?))
            }

            let ut = ut.downcast::<$t>().unwrap();

            let mut queue = futures::stream::FuturesUnordered::new();
            let mut sync_future = None;
            let mut flushing = false;
            let mut closing = false;
            let mut count = cfg.max_parallel;

            futures::future::poll_fn(move |ctx| 'main: loop  {
                while let Poll::Ready(res) = queue.poll_next_unpin(ctx) {
                    if res.is_some() {
                        count += 1;
                    } else {
                        break;
                    }
                }

                if (closing || flushing || sync_future.is_some()) && count < cfg.max_parallel {
                    return Poll::Pending
                }

                if flushing {
                    flushing = false;
                    stx.send(Event::Flushed).unwrap();
                }
                
                if let Some(fut) = unsafe { fix_type(&mut sync_future) } {
                    let resp: Result<_, E> = futures::ready!(fut.poll(ctx));
                    stx.send(Event::Synchronized(resp.map_err(Error::Other)))
                                        .unwrap();

                    let _ = sync_future.take();
                }

                if closing {
                    return Poll::Ready(());
                }

                while count > 0 {
                    let msg = futures::ready!(rx.poll_recv(ctx));

                    if let Some(msg) = msg {
                        match msg {
                            Request::Request(mid, msg, _req) => {
                                count -= 1;

                                queue.push(($st1)(
                                    mid,
                                    msg,
                                    bus.clone(),
                                    ut.clone(),
                                    stx.clone(),
                                ));

                                ctx.waker().wake_by_ref();
                            }
                            Request::Action(Action::Init) => {
                                stx.send(Event::Ready).unwrap();
                            }
                            Request::Action(Action::Close) => {
                                rx.close();
                            }
                            Request::Action(Action::Flush) => {
                                flushing = true;
                                continue 'main;
                            }

                            Request::Action(Action::Sync) => {
                                sync_future.replace(($st2)(bus.clone(), ut.clone()));
                                continue 'main;
                            }

                            _ => unimplemented!(),
                        }
                    } else {
                        closing = true;
                        continue 'main;
                    }
                }

                return Poll::Pending;
            }).await;
        }
    };
}
