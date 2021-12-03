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
            use futures::{future, pin_mut, select, FutureExt};

            let ut = ut.downcast::<$t>().unwrap();
            let semaphore = Arc::new(tokio::sync::Semaphore::new(cfg.max_parallel));

            let mut idling = true;

            loop {
                let wait_fut = async move {
                    if idling {
                        let () = future::pending().await;
                    } else {
                        let _ = tokio::time::sleep(std::time::Duration::from_millis(100))
                            .fuse()
                            .await;
                    }
                };

                pin_mut!(wait_fut);
                let msg = select! {
                    m = rx.recv().fuse() => if let Some(msg) = m {
                        msg
                    } else {
                        break;
                    },

                    _ = wait_fut.fuse() => {
                        idling = true;
                        stx.send(Event::IdleBegin).unwrap();
                        continue;
                    }
                };

                if idling {
                    stx.send(Event::IdleEnd).unwrap();
                }

                idling = false;

                match msg {
                    Request::Request(mid, msg, _req) => {
                        #[allow(clippy::redundant_closure_call)]
                        let _ = ($st1)(
                            mid,
                            msg,
                            bus.clone(),
                            ut.clone(),
                            stx.clone(),
                            semaphore.clone().acquire_owned().await,
                        );
                    }

                    Request::Action(Action::Init(..)) => stx.send(Event::Ready).unwrap(),
                    Request::Action(Action::Close) => rx.close(),

                    Request::Action(Action::Flush) => {
                        let _ = semaphore.acquire_many(cfg.max_parallel as _).await;
                        stx.send(Event::Flushed).unwrap();
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
