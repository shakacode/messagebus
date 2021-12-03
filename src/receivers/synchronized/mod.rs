mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use r#async::SynchronizedAsync;
use serde_derive::{Deserialize, Serialize};
pub use sync::SynchronizedSync;

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
        async fn synchronized_poller<$t, M, R>(
            mut rx: mpsc::UnboundedReceiver<Request<M>>,
            bus: Bus,
            ut: Untyped,
            stx: mpsc::UnboundedSender<Event<R, $t::Error>>,
        ) where
            $t: $h<M, Response = R> + 'static,
            $t::Error: StdSyncSendError,
            M: Message,
            R: Message,
        {
            use futures::{future, pin_mut, select, FutureExt};
            let ut = ut.downcast::<Mutex<T>>().unwrap();
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
                    Request::Request(mid, msg, _req) =>
                    {
                        #[allow(clippy::redundant_closure_call)]
                        ($st1)(mid, msg, bus.clone(), ut.clone(), stx.clone())
                            .await
                            .unwrap()
                    }
                    Request::Action(Action::Init(..)) => {
                        stx.send(Event::Ready).unwrap();
                    }
                    Request::Action(Action::Close) => {
                        rx.close();
                    }
                    Request::Action(Action::Flush) => {
                        stx.send(Event::Flushed).unwrap();
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
