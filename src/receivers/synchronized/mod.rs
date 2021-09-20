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
            let ut = ut.downcast::<Mutex<T>>().unwrap();

            while let Some(msg) = rx.recv().await {
                match msg {
                    Request::Request(mid, msg, _req) => {
                        ($st1)(mid, msg, bus.clone(), ut.clone(), stx.clone())
                            .await
                            .unwrap()
                    }
                    Request::Action(Action::Init) => {
                        stx.send(Event::Ready).unwrap();
                    }
                    Request::Action(Action::Close) => {
                        rx.close();
                    }
                    Request::Action(Action::Flush) => {
                        stx.send(Event::Flushed).unwrap();
                    }
                    Request::Action(Action::Sync) => {
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
