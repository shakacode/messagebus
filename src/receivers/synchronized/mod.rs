mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use r#async::SynchronizedAsync;
use serde_derive::{Deserialize, Serialize};
pub use sync::SynchronizedSync;

#[allow(dead_code)]
#[derive(Debug)]
pub struct SynchronizedStats {
    pub buffer: AtomicU64,
    pub buffer_total: AtomicU64,
}

/// Configuration for synchronized (sequential) receivers.
///
/// Used with [`SynchronizedHandler`](crate::SynchronizedHandler) and
/// [`AsyncSynchronizedHandler`](crate::AsyncSynchronizedHandler)
/// when subscribing with `subscribe_sync` or `subscribe_async` on
/// handlers registered with `register_unsync`.
///
/// # Example
///
/// ```rust,no_run
/// use messagebus::{Bus, AsyncSynchronizedHandler, error};
/// use messagebus::derive::Message;
/// use messagebus::receivers::SynchronizedConfig;
/// use async_trait::async_trait;
///
/// #[derive(Debug, Clone, Message)]
/// #[message(clone)]
/// struct MyMessage(String);
///
/// struct MyStatefulHandler {
///     count: u64,
/// }
///
/// impl MyStatefulHandler {
///     fn new() -> Self { Self { count: 0 } }
/// }
///
/// #[async_trait]
/// impl AsyncSynchronizedHandler<MyMessage> for MyStatefulHandler {
///     type Error = error::GenericError;
///     type Response = ();
///     async fn handle(&mut self, _msg: MyMessage, _bus: &Bus) -> Result<(), Self::Error> {
///         self.count += 1;
///         Ok(())
///     }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     let (bus, poller) = Bus::build()
///         .register_unsync(MyStatefulHandler::new())
///         .subscribe_async::<MyMessage>(8, SynchronizedConfig {
///             buffer_size: 4,
///         })
///         .done()
///         .build();
///     tokio::spawn(poller);
/// }
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SynchronizedConfig {
    /// Size of the internal message buffer.
    ///
    /// Since messages are processed sequentially, this determines how many
    /// can be queued while waiting. Default: 1
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
