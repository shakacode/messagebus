use std::{pin::Pin, sync::Arc};

use super::SynchronizedBatchedConfig;
use crate::{
    batch_synchronized_poller_macro,
    builder::ReceiverSubscriberBuilder,
    error::{Error, StdSyncSendError},
    receiver::{
        Action, Event, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver,
        UntypedPollerCallback,
    },
    receivers::Request,
    AsyncBatchSynchronizedHandler, Bus, Message, Untyped,
};

use futures::{Future, Stream};
use tokio::sync::{
    mpsc::{self, UnboundedSender},
    Mutex,
};

batch_synchronized_poller_macro! {
    T,
    AsyncBatchSynchronizedHandler,
    |mids: Vec<_>, msgs, bus, ut: Arc<Mutex<T>>, stx: UnboundedSender<_>| {
        tokio::spawn(async move {
            let resp = ut.lock().await.handle(msgs, &bus).await;
            if let Err(err) = &resp {
                log::error!("AsyncBatchSynchronizedHandler error: {err}");
            }

            crate::process_batch_result!(resp, mids, stx);
        })
    },
    |bus, ut: Arc<Mutex<T>>| { async move { ut.lock().await.sync(&bus).await } }
}

pub struct SynchronizedBatchedAsync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    tx: mpsc::UnboundedSender<Request<M>>,
    srx: parking_lot::Mutex<Option<mpsc::UnboundedReceiver<Event<R, E>>>>,
}

impl<T, M, R> ReceiverSubscriberBuilder<T, M, R, T::Error>
    for SynchronizedBatchedAsync<M, R, T::Error>
where
    T: AsyncBatchSynchronizedHandler<M, Response = R> + 'static,
    T::Error: StdSyncSendError + Clone,
    R: Message,
    M: Message,
{
    type Config = SynchronizedBatchedConfig;

    fn build(cfg: Self::Config) -> (Self, UntypedPollerCallback) {
        let (stx, srx) = mpsc::unbounded_channel();
        let (tx, rx) = mpsc::unbounded_channel();

        let poller = Box::new(move |ut| {
            Box::new(move |bus| {
                Box::pin(batch_synchronized_poller::<T, M, R>(rx, bus, ut, cfg, stx))
                    as Pin<Box<dyn Future<Output = ()> + Send>>
            }) as Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
        });

        (
            SynchronizedBatchedAsync::<M, R, T::Error> {
                tx,
                srx: parking_lot::Mutex::new(Some(srx)),
            },
            poller,
        )
    }
}

impl<M, R, E> SendUntypedReceiver for SynchronizedBatchedAsync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    fn send(&self, m: Action, _bus: &Bus) -> Result<(), Error<Action>> {
        match self.tx.send(Request::Action(m)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Action(msg))) => Err(Error::send_closed(msg)),
            _ => unimplemented!(),
        }
    }
}

impl<M, R, E> SendTypedReceiver<M> for SynchronizedBatchedAsync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    fn send(&self, mid: u64, m: M, req: bool, _bus: &Bus) -> Result<(), Error<M>> {
        match self.tx.send(Request::Request(mid, m, req)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Request(_, msg, _))) => {
                Err(Error::send_closed(msg))
            }
            _ => unimplemented!(),
        }
    }
}

impl<M, R, E> ReciveTypedReceiver<R, E> for SynchronizedBatchedAsync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    type Stream = Pin<Box<dyn Stream<Item = Event<R, E>> + Send>>;

    fn event_stream(&self, _: Bus) -> Self::Stream {
        let mut rx = self.srx.lock().take().unwrap();

        Box::pin(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
    }
}
