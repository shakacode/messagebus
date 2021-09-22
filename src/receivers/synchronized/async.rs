use std::{pin::Pin, sync::Arc};

use crate::{receiver::UntypedPollerCallback, synchronized_poller_macro};
use futures::{Future, Stream};

use super::SynchronizedConfig;
use crate::{
    builder::ReceiverSubscriberBuilder,
    error::{Error, SendError, StdSyncSendError},
    receiver::{Action, Event, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver},
    receivers::Request,
    AsyncSynchronizedHandler, Bus, Message, Untyped,
};
use tokio::sync::{
    mpsc::{self, UnboundedSender},
    Mutex,
};

synchronized_poller_macro! {
    T,
    AsyncSynchronizedHandler,
    |mid, msg, bus, ut: Arc<Mutex<T>>, stx: UnboundedSender<_>| {
        tokio::spawn(async move {
            let resp = ut.lock().await.handle(msg, &bus).await;

            stx.send(Event::Response(mid, resp.map_err(Error::Other)))
                .unwrap();
        })
    },

    |bus, ut: Arc<Mutex<T>>| async move {
        ut.lock().await.sync(&bus).await
    }
}

pub struct SynchronizedAsync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    tx: mpsc::UnboundedSender<Request<M>>,
    srx: parking_lot::Mutex<Option<mpsc::UnboundedReceiver<Event<R, E>>>>,
}

impl<T, M, R, E> ReceiverSubscriberBuilder<T, M, R, E> for SynchronizedAsync<M, R, E>
where
    T: AsyncSynchronizedHandler<M, Response = R, Error = E> + 'static,
    R: Message,
    M: Message,
    E: StdSyncSendError,
{
    type Config = SynchronizedConfig;

    fn build(_cfg: Self::Config) -> (Self, UntypedPollerCallback) {
        let (stx, srx) = mpsc::unbounded_channel();
        let (tx, rx) = mpsc::unbounded_channel();

        let poller = Box::new(move |ut| {
            Box::new(move |bus| {
                Box::pin(synchronized_poller::<T, M, R>(rx, bus, ut, stx))
                    as Pin<Box<dyn Future<Output = ()> + Send>>
            }) as Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
        });

        (
            SynchronizedAsync::<M, R, E> {
                tx,
                srx: parking_lot::Mutex::new(Some(srx)),
            },
            poller,
        )
    }
}

impl<M, R, E> SendUntypedReceiver for SynchronizedAsync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    fn send(&self, m: Action, _bus: &Bus) -> Result<(), SendError<Action>> {
        match self.tx.send(Request::Action(m)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Action(msg))) => Err(SendError::Closed(msg)),
            _ => unimplemented!(),
        }
    }
}

impl<M, R, E> SendTypedReceiver<M> for SynchronizedAsync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    fn send(&self, mid: u64, m: M, req: bool, _bus: &Bus) -> Result<(), SendError<M>> {
        match self.tx.send(Request::Request(mid, m, req)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Request(_, msg, _))) => Err(SendError::Closed(msg)),
            _ => unimplemented!(),
        }
    }
}

impl<M, R, E> ReciveTypedReceiver<R, E> for SynchronizedAsync<M, R, E>
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
