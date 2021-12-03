use std::{pin::Pin, sync::Arc};

use crate::{receiver::UntypedPollerCallback, synchronized_poller_macro};
use futures::{executor::block_on, Future, Stream};

use super::SynchronizedConfig;
use crate::{
    builder::ReceiverSubscriberBuilder,
    error::{Error, StdSyncSendError},
    receiver::{Action, Event, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver},
    receivers::Request,
    Bus, Message, SynchronizedHandler, Untyped,
};
use tokio::sync::{
    mpsc::{self, UnboundedSender},
    Mutex,
};

synchronized_poller_macro! {
    T,
    SynchronizedHandler,
    |mid, msg, bus, ut: Arc<Mutex<T>>, stx: UnboundedSender<_>| {
        tokio::task::spawn_blocking(move || {
            let resp = block_on(ut.lock()).handle(msg, &bus);

            stx.send(Event::Response(mid, resp.map_err(Error::Other)))
                .unwrap();
        })
    },
    |bus, ut: Arc<Mutex<T>>| async move {
        tokio::task::spawn_blocking(move || block_on(ut.lock()).sync(&bus))
            .await
            .unwrap()
    }
}

pub struct SynchronizedSync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    tx: mpsc::UnboundedSender<Request<M>>,
    srx: parking_lot::Mutex<Option<mpsc::UnboundedReceiver<Event<R, E>>>>,
}

impl<T, M, R, E> ReceiverSubscriberBuilder<T, M, R, E> for SynchronizedSync<M, R, E>
where
    T: SynchronizedHandler<M, Response = R, Error = E> + 'static,
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
            SynchronizedSync::<M, R, E> {
                tx,
                srx: parking_lot::Mutex::new(Some(srx)),
            },
            poller,
        )
    }
}

impl<M, R, E> SendUntypedReceiver for SynchronizedSync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    fn send(&self, msg: Action, _bus: &Bus) -> Result<(), Error<Action>> {
        match self.tx.send(Request::Action(msg)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Action(msg))) => Err(Error::send_closed(msg)),
            _ => unimplemented!(),
        }
    }
}

impl<M, R, E> SendTypedReceiver<M> for SynchronizedSync<M, R, E>
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

impl<M, R, E> ReciveTypedReceiver<R, E> for SynchronizedSync<M, R, E>
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
