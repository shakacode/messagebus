use std::pin::Pin;

use futures::{pin_mut, Future, Stream};
use tokio::sync::{mpsc, Mutex};

use crate::builder::ReceiverSubscriberBuilder;
use crate::error::{Error, StdSyncSendError};
use crate::handler::AsyncProducer as AsyncProducerHandler;
use crate::receiver::UntypedPollerCallback;
use crate::receivers::Request;
use crate::{
    Action, Bus, Event, Message, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver,
    Untyped,
};

#[derive(Default)]
pub struct AsyncProducerConfig {}

async fn producer_poller<T, M>(
    mut rx: mpsc::UnboundedReceiver<Request<M>>,
    bus: Bus,
    ut: Untyped,
    stx: mpsc::UnboundedSender<Event<T::Response, T::Error>>,
) where
    T: AsyncProducerHandler<M> + 'static,
    T::Error: StdSyncSendError,
    T::Item: Message,
    T::Response: Message,
    M: Message,
{
    let ut = ut.downcast::<Mutex<T>>().expect("handler type mismatch - this is a bug");
    let stream = Option<Pin<Box<dyn Stream<Item = Result<Self::Item, Self::Error>> + Send + '_>>, Self::Error>;

    while let Some(msg) = rx.recv().await {
        match msg {
            Request::Request(mid, msg, _req) => {
                let lock = ut.lock().await;
                match lock.producer(msg, &bus).await {
                    Ok(stream) => {
                        pin_mut!(stream);
                        let _ = stx.send(Event::BatchComplete(M::type_tag_(), 1));
                    }
                    Err(err) => {
                        let _ = stx.send(Event::Error(Error::Other(err)));
                    }
                }
            }
            Request::Action(Action::Init(..)) => {
                let _ = stx.send(Event::Ready);
            }
            Request::Action(Action::Close) => {
                rx.close();
            }
            Request::Action(Action::Flush) => {
                let _ = stx.send(Event::Flushed);
            }
            Request::Action(Action::Sync) => {}

            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
pub struct AsyncProducer<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    tx: mpsc::UnboundedSender<Request<M>>,
    srx: parking_lot::Mutex<Option<mpsc::UnboundedReceiver<Event<R, E>>>>,
}

impl<T, M> ReceiverSubscriberBuilder<T, M, T::Response, T::Error>
    for AsyncProducer<M, T::Response, T::Error>
where
    T: AsyncProducerHandler<M> + 'static,
    T::Item: Message,
    T::Response: Message,
    T::Error: StdSyncSendError,
    M: Message,
{
    type Config = AsyncProducerConfig;

    fn build(_cfg: Self::Config) -> (Self, UntypedPollerCallback) {
        let (stx, srx) = mpsc::unbounded_channel();
        let (tx, rx) = mpsc::unbounded_channel();

        let poller = Box::new(move |ut| {
            Box::new(move |bus| {
                Box::pin(producer_poller::<T, M>(rx, bus, ut, stx))
                    as Pin<Box<dyn Future<Output = ()> + Send>>
            }) as Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
        });

        (
            AsyncProducer::<M, T::Response, T::Error> {
                tx,
                srx: parking_lot::Mutex::new(Some(srx)),
            },
            poller,
        )
    }
}

impl<M, R, E> SendUntypedReceiver for AsyncProducer<M, R, E>
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

impl<M, R, E> SendTypedReceiver<M> for AsyncProducer<M, R, E>
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

impl<M, R, E> ReciveTypedReceiver<R, E> for AsyncProducer<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    type Stream = Pin<Box<dyn Stream<Item = Event<R, E>> + Send>>;

    fn event_stream(&self, _: Bus) -> Self::Stream {
        let mut rx = self.srx.lock().take().expect("event_stream called twice");

        Box::pin(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
    }
}
