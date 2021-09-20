use std::{pin::Pin, sync::Arc};

use crate::{
    batch_synchronized_poller_macro,
    builder::ReceiverSubscriberBuilder,
    error::{Error, SendError, StdSyncSendError},
    receiver::{Action, Event, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver},
    receivers::Request,
    BatchSynchronizedHandler, Bus, Message, Untyped,
};

use super::SynchronizedBatchedConfig;
use futures::{executor::block_on, Future, Stream};
use tokio::sync::{
    mpsc::{self, UnboundedSender},
    Mutex,
};

batch_synchronized_poller_macro! {
    T,
    BatchSynchronizedHandler,
    |mids: Vec<_>, msgs, bus, ut: Arc<Mutex<T>>, stx: UnboundedSender<_>| {
        tokio::task::spawn_blocking(move || {
            let resp = block_on(ut.lock()).handle(msgs, &bus);

            crate::process_batch_result!(resp, mids, stx);
        })
    },
    |bus, ut: Arc<Mutex<T>>| async move {
        tokio::task::spawn_blocking(move || block_on(ut.lock()).sync(&bus))
            .await
            .unwrap()
    }
}

pub struct SynchronizedBatchedSync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    tx: mpsc::UnboundedSender<Request<M>>,
    srx: parking_lot::Mutex<Option<mpsc::UnboundedReceiver<Event<R, E>>>>,
}

impl<T, M, R> ReceiverSubscriberBuilder<T, M, R, T::Error>
    for SynchronizedBatchedSync<M, R, T::Error>
where
    T: BatchSynchronizedHandler<M, Response = R> + 'static,
    T::Error: StdSyncSendError,
    R: Message,
    M: Message,
{
    type Config = SynchronizedBatchedConfig;

    fn build(
        cfg: Self::Config,
    ) -> (
        Self,
        Box<
            dyn FnOnce(Untyped) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>,
        >,
    ) {
        let (stx, srx) = mpsc::unbounded_channel();
        let (tx, rx) = mpsc::unbounded_channel();

        let poller = Box::new(move |ut| {
            Box::new(move |bus| {
                Box::pin(batch_synchronized_poller::<T, M, R>(rx, bus, ut, cfg, stx))
                    as Pin<Box<dyn Future<Output = ()> + Send>>
            }) as Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
        });

        (
            SynchronizedBatchedSync::<M, R, T::Error> {
                tx,
                srx: parking_lot::Mutex::new(Some(srx)),
            },
            poller,
        )
    }
}

impl<M, R, E> SendUntypedReceiver for SynchronizedBatchedSync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    fn send(&self, msg: Action, _bus: &Bus) -> Result<(), SendError<Action>> {
        match self.tx.send(Request::Action(msg)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Action(msg))) => Err(SendError::Closed(msg)),
            _ => unimplemented!(),
        }
    }
}

impl<M, R, E> SendTypedReceiver<M> for SynchronizedBatchedSync<M, R, E>
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

impl<M, R, E> ReciveTypedReceiver<R, E> for SynchronizedBatchedSync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    type Stream = Pin<Box<dyn Stream<Item = Event<R, E>> + Send>>;

    fn event_stream(&self) -> Self::Stream {
        let mut rx = self.srx.lock().take().unwrap();

        Box::pin(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
    }
}
