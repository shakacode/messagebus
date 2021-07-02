use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    batch_synchronized_poller_macro,
    builder::ReceiverSubscriberBuilder,
    error::{Error, SendError, StdSyncSendError},
    receiver::{Action, Event, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver},
    receivers::{fix_type, fix_into_iter, Request},
    AsyncBatchSynchronizedHandler, Bus, Message, Untyped,
};

use futures::Future;

use super::SynchronizedBatchedConfig;

use tokio::sync::{mpsc, Mutex};

batch_synchronized_poller_macro! {
    T,
    AsyncBatchSynchronizedHandler,
    |mids, buffer, bus, ut: Arc<Mutex<T>>| {
        async move {
            (mids, ut.lock().await.handle(buffer, &bus).await)
        }
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
    srx: parking_lot::Mutex<mpsc::UnboundedReceiver<Event<R, E>>>,
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
            SynchronizedBatchedAsync::<M, R, T::Error> {
                tx,
                srx: parking_lot::Mutex::new(srx),
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
    fn send(&self, m: Action) -> Result<(), SendError<Action>> {
        match self.tx.send(Request::Action(m)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Action(msg))) => Err(SendError::Closed(msg)),
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
    fn send(&self, mid: u64, m: M) -> Result<(), SendError<M>> {
        match self.tx.send(Request::Request(mid, m)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Request(_, msg))) => Err(SendError::Closed(msg)),
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
    fn poll_events(&self, ctx: &mut Context<'_>) -> Poll<Event<R, E>> {
        let poll = self.srx.lock().poll_recv(ctx);
        match poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(event)) => Poll::Ready(event),
            Poll::Ready(None) => Poll::Ready(Event::Exited),
        }
    }
}
