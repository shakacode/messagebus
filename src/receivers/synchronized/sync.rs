use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::synchronized_poller_macro;
use futures::{executor::block_on, Future};

use super::SynchronizedConfig;
use crate::{
    builder::ReceiverSubscriberBuilder,
    error::{Error, SendError, StdSyncSendError},
    receiver::{Action, Event, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver},
    receivers::{fix_type, Request},
    Bus, Message, SynchronizedHandler, Untyped,
};
use tokio::sync::{mpsc, Mutex};

synchronized_poller_macro! {
    T,
    SynchronizedHandler,
    |mid, msg, bus, ut: Arc<Mutex<T>>| async move {
        (mid, tokio::task::spawn_blocking(move || {
            block_on(ut.lock()).handle(msg, &bus)
        })
        .await
        .unwrap())
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
    srx: parking_lot::Mutex<mpsc::UnboundedReceiver<Event<R, E>>>,
}

impl<T, M, R, E> ReceiverSubscriberBuilder<T, M, R, E> for SynchronizedSync<M, R, E>
where
    T: SynchronizedHandler<M, Response = R, Error = E> + 'static,
    R: Message,
    M: Message,
    E: StdSyncSendError,
{
    type Config = SynchronizedConfig;

    fn build(
        _cfg: Self::Config,
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
                Box::pin(synchronized_poller::<T, M, R>(rx, bus, ut, stx))
                    as Pin<Box<dyn Future<Output = ()> + Send>>
            }) as Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
        });

        (
            SynchronizedSync::<M, R, E> {
                tx,
                srx: parking_lot::Mutex::new(srx),
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
    fn send(&self, msg: Action) -> Result<(), SendError<Action>> {
        match self.tx.send(Request::Action(msg)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Action(msg))) => Err(SendError::Closed(msg)),
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
    fn send(&self, mid: u64, m: M) -> Result<(), SendError<M>> {
        match self.tx.send(Request::Request(mid, m)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Request(_, msg))) => Err(SendError::Closed(msg)),
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
    fn poll_events(&self, ctx: &mut Context<'_>) -> Poll<Event<R, E>> {
        let poll = self.srx.lock().poll_recv(ctx);
        match poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(event)) => Poll::Ready(event),
            Poll::Ready(None) => Poll::Ready(Event::Exited),
        }
    }
}
