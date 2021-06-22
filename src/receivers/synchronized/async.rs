use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    receiver::{Action, Event, ReciveTypedReceiver, SendUntypedReceiver},
    receivers::Request,
};
use anyhow::Result;
use futures::Future;

use super::SynchronizedConfig;
use crate::{
    builder::ReceiverSubscriberBuilder,
    receiver::{SendError, SendTypedReceiver},
    AsyncSynchronizedHandler, Bus, Message, Untyped,
};
use tokio::sync::{mpsc, Mutex};

fn synchronized_poller<T, M, R, E>(
    mut rx: mpsc::UnboundedReceiver<Request<M>>,
    bus: Bus,
    ut: Untyped,
    stx: mpsc::UnboundedSender<Event<R, E>>,
) -> impl Future<Output = ()>
where
    T: AsyncSynchronizedHandler<M, Response = R, Error = E> + 'static,
    M: Message,
    R: Message,
    E: crate::Error,
{
    let ut = ut.downcast::<Mutex<T>>().unwrap();
    let mut handle_future: Option<Pin<Box<dyn Future<Output = (u64, Result<R, E>)> + Send>>> = None;
    let mut sync_future: Option<Pin<Box<dyn Future<Output = Result<(), E>> + Send>>> = None;
    let mut need_sync = false;
    let mut rx_closed = false;

    futures::future::poll_fn(move |cx| loop {
        if let Some(mut fut) = handle_future.take() {
            match fut.as_mut().poll(cx) {
                Poll::Pending => {
                    handle_future = Some(fut);
                    return Poll::Pending;
                }

                Poll::Ready((mid, resp)) => {
                    stx.send(Event::Response(mid, resp)).ok();
                }
            }
        }

        if !rx_closed && !need_sync {
            match rx.poll_recv(cx) {
                Poll::Ready(Some(a)) => match a {
                    Request::Request(mid, msg) => {
                        let bus = bus.clone();
                        let ut = ut.clone();
                        handle_future.replace(Box::pin(async move {
                            (mid, ut.lock().await.handle(msg, &bus).await)
                        }));
                        continue;
                    }
                    Request::Action(Action::Flush) => {
                        stx.send(Event::Flushed).ok();
                    }
                    Request::Action(Action::Sync) => need_sync = true,
                    Request::Action(Action::Close) => {
                        rx.close();
                        continue;
                    }
                    _ => unimplemented!(),
                },
                Poll::Ready(None) => {
                    need_sync = true;
                    rx_closed = true;
                    
                }
                Poll::Pending => {},
            }
        }

        if need_sync {
            if let Some(mut fut) = sync_future.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        sync_future = Some(fut);
                        return Poll::Pending;
                    }
                    Poll::Ready(res) => {
                        need_sync = false;
                        stx.send(Event::Synchronized(res)).ok();
                    }
                }
            } else {
                let ut = ut.clone();
                let bus_clone = bus.clone();
                sync_future.replace(Box::pin(
                    async move { ut.lock().await.sync(&bus_clone).await },
                ));
            }
        }

        return if rx_closed {
            Poll::Ready(())
        } else {
            Poll::Pending
        };
    })
}

pub struct SynchronizedAsync<M, R = (), E = anyhow::Error>
where
    M: Message,
    R: Message,
    E: crate::Error,
{
    tx: mpsc::UnboundedSender<Request<M>>,
    srx: parking_lot::Mutex<mpsc::UnboundedReceiver<Event<R, E>>>,
}

impl<T, M, R, E> ReceiverSubscriberBuilder<T, M, R, E> for SynchronizedAsync<M, R, E>
where
    T: AsyncSynchronizedHandler<M, Response = R, Error = E> + 'static,
    R: Message,
    M: Message,
    E: crate::Error,
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
                Box::pin(synchronized_poller::<T, M, R, E>(rx, bus, ut, stx))
                    as Pin<Box<dyn Future<Output = ()> + Send>>
            }) as Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
        });

        (
            SynchronizedAsync::<M, R, E> {
                tx,
                srx: parking_lot::Mutex::new(srx),
            },
            poller,
        )
    }
}

impl<M, R, E> SendUntypedReceiver for SynchronizedAsync<M, R, E>
where
    M: Message,
    R: Message,
    E: crate::Error,
{
    fn send(&self, m: Action) -> Result<(), SendError<Action>> {
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
    E: crate::Error,
{
    fn send(&self, mid: u64, m: M) -> Result<(), SendError<M>> {
        match self.tx.send(Request::Request(mid, m)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Request(_, msg))) => Err(SendError::Closed(msg)),
            _ => unimplemented!(),
        }
    }
}

impl<M, R, E> ReciveTypedReceiver<R, E> for SynchronizedAsync<M, R, E>
where
    M: Message,
    R: Message,
    E: crate::Error,
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
