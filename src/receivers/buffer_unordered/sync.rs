use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use crate::{
    buffer_unordered_poller_macro,
    receiver::{Action, Event, ReciveTypedReceiver, SendUntypedReceiver},
    receivers::{fix_type, Request},
};
use anyhow::Result;
use futures::{stream::FuturesUnordered, Future, StreamExt};

use super::{BufferUnorderedConfig, BufferUnorderedStats};
use crate::{
    builder::ReceiverSubscriberBuilder,
    receiver::{SendError, SendTypedReceiver},
    Bus, Handler, Message, Untyped,
};
use parking_lot::Mutex;
use tokio::sync::mpsc;

buffer_unordered_poller_macro!(
    T,
    Handler,
    |mid, msg, bus, ut: Arc<T>, _stats: Arc<BufferUnorderedStats>| async move {
        tokio::task::spawn_blocking(move || (mid, ut.handle(msg, &bus)))
            .await
            .unwrap()
    },
    |bus, ut: Arc<T>| async move {
        tokio::task::spawn_blocking(move || ut.sync(&bus))
            .await
            .unwrap()
    }
);

pub struct BufferUnorderedSync<M, R = (), E = crate::error::Error>
where
    M: Message,
    R: Message,
    E: crate::Error,
{
    tx: mpsc::UnboundedSender<Request<M>>,
    stats: Arc<BufferUnorderedStats>,
    srx: Mutex<mpsc::UnboundedReceiver<Event<R, E>>>,
}

impl<T, M, R, E> ReceiverSubscriberBuilder<T, M, R, E> for BufferUnorderedSync<M, R, E>
where
    T: Handler<M, Response = R, Error = E> + 'static,
    R: Message,
    M: Message,
    E: crate::Error,
{
    type Config = BufferUnorderedConfig;

    fn build(
        cfg: Self::Config,
    ) -> (
        Self,
        Box<
            dyn FnOnce(Untyped) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>,
        >,
    ) {
        let stats = Arc::new(BufferUnorderedStats {
            buffer: AtomicU64::new(0),
            buffer_total: AtomicU64::new(cfg.buffer_size as _),
            parallel: AtomicU64::new(0),
            parallel_total: AtomicU64::new(cfg.max_parallel as _),
        });

        let (stx, srx) = mpsc::unbounded_channel();
        let (tx, rx) = mpsc::unbounded_channel();
        let stats_clone = stats.clone();

        let poller = Box::new(move |ut| {
            Box::new(move |bus| {
                Box::pin(buffer_unordered_poller::<T, M, R, E>(
                    rx,
                    bus,
                    ut,
                    stats_clone,
                    cfg,
                    stx,
                )) as Pin<Box<dyn Future<Output = ()> + Send>>
            }) as Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
        });

        (
            BufferUnorderedSync::<M, R, E> {
                tx,
                stats,
                srx: Mutex::new(srx),
            },
            poller,
        )
    }
}

impl<M, R, E> SendUntypedReceiver for BufferUnorderedSync<M, R, E>
where
    M: Message,
    R: Message,
    E: crate::Error,
{
    fn send(&self, msg: Action) -> Result<(), SendError<Action>> {
        match self.tx.send(Request::Action(msg)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Action(msg))) => Err(SendError::Closed(msg)),
            _ => unimplemented!(),
        }
    }
}

impl<M, R, E> SendTypedReceiver<M> for BufferUnorderedSync<M, R, E>
where
    M: Message,
    R: Message,
    E: crate::Error,
{
    fn send(&self, mid: u64, m: M) -> Result<(), SendError<M>> {
        match self.tx.send(Request::Request(mid, m)) {
            Ok(_) => {
                self.stats.buffer.fetch_add(1, Ordering::Relaxed);

                Ok(())
            }
            Err(mpsc::error::SendError(Request::Request(_, msg))) => Err(SendError::Closed(msg)),
            _ => unimplemented!(),
        }
    }
}

impl<M, R, E> ReciveTypedReceiver<R, E> for BufferUnorderedSync<M, R, E>
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
