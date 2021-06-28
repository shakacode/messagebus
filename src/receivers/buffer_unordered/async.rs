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
    builder::ReceiverSubscriberBuilder,
    error::{Error, StdSyncSendError, SendError}, 
    receiver::{Action, Event, ReciveTypedReceiver, SendUntypedReceiver, SendTypedReceiver}, 
    receivers::{fix_type, Request},
    AsyncHandler, Bus, Message, Untyped,
};

use super::{BufferUnorderedConfig, BufferUnorderedStats};

use futures::{stream::FuturesUnordered, Future, StreamExt};
use parking_lot::Mutex;
use tokio::sync::mpsc;

buffer_unordered_poller_macro!(
    T,
    AsyncHandler,
    |mid, msg, bus, ut: Arc<T>, _stats: Arc<BufferUnorderedStats>| {
        async move { (mid, ut.handle(msg, &bus).await) }
    },
    |bus, ut: Arc<T>| { async move { ut.sync(&bus).await } }
);

pub struct BufferUnorderedAsync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    tx: mpsc::UnboundedSender<Request<M>>,
    stats: Arc<BufferUnorderedStats>,
    srx: Mutex<mpsc::UnboundedReceiver<Event<R, E>>>,
}

impl<T, M, R, E> ReceiverSubscriberBuilder<T, M, R, E> for BufferUnorderedAsync<M, R, E>
where
    T: AsyncHandler<M, Response = R, Error = E> + 'static,
    R: Message,
    M: Message,
    E: StdSyncSendError,
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
            BufferUnorderedAsync::<M, R, E> {
                tx,
                stats,
                srx: Mutex::new(srx),
            },
            poller,
        )
    }
}

impl<M, R, E> SendUntypedReceiver for BufferUnorderedAsync<M, R, E>
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

impl<M, R, E> SendTypedReceiver<M> for BufferUnorderedAsync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
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

impl<M, R, E> ReciveTypedReceiver<R, E> for BufferUnorderedAsync<M, R, E>
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
