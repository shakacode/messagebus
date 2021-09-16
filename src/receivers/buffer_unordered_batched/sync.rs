use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use super::{BufferUnorderedBatchedConfig, BufferUnorderedBatchedStats};
use crate::{
    buffer_unordered_batch_poller_macro,
    builder::ReceiverSubscriberBuilder,
    error::{Error, SendError, StdSyncSendError},
    receiver::{Action, Event, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver},
    receivers::Request,
    BatchHandler, Bus, Message, Untyped,
};

use futures::Future;
use parking_lot::Mutex;
use tokio::sync::mpsc::{self, UnboundedSender};

buffer_unordered_batch_poller_macro!(
    T,
    BatchHandler,
    |mids: Vec<_>, msgs, bus, ut: Arc<T>, flush_permit, task_permit, stx: UnboundedSender<_>| {
        tokio::task::spawn_blocking(move || {
            let resp = ut.handle(msgs, &bus);
            
            drop(task_permit);
            drop(flush_permit);

            crate::process_batch_result!(resp, mids, stx);
        })
    },
    |bus, ut: Arc<T>| {
        async move {
            tokio::task::spawn_blocking(move || ut.sync(&bus))
                .await
                .unwrap()
        }
    }
);

pub struct BufferUnorderedBatchedSync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    tx: mpsc::UnboundedSender<Request<M>>,
    stats: Arc<BufferUnorderedBatchedStats>,
    srx: Mutex<mpsc::UnboundedReceiver<Event<R, E>>>,
}

impl<T, M, R> ReceiverSubscriberBuilder<T, M, R, T::Error>
    for BufferUnorderedBatchedSync<M, R, T::Error>
where
    T: BatchHandler<M, Response = R> + 'static,
    T::Error: StdSyncSendError,
    R: Message,
    M: Message,
{
    type Config = BufferUnorderedBatchedConfig;

    fn build(
        cfg: Self::Config,
    ) -> (
        Self,
        Box<
            dyn FnOnce(Untyped) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>,
        >,
    ) {
        let stats = Arc::new(BufferUnorderedBatchedStats {
            buffer: AtomicU64::new(0),
            buffer_total: AtomicU64::new(cfg.buffer_size as _),
            parallel: AtomicU64::new(0),
            parallel_total: AtomicU64::new(cfg.max_parallel as _),
            batch: AtomicU64::new(0),
            batch_size: AtomicU64::new(cfg.batch_size as _),
        });

        let (stx, srx) = mpsc::unbounded_channel();
        let (tx, rx) = mpsc::unbounded_channel();
        let stats_clone = stats.clone();

        let poller = Box::new(move |ut| {
            Box::new(move |bus| {
                Box::pin(buffer_unordered_batch_poller::<T, M, R>(
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
            BufferUnorderedBatchedSync::<M, R, T::Error> {
                tx,
                stats,
                srx: Mutex::new(srx),
            },
            poller,
        )
    }
}

impl<M, R, E> SendUntypedReceiver for BufferUnorderedBatchedSync<M, R, E>
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

impl<M, R, E> SendTypedReceiver<M> for BufferUnorderedBatchedSync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    fn send(&self, mid: u64, m: M, req: bool, _bus: &Bus) -> Result<(), SendError<M>> {
        match self.tx.send(Request::Request(mid, m, req)) {
            Ok(_) => {
                self.stats.buffer.fetch_add(1, Ordering::Relaxed);

                Ok(())
            }
            Err(mpsc::error::SendError(Request::Request(_, msg, _))) => Err(SendError::Closed(msg)),
            _ => unimplemented!(),
        }
    }
}

impl<M, R, E> ReciveTypedReceiver<R, E> for BufferUnorderedBatchedSync<M, R, E>
where
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    fn poll_events(&self, ctx: &mut Context<'_>, _bus: &Bus) -> Poll<Event<R, E>> {
        let poll = self.srx.lock().poll_recv(ctx);
        match poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(event)) => Poll::Ready(event),
            Poll::Ready(None) => Poll::Ready(Event::Exited),
        }
    }
}
