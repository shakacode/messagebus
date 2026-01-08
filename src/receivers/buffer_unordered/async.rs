use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::{
    buffer_unordered_poller_macro,
    builder::ReceiverSubscriberBuilder,
    error::{Error, StdSyncSendError},
    receiver::{
        Action, Event, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver,
        UntypedPollerCallback,
    },
    receivers::Request,
    AsyncHandler, Bus, Message, Untyped,
};

use super::{BufferUnorderedConfig, BufferUnorderedStats};

use futures::{Future, Stream};
use parking_lot::Mutex;
use tokio::sync::mpsc::{self, UnboundedSender};

buffer_unordered_poller_macro!(
    T,
    AsyncHandler,
    |mid, msg, bus, ut: Arc<T>, stx: UnboundedSender<_>, task_permit| {
        tokio::spawn(async move {
            let resp = ut.handle(msg, &bus).await;
            if let Err(err) = &resp {
                log::error!("AsyncHandler error: {err}");
            }
            drop(task_permit);

            if stx
                .send(Event::Response(mid, resp.map_err(Error::Other)))
                .is_err()
            {
                log::trace!("failed to send response - channel closed during shutdown");
            }
        })
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
    srx: Mutex<Option<mpsc::UnboundedReceiver<Event<R, E>>>>,
}

impl<T, M, R, E> ReceiverSubscriberBuilder<T, M, R, E> for BufferUnorderedAsync<M, R, E>
where
    T: AsyncHandler<M, Response = R, Error = E> + 'static,
    R: Message,
    M: Message,
    E: StdSyncSendError,
{
    type Config = BufferUnorderedConfig;

    fn build(cfg: Self::Config) -> (Self, UntypedPollerCallback) {
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
                srx: Mutex::new(Some(srx)),
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
    fn send(&self, m: Action, _bus: &Bus) -> Result<(), Error<Action>> {
        match self.tx.send(Request::Action(m)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Action(msg))) => Err(Error::send_closed(msg)),
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
    fn send(&self, mid: u64, m: M, req: bool, _bus: &Bus) -> Result<(), Error<M>> {
        match self.tx.send(Request::Request(mid, m, req)) {
            Ok(_) => {
                self.stats.buffer.fetch_add(1, Ordering::Relaxed);

                Ok(())
            }
            Err(mpsc::error::SendError(Request::Request(_, msg, _))) => {
                Err(Error::send_closed(msg))
            }
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
    type Stream = Pin<Box<dyn Stream<Item = Event<R, E>> + Send>>;

    fn event_stream(&self, _: Bus) -> Self::Stream {
        let mut rx = self.srx.lock().take().expect("event_stream called twice");

        Box::pin(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
    }
}
