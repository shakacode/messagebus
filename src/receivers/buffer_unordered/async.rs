use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use crate::{receiver::{Action, Event, ReceiverStats, ReciveTypedReceiver, SendUntypedReceiver}, receivers::Request};
use anyhow::Result;
use futures::{Future, StreamExt, stream::FuturesUnordered};

use super::{BufferUnorderedConfig, BufferUnorderedStats};
use crate::{
    builder::ReceiverSubscriberBuilder,
    receiver::{SendError, SendTypedReceiver},
    AsyncHandler, Bus, Message, Untyped,
};
use parking_lot::Mutex;
use tokio::sync::mpsc;


fn buffer_unordered_poller<T, M, R, E>(
    mut rx: mpsc::UnboundedReceiver<Request<M>>,
    bus: Bus,
    ut: Untyped,
    stats: Arc<BufferUnorderedStats>,
    cfg: BufferUnorderedConfig,
    stx: mpsc::UnboundedSender<Event<R, E>>,
) -> impl Future<Output = ()>
where
    T: AsyncHandler<M, Response = R, Error = E> + 'static,
    M: Message,
    R: Message,
    E: crate::Error
{
    let ut = ut.downcast::<T>().unwrap();
    let mut queue = FuturesUnordered::new();
    let mut sync_future: Option<Pin<Box<dyn Future<Output = Result<(), E>> + Send>>> = None;
    let mut need_sync = false;
    let mut rx_closed = false;
    let mut need_flush = false;

    futures::future::poll_fn(move |cx| loop {
        if !rx_closed && !need_flush && !need_sync {
            while queue.len() < cfg.max_parallel {
                match rx.poll_recv(cx) {
                    Poll::Ready(Some(a)) => {
                        match a {
                            Request::Request(mid, msg) => {
                                stats.buffer.fetch_sub(1, Ordering::Relaxed);
                                stats.parallel.fetch_add(1, Ordering::Relaxed);

                                let bus = bus.clone();
                                let ut = ut.clone();
                                queue.push(tokio::task::spawn(async move { (mid, ut.handle(msg, &bus).await) }));
                            },
                            Request::Action(Action::Flush) => need_flush = true,
                            Request::Action(Action::Sync) => need_sync = true,
                            Request::Action(Action::Close) => rx.close(),
                            _ => unimplemented!()
                        }
                    },
                    Poll::Ready(None) => {
                        need_sync = true;
                        rx_closed = true;
                    },
                    Poll::Pending => break,
                }
            }
        }

        let queue_len = queue.len();
        
        loop {
            if queue_len != 0 {
                loop {
                    match queue.poll_next_unpin(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Some(Ok((mid, res)))) => {
                            stx.send(Event::Response(mid, res)).ok();
                        },
                        Poll::Ready(None) => break,
                        _ => {}
                    }
                }
            }

            if need_flush {
                need_flush = false;
                stx.send(Event::Flushed).ok();
            }

            if need_sync {
                if let Some(mut fut) = sync_future.take() {
                    match fut.as_mut().poll(cx) {
                        Poll::Pending => {
                            sync_future = Some(fut);
                            return Poll::Pending;
                        },
                        Poll::Ready(res) => {
                            need_sync = false;
                            stx.send(Event::Synchronized(res)).ok();
                        }
                    }
                } else {
                    let ut = ut.clone();
                    let bus_clone = bus.clone();
                    sync_future.replace(Box::pin(async move { 
                        ut.sync(&bus_clone).await 
                    }));
                }
            } else {
                break;
            }
        }

        if queue_len == queue.len() {
            return if rx_closed { Poll::Ready(()) } else { Poll::Pending };
        }
    })
}

pub struct BufferUnorderedAsync<M, R = (), E = anyhow::Error> 
    where 
        M: Message,
        R: Message,
        E: crate::Error
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
        E: crate::Error
{
    type Config = BufferUnorderedConfig;

    fn build(cfg: Self::Config) -> (Self, Box<dyn FnOnce(Untyped) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>>)
    {
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
                Box::pin(buffer_unordered_poller::<T, M, R, E>(rx, bus, ut, stats_clone, cfg, stx))
                    as Pin<Box<dyn Future<Output = ()> + Send>>
            }) as Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
        });

        (BufferUnorderedAsync::<M, R, E> {
            tx,
            stats,
            srx: Mutex::new(srx),
        }, poller)
    }
}

impl<M, R, E> SendUntypedReceiver for BufferUnorderedAsync<M, R, E> 
    where
        M: Message,
        R: Message,
        E: crate::Error
{
    fn send(&self, m: Action) -> Result<(), SendError<Action>> {
        match self.tx.send(Request::Action(m)) {
            Ok(_) => Ok(()),
            Err(mpsc::error::SendError(Request::Action(msg))) => Err(SendError::Closed(msg)),
            _ => unimplemented!()
        }
    }
}

impl<M, R, E> SendTypedReceiver<M> for BufferUnorderedAsync<M, R, E> 
    where
        M: Message,
        R: Message,
        E: crate::Error
{
    fn send(&self, mid: u64, m: M) -> Result<(), SendError<M>> {
        match self.tx.send(Request::Request(mid, m)) {
            Ok(_) => {
                self.stats.buffer.fetch_add(1, Ordering::Relaxed);

                Ok(())
            }
            Err(mpsc::error::SendError(Request::Request(_, msg))) => Err(SendError::Closed(msg)),
            _ => unimplemented!()
        }
    }
}

impl<M, R, E> ReciveTypedReceiver<R, E> for BufferUnorderedAsync<M, R, E> 
    where
        M: Message,
        R: Message,
        E: crate::Error
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