use super::{SynchronizedConfig, SynchronizedStats};
use crate::{receiver::ReceiverStats, receivers::mpsc};
use futures::{executor::block_on, Future, StreamExt};
use std::{
    any::TypeId,
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use crate::{
    builder::{ReceiverSubscriber, ReceiverSubscriberBuilder},
    msgs,
    receiver::{AnyReceiver, ReceiverTrait, SendError, TypedReceiver},
    Bus, Message, SynchronizedHandler, Untyped,
};

pub struct SynchronizedSyncSubscriber<T, M>
where
    T: SynchronizedHandler<M> + 'static,
    M: Message,
{
    cfg: SynchronizedConfig,
    _m: PhantomData<(M, T)>,
}

impl<T, M> ReceiverSubscriber<T> for SynchronizedSyncSubscriber<T, M>
where
    T: SynchronizedHandler<M> + 'static,
    M: Message,
{
    fn subscribe(
        self,
    ) -> (
        Arc<dyn ReceiverTrait>,
        Box<
            dyn FnOnce(Untyped) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>,
        >,
    ) {
        let cfg = self.cfg;
        let (tx, rx) = mpsc::channel(cfg.buffer_size);
        let stats = Arc::new(SynchronizedStats {
            buffer: AtomicU64::new(0),
            buffer_total: AtomicU64::new(cfg.buffer_size as _),
        });

        let arc = Arc::new(SynchronizedSync::<M> {
            tx,
            stats: stats.clone(),
        });
        let poller = Box::new(move |ut| {
            Box::new(move |bus| {
                Box::pin(buffer_unordered_poller::<T, M>(rx, bus, ut, stats, cfg))
                    as Pin<Box<dyn Future<Output = ()> + Send>>
            }) as Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>
        });

        (arc, poller)
    }
}

async fn buffer_unordered_poller<T, M>(
    rx: mpsc::Receiver<M>,
    bus: Bus,
    ut: Untyped,
    stats: Arc<SynchronizedStats>,
    _cfg: SynchronizedConfig,
) where
    T: SynchronizedHandler<M> + 'static,
    M: Message,
{
    let ut = ut.downcast_send::<T>().unwrap();
    let mut x = rx.then(|msg| {
        let ut = ut.clone();
        let bus = bus.clone();

        tokio::task::spawn_blocking(move || {
            let mut uut = block_on(ut.lock());
            uut.handle(msg, &bus)
        })
    });

    while let Some(err) = x.next().await {
        stats.buffer.fetch_sub(1, Ordering::Relaxed);

        match err {
            Ok(Err(err)) => {
                let _ = bus.send(msgs::Error(Arc::new(err))).await;
            }
            _ => (),
        }
    }

    let ut = ut.clone();
    let bus_clone = bus.clone();
    let res = tokio::task::spawn_blocking(move || block_on(ut.lock()).sync(&bus_clone)).await;

    match res {
        Ok(Err(err)) => {
            let _ = bus.send(msgs::Error(Arc::new(err))).await;
        }
        _ => (),
    }

    println!("[EXIT] BufferUnorderedSync<{}>", std::any::type_name::<M>());
}

pub struct SynchronizedSync<M: Message> {
    tx: mpsc::Sender<M>,
    stats: Arc<SynchronizedStats>,
}

impl<T, M> ReceiverSubscriberBuilder<M, T> for SynchronizedSync<M>
where
    T: SynchronizedHandler<M> + 'static,
    M: Message,
{
    type Entry = SynchronizedSyncSubscriber<T, M>;
    type Config = SynchronizedConfig;

    fn build(cfg: Self::Config) -> Self::Entry {
        SynchronizedSyncSubscriber {
            cfg,
            _m: Default::default(),
        }
    }
}

impl<M: Message> TypedReceiver<M> for SynchronizedSync<M> {
    fn poll_ready(&self, ctx: &mut Context<'_>) -> Poll<()> {
        match self.tx.poll_ready(ctx) {
            Poll::Ready(_) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }

    fn try_send(&self, m: M) -> Result<(), SendError<M>> {
        match self.tx.try_send(m) {
            Ok(_) => {
                self.stats.buffer.fetch_add(1, Ordering::Relaxed);

                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

impl<M: Message> ReceiverTrait for SynchronizedSync<M> {
    fn typed(&self) -> AnyReceiver<'_> {
        AnyReceiver::new(self)
    }

    fn type_id(&self) -> TypeId {
        TypeId::of::<SynchronizedSync<M>>()
    }

    fn stats(&self) -> ReceiverStats {
        ReceiverStats {
            name: std::any::type_name::<M>().into(),
            fields: vec![
                ("buffer".into(), self.stats.buffer.load(Ordering::SeqCst)),
                (
                    "buffer_total".into(),
                    self.stats.buffer_total.load(Ordering::SeqCst),
                ),
            ],
        }
    }

    fn close(&self) {
        self.tx.close();
    }

    fn sync(&self) {
        self.tx.flush();
    }

    fn poll_synchronized(&self, _ctx: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}
