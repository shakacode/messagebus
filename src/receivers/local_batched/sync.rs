use super::{LocalBatchedConfig, LocalBatchedStats};
use crate::{
    builder::{ReceiverSubscriber, ReceiverSubscriberBuilder},
    receiver::{AnyReceiver, ReceiverTrait, SendError, TypedReceiver},
    Bus, LocalBatchHandler, Message, Untyped,
};
use crate::{receiver::ReceiverStats, receivers::mpsc};
use futures::{Future, StreamExt};
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

pub struct LocalBatchedSyncSubscriber<T, M>
where
    T: LocalBatchHandler<M> + 'static,
    M: Message,
{
    cfg: LocalBatchedConfig,
    _m: PhantomData<(M, T)>,
}

impl<T, M> ReceiverSubscriber<T> for LocalBatchedSyncSubscriber<T, M>
where
    T: LocalBatchHandler<M> + 'static,
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
        let stats = Arc::new(LocalBatchedStats {
            buffer: AtomicU64::new(0),
            buffer_total: AtomicU64::new(cfg.buffer_size as _),
            batch: AtomicU64::new(0),
            batch_size: AtomicU64::new(cfg.batch_size as _),
        });

        let arc = Arc::new(LocalBatchedSync::<M> {
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
    stats: Arc<LocalBatchedStats>,
    cfg: LocalBatchedConfig,
) where
    T: LocalBatchHandler<M> + 'static,
    M: Message,
{
    let ut = ut.downcast_local::<T>().unwrap();

    let rx = rx.inspect(|_| {
        stats.buffer.fetch_sub(1, Ordering::Relaxed);
        stats.batch.fetch_add(1, Ordering::Relaxed);
    });

    let mut rx = if cfg.when_ready {
        rx.ready_chunks(cfg.batch_size).left_stream()
    } else {
        rx.chunks(cfg.batch_size).right_stream()
    };

    while let Some(msgs) = rx.next().await {
        stats.batch.fetch_sub(msgs.len() as _, Ordering::Relaxed);

        let bus_clone = bus.clone();
        let ut = ut.clone();

        ut.spawn_local(move |item| {
            let _ = item.handle(msgs, &bus_clone);

            Box::pin(async move {})
        })
        .await;
    }

    let ut = ut.clone();
    let bus_clone = bus.clone();
    ut.spawn_local(move |item| {
        let _ = item.sync(&bus_clone);
        Box::pin(async move {})
    })
    .await;

    println!("[EXIT] LocalBatchedSync<{}>", std::any::type_name::<M>());
}

pub struct LocalBatchedSync<M: Message> {
    tx: mpsc::Sender<M>,
    stats: Arc<LocalBatchedStats>,
}

impl<T, M> ReceiverSubscriberBuilder<M, T> for LocalBatchedSync<M>
where
    T: LocalBatchHandler<M> + 'static,
    M: Message,
{
    type Entry = LocalBatchedSyncSubscriber<T, M>;
    type Config = LocalBatchedConfig;

    fn build(cfg: Self::Config) -> Self::Entry {
        LocalBatchedSyncSubscriber {
            cfg,
            _m: Default::default(),
        }
    }
}

impl<M: Message> TypedReceiver<M> for LocalBatchedSync<M> {
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

impl<M: Message> ReceiverTrait for LocalBatchedSync<M> {
    fn typed(&self) -> AnyReceiver<'_> {
        AnyReceiver::new(self)
    }

    fn type_id(&self) -> TypeId {
        TypeId::of::<Self>()
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
                ("batch".into(), self.stats.batch.load(Ordering::SeqCst)),
                (
                    "batch_size".into(),
                    self.stats.batch_size.load(Ordering::SeqCst),
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
