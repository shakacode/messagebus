use super::{SynchronizeBatchedConfig, SynchronizeBatchedStats};
use crate::{
    builder::{ReceiverSubscriber, ReceiverSubscriberBuilder},
    msgs,
    receiver::{AnyReceiver, ReceiverTrait, SendError, TypedReceiver},
    BatchSynchronizedHandler, Bus, Message, Untyped,
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
use tokio::sync::Mutex;

pub struct SynchronizeBatchedSyncSubscriber<T, M>
where
    T: BatchSynchronizedHandler<M> + 'static,
    M: Message,
{
    cfg: SynchronizeBatchedConfig,
    _m: PhantomData<(M, T)>,
}

impl<T, M> ReceiverSubscriber<T> for SynchronizeBatchedSyncSubscriber<T, M>
where
    T: BatchSynchronizedHandler<M> + 'static,
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
        let stats = Arc::new(SynchronizeBatchedStats {
            buffer: AtomicU64::new(0),
            buffer_total: AtomicU64::new(cfg.buffer_size as _),
            batch: AtomicU64::new(0),
            batch_size: AtomicU64::new(cfg.batch_size as _),
        });

        let arc = Arc::new(SynchronizeBatchedSync::<M> {
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
    mut rx: mpsc::Receiver<M>,
    bus: Bus,
    ut: Untyped,
    stats: Arc<SynchronizeBatchedStats>,
    cfg: SynchronizeBatchedConfig,
) where
    T: BatchSynchronizedHandler<M> + 'static,
    M: Message,
{
    let mut batch = Vec::with_capacity(cfg.batch_size);
    let ut = ut.downcast::<Mutex<T>>().unwrap();

    while let Some(msg) = rx.next().await {
        stats.buffer.fetch_sub(1, Ordering::Relaxed);
        batch.push(msg);
        stats.batch.fetch_add(1, Ordering::Relaxed);

        if batch.len() >= cfg.batch_size {
            let ut = ut.clone();
            let bus_clone = bus.clone();
            let msgs = batch.drain(..).collect::<Vec<_>>();
            stats.batch.store(0, Ordering::Relaxed);

            let res = tokio::task::spawn_blocking(move || {
                let mut uut = futures::executor::block_on(ut.lock());
                uut.handle(msgs, &bus_clone)
            })
            .await;

            match res {
                Ok(Err(err)) => {
                    let _ = bus.send(msgs::Error(Arc::new(err))).await;
                }
                _ => (),
            }
        }
    }

    if !batch.is_empty() {
        let ut = ut.clone();
        let bus_clone = bus.clone();
        stats.batch.store(0, Ordering::Relaxed);
        let res = tokio::task::spawn_blocking(move || {
            futures::executor::block_on(ut.lock()).handle(batch, &bus_clone)
        })
        .await;

        match res {
            Ok(Err(err)) => {
                let _ = bus.send(msgs::Error(Arc::new(err))).await;
            }
            _ => (),
        }
    }

    let ut = ut.clone();
    let bus_clone = bus.clone();
    let res = tokio::task::spawn_blocking(move || {
        futures::executor::block_on(ut.lock()).sync(&bus_clone)
    })
    .await;

    match res {
        Ok(Err(err)) => {
            let _ = bus.send(msgs::Error(Arc::new(err))).await;
        }
        _ => (),
    }

    println!(
        "[EXIT] SynchronizeBatchedSync<{}>",
        std::any::type_name::<M>()
    );
}

pub struct SynchronizeBatchedSync<M: Message> {
    tx: mpsc::Sender<M>,
    stats: Arc<SynchronizeBatchedStats>,
}

impl<T, M> ReceiverSubscriberBuilder<M, T> for SynchronizeBatchedSync<M>
where
    T: BatchSynchronizedHandler<M> + 'static,
    M: Message,
{
    type Entry = SynchronizeBatchedSyncSubscriber<T, M>;
    type Config = SynchronizeBatchedConfig;

    fn build(cfg: Self::Config) -> Self::Entry {
        SynchronizeBatchedSyncSubscriber {
            cfg,
            _m: Default::default(),
        }
    }
}

impl<M: Message> TypedReceiver<M> for SynchronizeBatchedSync<M> {
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

impl<M: Message> ReceiverTrait for SynchronizeBatchedSync<M> {
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
