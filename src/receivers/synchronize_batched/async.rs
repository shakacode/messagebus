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

use crate::{receiver::ReceiverStats, receivers::mpsc};
use futures::{Future, StreamExt};
use tokio::sync::Mutex;

use super::{SynchronizeBatchedConfig, SynchronizeBatchedStats};
use crate::{
    builder::{ReceiverSubscriber, ReceiverSubscriberBuilder},
    msgs,
    receiver::{AnyReceiver, ReceiverTrait, SendError, TypedReceiver},
    AsyncBatchSynchronizedHandler, Bus, Message, Untyped,
};

pub struct SynchronizeBatchedAsyncSubscriber<T, M>
where
    T: AsyncBatchSynchronizedHandler<M> + 'static,
    M: Message,
{
    cfg: SynchronizeBatchedConfig,
    _m: PhantomData<(T, M)>,
}

impl<T, M> ReceiverSubscriber<T> for SynchronizeBatchedAsyncSubscriber<T, M>
where
    T: AsyncBatchSynchronizedHandler<M> + 'static,
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

        let arc = Arc::new(SynchronizeBatchedAsync::<M> {
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
    stats: Arc<SynchronizeBatchedStats>,
    cfg: SynchronizeBatchedConfig,
) where
    T: AsyncBatchSynchronizedHandler<M> + 'static,
    M: Message,
{
    let ut = ut.downcast::<Mutex<T>>().unwrap();

    let rx = rx
        .inspect(|_|{ 
            stats.buffer.fetch_sub(1, Ordering::Relaxed);
            stats.batch.fetch_add(1, Ordering::Relaxed); 
        });

    let mut rx = if cfg.when_ready {
        rx.ready_chunks(cfg.batch_size)
            .left_stream()
    } else {
        rx.chunks(cfg.batch_size)
            .right_stream()
    };

    while let Some(msgs) = rx.next().await {
        stats.batch.fetch_sub(msgs.len() as _, Ordering::Relaxed);

        let bus_clone = bus.clone();
        let ut = ut.clone();

        let res =
            tokio::task::spawn(async move { ut.lock().await.handle(msgs, &bus_clone).await })
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
    let res = tokio::task::spawn(async move { ut.lock().await.sync(&bus_clone).await }).await;

    match res {
        Ok(Err(err)) => {
            let _ = bus.send(msgs::Error(Arc::new(err))).await;
        }
        _ => (),
    }

    println!(
        "[EXIT] SynchronizeBatchedAsync<{}>",
        std::any::type_name::<M>()
    );
}

pub struct SynchronizeBatchedAsync<M: Message> {
    tx: mpsc::Sender<M>,
    stats: Arc<SynchronizeBatchedStats>,
}

impl<T, M> ReceiverSubscriberBuilder<M, T> for SynchronizeBatchedAsync<M>
where
    T: AsyncBatchSynchronizedHandler<M> + 'static,
    M: Message,
{
    type Entry = SynchronizeBatchedAsyncSubscriber<T, M>;
    type Config = SynchronizeBatchedConfig;

    fn build(cfg: Self::Config) -> Self::Entry {
        SynchronizeBatchedAsyncSubscriber {
            cfg,
            _m: Default::default(),
        }
    }
}

impl<M: Message> TypedReceiver<M> for SynchronizeBatchedAsync<M> {
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

impl<M: Message> ReceiverTrait for SynchronizeBatchedAsync<M> {
    fn typed(&self) -> AnyReceiver<'_> {
        AnyReceiver::new(self)
    }

    fn type_id(&self) -> TypeId {
        TypeId::of::<Self>()
    }

    fn close(&self) {
        self.tx.close();
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

    fn sync(&self) {
        self.tx.flush();
    }

    fn poll_synchronized(&self, _ctx: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}
