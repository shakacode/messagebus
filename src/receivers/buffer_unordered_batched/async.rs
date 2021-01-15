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

use super::{BufferUnorderedBatchedConfig, BufferUnorderedBatchedStats};
use crate::{
    builder::{ReceiverSubscriber, ReceiverSubscriberBuilder},
    msgs,
    receiver::{AnyReceiver, ReceiverTrait, SendError, TypedReceiver},
    AsyncBatchHandler, Bus, Message, Untyped,
};

pub struct BufferUnorderedBatchedAsyncSubscriber<T, M>
where
    T: AsyncBatchHandler<M> + 'static,
    M: Message,
{
    cfg: BufferUnorderedBatchedConfig,
    _m: PhantomData<(T, M)>,
}

impl<T, M> ReceiverSubscriber<T> for BufferUnorderedBatchedAsyncSubscriber<T, M>
where
    T: AsyncBatchHandler<M> + 'static,
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
        let stats = Arc::new(BufferUnorderedBatchedStats {
            buffer: AtomicU64::new(0),
            buffer_total: AtomicU64::new(cfg.buffer_size as _),
            parallel: AtomicU64::new(0),
            parallel_total: AtomicU64::new(cfg.max_parallel as _),
            batch: AtomicU64::new(0),
            batch_size: AtomicU64::new(cfg.batch_size as _),
        });

        let (tx, rx) = mpsc::channel(cfg.buffer_size);
        let arc = Arc::new(BufferUnorderedBatchedAsync::<M> {
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
    stats: Arc<BufferUnorderedBatchedStats>,
    cfg: BufferUnorderedBatchedConfig,
) where
    T: AsyncBatchHandler<M> + 'static,
    M: Message,
{
    let ut = ut.downcast_sync::<T>().unwrap();
    let rx = rx.inspect(|_| {
        stats.buffer.fetch_sub(1, Ordering::Relaxed);
        stats.batch.fetch_add(1, Ordering::Relaxed);
    });

    let rx = if cfg.when_ready {
        rx.ready_chunks(cfg.batch_size).left_stream()
    } else {
        rx.chunks(cfg.batch_size).right_stream()
    };

    let mut rx = rx
        .map(|msgs| {
            stats.batch.fetch_sub(msgs.len() as _, Ordering::Relaxed);
            stats.parallel.fetch_add(1, Ordering::Relaxed);

            let bus_clone = bus.clone();
            let ut = ut.clone();

            tokio::task::spawn(async move {
                ut.lock_read()
                    .await
                    .get_ref()
                    .handle(msgs, &bus_clone)
                    .await
            })
        })
        .buffer_unordered(cfg.max_parallel);

    while let Some(err) = rx.next().await {
        stats.parallel.fetch_sub(1, Ordering::Relaxed);

        match err {
            Ok(Err(err)) => {
                let _ = bus.send(msgs::Error(Arc::new(err))).await;
            }
            _ => (),
        }
    }

    let ut = ut.clone();
    let bus_clone = bus.clone();
    let res =
        tokio::task::spawn(async move { ut.lock_read().await.get_ref().sync(&bus_clone).await })
            .await;

    match res {
        Ok(Err(err)) => {
            let _ = bus.send(msgs::Error(Arc::new(err))).await;
        }
        _ => (),
    }

    println!(
        "[EXIT] BufferUnorderedBatchedAsync<{}>",
        std::any::type_name::<M>()
    );
}

pub struct BufferUnorderedBatchedAsync<M: Message> {
    tx: mpsc::Sender<M>,
    stats: Arc<BufferUnorderedBatchedStats>,
}

impl<T, M> ReceiverSubscriberBuilder<M, T> for BufferUnorderedBatchedAsync<M>
where
    T: AsyncBatchHandler<M> + 'static,
    M: Message,
{
    type Entry = BufferUnorderedBatchedAsyncSubscriber<T, M>;
    type Config = BufferUnorderedBatchedConfig;

    fn build(cfg: Self::Config) -> Self::Entry {
        BufferUnorderedBatchedAsyncSubscriber {
            cfg,
            _m: Default::default(),
        }
    }
}

impl<M: Message> TypedReceiver<M> for BufferUnorderedBatchedAsync<M> {
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

impl<M: Message> ReceiverTrait for BufferUnorderedBatchedAsync<M> {
    fn typed(&self) -> AnyReceiver<'_> {
        AnyReceiver::new(self)
    }

    fn type_id(&self) -> TypeId {
        TypeId::of::<BufferUnorderedBatchedAsync<M>>()
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
                (
                    "parallel".into(),
                    self.stats.parallel.load(Ordering::SeqCst),
                ),
                (
                    "parallel_total".into(),
                    self.stats.parallel_total.load(Ordering::SeqCst),
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
