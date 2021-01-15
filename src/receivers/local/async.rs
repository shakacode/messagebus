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
use futures::{pin_mut, Future, StreamExt};

use super::{LocalConfig, LocalStats};
use crate::{
    builder::{ReceiverSubscriber, ReceiverSubscriberBuilder},
    receiver::{AnyReceiver, ReceiverTrait, SendError, TypedReceiver},
    Bus, LocalAsyncHandler, Message, Untyped,
};

pub struct LocalAsyncSubscriber<T, M>
where
    T: LocalAsyncHandler<M> + 'static,
    M: Message,
{
    cfg: LocalConfig,
    _m: PhantomData<(T, M)>,
}

impl<T, M> ReceiverSubscriber<T> for LocalAsyncSubscriber<T, M>
where
    T: LocalAsyncHandler<M> + 'static,
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
        let stats = Arc::new(LocalStats {
            buffer: AtomicU64::new(0),
            buffer_total: AtomicU64::new(cfg.buffer_size as _),
        });

        let arc = Arc::new(LocalAsync::<M> {
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
    stats: Arc<LocalStats>,
    _cfg: LocalConfig,
) where
    T: LocalAsyncHandler<M> + 'static,
    M: Message,
{
    let ut = ut.downcast_local::<T>().unwrap();
    let bus1 = bus.clone();

    let x = rx.then(|msg| {
        let bus1 = bus1.clone();
        ut.spawn_local(move |item| {
            Box::pin(async move {
                let _ = item.handle(msg, &bus1).await;
            })
        })
    });

    pin_mut!(x);

    while let Some(_) = x.next().await {
        stats.buffer.fetch_sub(1, Ordering::Relaxed);
    }

    let bus_clone = bus.clone();
    ut.spawn_local(move |item| {
        Box::pin(async move {
            let _ = item.sync(&bus_clone).await;
        })
    })
    .await;

    println!("[EXIT] LocalAsync<{}>", std::any::type_name::<M>());
}

pub struct LocalAsync<M: Message> {
    tx: mpsc::Sender<M>,
    stats: Arc<LocalStats>,
}

impl<T, M> ReceiverSubscriberBuilder<M, T> for LocalAsync<M>
where
    T: LocalAsyncHandler<M> + 'static,
    M: Message,
{
    type Entry = LocalAsyncSubscriber<T, M>;
    type Config = LocalConfig;

    fn build(cfg: Self::Config) -> Self::Entry {
        LocalAsyncSubscriber {
            cfg,
            _m: Default::default(),
        }
    }
}

impl<M: Message> TypedReceiver<M> for LocalAsync<M> {
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

impl<M: Message> ReceiverTrait for LocalAsync<M> {
    fn typed(&self) -> AnyReceiver<'_> {
        AnyReceiver::new(self)
    }

    fn type_id(&self) -> TypeId {
        TypeId::of::<LocalAsync<M>>()
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
