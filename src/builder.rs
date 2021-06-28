use std::{any::TypeId, collections::HashMap, marker::PhantomData, pin::Pin, sync::Arc};

use futures::{Future, FutureExt};
use tokio::sync::Mutex;

use crate::{Bus, BusInner, Message, Untyped, error::StdSyncSendError, receiver::{Receiver, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver}};

pub trait ReceiverSubscriberBuilder<T, M, R, E>:
    SendUntypedReceiver + SendTypedReceiver<M> + ReciveTypedReceiver<R, E>
where
    T: 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    type Config: Default;

    fn build(
        cfg: Self::Config,
    ) -> (
        Self,
        Box<
            dyn FnOnce(Untyped) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>,
        >,
    )
    where
        Self: Sized;
}

pub struct SyncEntry;
pub struct UnsyncEntry;

#[must_use]
pub struct RegisterEntry<K, T> {
    item: Untyped,
    builder: BusBuilder,
    receivers: HashMap<
        TypeId,
        Vec<(
            Receiver,
            Box<
                dyn FnOnce(
                    Untyped,
                )
                    -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>,
            >,
            Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>,
        )>,
    >,
    _m: PhantomData<(K, T)>,
}

impl<K, T: 'static> RegisterEntry<K, T> {
    pub fn done(self) -> BusBuilder {
        let mut builder = self.builder;

        for (tid, v) in self.receivers {
            for (r, poller, poller2) in v {
                let poller = poller(self.item.clone());

                builder.add_recevier((tid, r), poller, poller2);
            }
        }

        builder
    }
}

impl<T> RegisterEntry<UnsyncEntry, T> {
    pub fn subscribe<M, S, R, E>(mut self, queue: u64, cfg: S::Config) -> Self
    where
        T: Send + 'static,
        M: Message,
        R: Message,
        E: StdSyncSendError,
        S: ReceiverSubscriberBuilder<T, M, R, E> + 'static,
    {
        let (inner, poller) = S::build(cfg);

        let receiver = Receiver::new::<M, R, E, S>(queue, inner);
        let poller2 = receiver.start_polling_events::<R, E>();
        self.receivers
            .entry(TypeId::of::<M>())
            .or_insert_with(Vec::new)
            .push((receiver, poller, poller2));

        self
    }
}

impl<T> RegisterEntry<SyncEntry, T> {
    pub fn subscribe<M, S, R, E>(mut self, queue: u64, cfg: S::Config) -> Self
    where
        T: Send + Sync + 'static,
        M: Message,
        R: Message,
        E: StdSyncSendError,
        S: ReceiverSubscriberBuilder<T, M, R, E> + 'static,
    {
        let (inner, poller) = S::build(cfg);

        let receiver = Receiver::new::<M, R, E, S>(queue, inner);
        let poller2 = receiver.start_polling_events::<R, E>();
        self.receivers
            .entry(TypeId::of::<M>())
            .or_insert_with(Vec::new)
            .push((receiver, poller, poller2));

        self
    }
}

pub struct BusBuilder {
    receivers: Vec<(TypeId, Receiver)>,
    pollings: Vec<Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>>,
}

impl BusBuilder {
    pub fn new() -> Self {
        Self {
            receivers: Vec::new(),
            pollings: Vec::new(),
        }
    }

    pub fn register<T: Send + Sync + 'static>(self, item: T) -> RegisterEntry<SyncEntry, T> {
        RegisterEntry {
            item: Arc::new(item) as Untyped,
            builder: self,
            receivers: HashMap::new(),
            _m: Default::default(),
        }
    }

    pub fn register_unsync<T: Send + 'static>(self, item: T) -> RegisterEntry<UnsyncEntry, T> {
        RegisterEntry {
            item: Arc::new(Mutex::new(item)) as Untyped,
            builder: self,
            receivers: HashMap::new(),
            _m: Default::default(),
        }
    }

    pub fn add_recevier(
        &mut self,
        val: (TypeId, Receiver),
        poller: Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>,
        poller2: Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>,
    ) {
        self.receivers.push(val);
        self.pollings.push(poller);
        self.pollings.push(poller2);
    }

    pub fn build(self) -> (Bus, impl Future<Output = ()>) {
        let bus = Bus {
            inner: Arc::new(BusInner::new(self.receivers)),
        };

        let mut futs = Vec::with_capacity(self.pollings.len() * 2);
        for poller in self.pollings {
            futs.push(tokio::task::spawn(poller(bus.clone())));
        }

        let poller = futures::future::join_all(futs).map(|_| ()).map(|_| ());

        (bus, poller)
    }
}
