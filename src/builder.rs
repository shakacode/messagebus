use std::{any::TypeId, collections::HashMap, marker::PhantomData, pin::Pin, sync::Arc};

use futures::{Future, FutureExt};
use receiver::ReceiverTrait;

use crate::{
    receiver::{self, Receiver},
    Bus, BusInner, Message, Untyped,
};

pub trait ReceiverSubscriber<T: 'static> {
    fn subscribe(
        self,
    ) -> (
        Arc<dyn ReceiverTrait>,
        Box<
            dyn FnOnce(Untyped) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>,
        >,
    );
}

pub trait ReceiverSubscriberBuilder<M, T: 'static> {
    type Entry: ReceiverSubscriber<T>;
    type Config: Default;

    fn build(cfg: Self::Config) -> Self::Entry;
}

pub struct SyncEntry;
pub struct UnsyncEntry;

pub struct LocalEntry;

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
        )>,
    >,
    _m: PhantomData<(K, T)>,
}

impl<K, T: 'static> RegisterEntry<K, T> {
    pub fn done(self) -> BusBuilder {
        let mut builder = self.builder;

        for (tid, v) in self.receivers {
            for (r, poller) in v {
                let poller = poller(self.item.clone());

                builder.add_recevier((tid, r), poller);
            }
        }

        builder
    }
}


impl<T: 'static> RegisterEntry<LocalEntry, T> {
    pub fn subscribe<M, R>(mut self, cfg: R::Config) -> Self
    where
        T: 'static,
        M: Message + 'static,
        R: ReceiverSubscriberBuilder<M, T> + 'static,
    {
        let (inner, poller) = R::build(cfg).subscribe();

        let receiver = Receiver::new(inner);
        self.receivers
            .entry(TypeId::of::<M>())
            .or_insert_with(Vec::new)
            .push((receiver, poller));

        self
    }
}


impl<T: Send + 'static> RegisterEntry<UnsyncEntry, T> {
    pub fn subscribe<M, R>(mut self, cfg: R::Config) -> Self
    where
        T: Send + 'static,
        M: Message + 'static,
        R: ReceiverSubscriberBuilder<M, T> + 'static,
    {
        let (inner, poller) = R::build(cfg).subscribe();

        let receiver = Receiver::new(inner);
        self.receivers
            .entry(TypeId::of::<M>())
            .or_insert_with(Vec::new)
            .push((receiver, poller));

        self
    }
}

impl<T: Send + Sync + 'static> RegisterEntry<SyncEntry, T> {
    pub fn subscribe<M, R>(mut self, cfg: R::Config) -> Self
    where
        T: Send + 'static,
        M: Message + 'static,
        R: ReceiverSubscriberBuilder<M, T> + 'static,
    {
        let (inner, poller) = R::build(cfg).subscribe();

        let receiver = Receiver::new(inner);
        self.receivers
            .entry(TypeId::of::<M>())
            .or_insert_with(Vec::new)
            .push((receiver, poller));

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
            item: Untyped::new_readonly(item),
            builder: self,
            receivers: HashMap::new(),
            _m: Default::default(),
        }
    }

    pub fn register_unsync<T: Send + 'static>(self, item: T) -> RegisterEntry<UnsyncEntry, T> {
        RegisterEntry {
            item: Untyped::new_mutex(item),
            builder: self,
            receivers: HashMap::new(),
            _m: Default::default(),
        }
    }

    pub fn register_local<T: 'static>(
        self,
        item: impl FnOnce() -> T + Send + 'static,
    ) -> RegisterEntry<LocalEntry, T> {
        RegisterEntry {
            item: Untyped::new_local(item),
            builder: self,
            receivers: HashMap::new(),
            _m: Default::default(),
        }
    }

    pub fn add_recevier(
        &mut self,
        val: (TypeId, Receiver),
        poller: Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>,
    ) {
        self.receivers.push(val);
        self.pollings.push(poller);
    }

    pub fn build(self) -> (Bus, impl Future<Output = ()>) {
        let bus = Bus {
            inner: Arc::new(BusInner::new(self.receivers)),
        };

        let mut futs = Vec::with_capacity(self.pollings.len());
        for poller in self.pollings {
            futs.push(tokio::task::spawn(poller(bus.clone())));
        }

        let poller = futures::future::join_all(futs).map(|_| ()).map(|_| ());

        (bus, poller)
    }
}
