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
pub struct RegisterEntry<K, T, F, B> {
    item: Untyped,
    payload: B,
    builder: F,
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

impl<K, T: 'static, F, B> RegisterEntry<K, T, F, B> 
    where F: FnMut(&mut B, (TypeId, Receiver), Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>, Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>),
{
    pub fn done(mut self) -> B {
        for (tid, v) in self.receivers {
            for (r, poller, poller2) in v {
                let poller = poller(self.item.clone());

                (self.builder)(&mut self.payload, (tid, r), poller, poller2);
            }
        }

        self.payload
    }
}

impl<T, F, B> RegisterEntry<UnsyncEntry, T, F, B> {
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

impl<T, F, B> RegisterEntry<SyncEntry, T, F, B> {
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


pub struct Module {
    receivers: Vec<(TypeId, Receiver)>,
    pollings: Vec<Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>>,
}

impl Module {
    pub fn new() -> Self {
        Self {
            receivers: Vec::new(),
            pollings: Vec::new(),
        }
    }

    pub fn register<T: Send + Sync + 'static>(self, item: T) -> RegisterEntry<SyncEntry, T, impl FnMut(&mut Self, (TypeId, Receiver), Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>, Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>), Self> {
        RegisterEntry {
            item: Arc::new(item) as Untyped,
            payload: self,
            builder: |p: &mut Self, val, poller, poller2| {
                p.receivers.push(val);
                p.pollings.push(poller);
                p.pollings.push(poller2);
            },
            receivers: HashMap::new(),
            _m: Default::default(),
        }
    }

    pub fn register_unsync<T: Send + 'static>(self, item: T) -> RegisterEntry<UnsyncEntry, T, impl FnMut(&mut Self, (TypeId, Receiver), Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>, Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>), Self> {
        RegisterEntry {
            item: Arc::new(Mutex::new(item)) as Untyped,
            payload: self,
            builder: |p: &mut Self, val, poller, poller2| {
                p.receivers.push(val);
                p.pollings.push(poller);
                p.pollings.push(poller2);
            },
            receivers: HashMap::new(),
            _m: Default::default(),
        }
    }

    fn extend(&mut self, other: Module) {
        self.receivers.extend(other.receivers.into_iter());
        self.pollings.extend(other.pollings.into_iter());
    }
}

pub struct BusBuilder {
    inner: Module,
}

impl BusBuilder {
    pub(crate) fn new() -> Self {
        Self {
            inner: Module::new(),
        }
    }

    pub fn register<T: Send + Sync + 'static>(self, item: T) -> RegisterEntry<SyncEntry, T, impl FnMut(&mut Self, (TypeId, Receiver), Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>, Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>), Self> {
        RegisterEntry {
            item: Arc::new(item) as Untyped,
            payload: self,
            builder: |p: &mut Self, val, poller, poller2| {
                p.inner.receivers.push(val);
                p.inner.pollings.push(poller);
                p.inner.pollings.push(poller2);
            },
            receivers: HashMap::new(),
            _m: Default::default(),
        }
    }

    pub fn register_unsync<T: Send + 'static>(self, item: T) -> RegisterEntry<UnsyncEntry, T, impl FnMut(&mut Self, (TypeId, Receiver), Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>, Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>), Self> {
        RegisterEntry {
            item: Arc::new(Mutex::new(item)) as Untyped,
            payload: self,
            builder: |p: &mut Self, val, poller, poller2| {
                p.inner.receivers.push(val);
                p.inner.pollings.push(poller);
                p.inner.pollings.push(poller2);
            },
            receivers: HashMap::new(),
            _m: Default::default(),
        }
    }

    pub fn add_module(mut self, module: Module) -> Self {
        self.inner.extend(module);

        self
    }

    pub fn build(self) -> (Bus, impl Future<Output = ()>) {
        let bus = Bus {
            inner: Arc::new(BusInner::new(self.inner.receivers)),
        };

        let mut futs = Vec::with_capacity(self.inner.pollings.len() * 2);
        for poller in self.inner.pollings {
            futs.push(tokio::task::spawn(poller(bus.clone())));
        }

        let poller = futures::future::join_all(futs).map(|_| ()).map(|_| ());

        (bus, poller)
    }
}
