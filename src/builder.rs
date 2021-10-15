use core::{marker::PhantomData, pin::Pin};

use std::{collections::HashSet, sync::{Arc, atomic::{AtomicU64, Ordering}}};

use futures::{Future, FutureExt};
use tokio::sync::Mutex;

use crate::{AsyncBatchHandler, AsyncBatchSynchronizedHandler, AsyncHandler, AsyncSynchronizedHandler, BatchHandler, BatchSynchronizedHandler, Bus, BusInner, Handler, Message, Relay, SynchronizedHandler, Untyped, error::StdSyncSendError, receiver::{
        BusPollerCallback, Receiver, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver,
        UntypedPollerCallback,
    }, receivers};

static RECEVIER_ID_SEQ: AtomicU64 = AtomicU64::new(1);

pub trait ReceiverSubscriberBuilder<T, M, R, E>:
    SendUntypedReceiver + SendTypedReceiver<M> + ReciveTypedReceiver<R, E>
where
    T: 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    type Config: Default;

    fn build(cfg: Self::Config) -> (Self, UntypedPollerCallback)
    where
        Self: Sized;
}

pub struct SyncEntry;
pub struct UnsyncEntry;

#[must_use]
pub struct RegisterEntry<K, T, F, P, B> {
    item: Untyped,
    payload: B,
    builder: F,
    poller: P,
    receivers: HashSet<Receiver>,
    pollers: Vec<BusPollerCallback>,
    _m: PhantomData<(K, T)>,
}

impl<K, T: 'static, F, P, B> RegisterEntry<K, T, F, P, B>
where
    F: FnMut(&mut B, Receiver),
    P: FnMut(&mut B, BusPollerCallback),
{
    pub fn done(mut self) -> B {
        for r in self.receivers {
            (self.builder)(&mut self.payload, r);
        }

        for p in self.pollers {
            (self.poller)(&mut self.payload, p);
        }

        self.payload
    }
}

impl<T, F, P, B> RegisterEntry<UnsyncEntry, T, F, P, B> {
    pub fn subscribe<M, S, R, E>(mut self, queue: u64, cfg: S::Config) -> Self
    where
        T: Send + 'static,
        M: Message,
        R: Message,
        E: StdSyncSendError,
        S: ReceiverSubscriberBuilder<T, M, R, E> + 'static,
    {
        let (inner, poller) = S::build(cfg);

        let receiver = Receiver::new::<M, R, E, S>(RECEVIER_ID_SEQ.fetch_add(1, Ordering::Relaxed), queue, true, inner);
        let poller2 = receiver.start_polling();
        self.receivers.insert(receiver);
        self.pollers.push(poller(self.item.clone()));
        self.pollers.push(poller2);

        self
    }

    #[inline]
    pub fn subscribe_sync<M>(self, queue: u64, cfg: receivers::SynchronizedConfig) -> Self
    where
        T: SynchronizedHandler<M> + Send + 'static,
        M: Message,
        T::Response: Message,
    {
        self.subscribe::<M, receivers::SynchronizedSync<M, T::Response, T::Error>, T::Response, T::Error>(queue, cfg)
    }

    #[inline]
    pub fn subscribe_async<M>(self, queue: u64, cfg: receivers::SynchronizedConfig) -> Self
    where
        T: AsyncSynchronizedHandler<M> + Send + 'static,
        M: Message,
        T::Response: Message,
    {
        self.subscribe::<M, receivers::SynchronizedAsync<M, T::Response, T::Error>, T::Response, T::Error>(queue, cfg)
    }

    #[inline]
    pub fn subscribe_batch_sync<M>(
        self,
        queue: u64,
        cfg: receivers::SynchronizedBatchedConfig,
    ) -> Self
    where
        T: BatchSynchronizedHandler<M> + Send + 'static,
        M: Message,
        T::Response: Message,
    {
        self.subscribe::<M, receivers::SynchronizedBatchedSync<M, T::Response, T::Error>, T::Response, T::Error>(queue, cfg)
    }

    #[inline]
    pub fn subscribe_batch_async<M>(
        self,
        queue: u64,
        cfg: receivers::SynchronizedBatchedConfig,
    ) -> Self
    where
        T: AsyncBatchSynchronizedHandler<M> + Send + 'static,
        M: Message,
        T::Response: Message,
    {
        self.subscribe::<M, receivers::SynchronizedBatchedAsync<M, T::Response, T::Error>, T::Response, T::Error>(queue, cfg)
    }
}

impl<T, F, P, B> RegisterEntry<SyncEntry, T, F, P, B> {
    pub fn subscribe<M, S, R, E>(mut self, queue: u64, cfg: S::Config) -> Self
    where
        T: Send + Sync + 'static,
        M: Message,
        R: Message,
        E: StdSyncSendError,
        S: ReceiverSubscriberBuilder<T, M, R, E> + 'static,
    {
        let (inner, poller) = S::build(cfg);

        let receiver = Receiver::new::<M, R, E, S>(RECEVIER_ID_SEQ.fetch_add(1, Ordering::Relaxed), queue, true, inner);
        let poller2 = receiver.start_polling();
        self.receivers.insert(receiver);
        self.pollers.push(poller(self.item.clone()));
        self.pollers.push(poller2);

        self
    }

    #[inline]
    pub fn subscribe_sync<M>(self, queue: u64, cfg: receivers::BufferUnorderedConfig) -> Self
    where
        T: Handler<M> + Send + Sync + 'static,
        M: Message,
        T::Response: Message,
    {
        self.subscribe::<M, receivers::BufferUnorderedSync<M, T::Response, T::Error>, T::Response, T::Error>(queue, cfg)
    }

    #[inline]
    pub fn subscribe_async<M>(self, queue: u64, cfg: receivers::BufferUnorderedConfig) -> Self
    where
        T: AsyncHandler<M> + Send + Sync + 'static,
        M: Message,
        T::Response: Message,
        T::Error: StdSyncSendError,
    {
        self.subscribe::<M, receivers::BufferUnorderedAsync<M, T::Response, T::Error>, T::Response, T::Error>(queue, cfg)
    }

    #[inline]
    pub fn subscribe_batch_sync<M>(
        self,
        queue: u64,
        cfg: receivers::BufferUnorderedBatchedConfig,
    ) -> Self
    where
        T: BatchHandler<M> + Send + 'static,
        M: Message,
        T::Response: Message,
    {
        self.subscribe::<M, receivers::BufferUnorderedBatchedSync<M, T::Response, T::Error>, T::Response, T::Error>(queue, cfg)
    }

    #[inline]
    pub fn subscribe_batch_async<M>(
        self,
        queue: u64,
        cfg: receivers::BufferUnorderedBatchedConfig,
    ) -> Self
    where
        T: AsyncBatchHandler<M> + Send + 'static,
        M: Message,
        T::Response: Message,
    {
        self.subscribe::<M, receivers::BufferUnorderedBatchedAsync<M, T::Response, T::Error>, T::Response, T::Error>(queue, cfg)
    }
}

#[derive(Default)]
pub struct Module {
    receivers: HashSet<Receiver>,
    pollings: Vec<BusPollerCallback>,
}

impl Module {
    pub fn new() -> Self {
        Self {
            receivers: HashSet::new(),
            pollings: Vec::new(),
        }
    }

    pub fn register_relay<S: Relay + Send + Sync + 'static>(mut self, inner: S) -> Self {
        let receiver = Receiver::new_relay::<S>(RECEVIER_ID_SEQ.fetch_add(1, Ordering::Relaxed), inner);
        self.pollings.push(receiver.start_polling());
        self.receivers.insert(receiver);

        self
    }

    pub fn register<T: Send + Sync + 'static>(
        self,
        item: T,
    ) -> RegisterEntry<
        SyncEntry,
        T,
        impl FnMut(&mut Self, Receiver),
        impl FnMut(&mut Self, Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>),
        Self,
    > {
        RegisterEntry {
            item: Arc::new(item) as Untyped,
            payload: self,
            builder: |p: &mut Self, r| { p.receivers.insert(r); },
            poller: |p: &mut Self, poller| p.pollings.push(poller),
            receivers: HashSet::new(),
            pollers: Vec::new(),
            _m: Default::default(),
        }
    }

    pub fn register_unsync<T: Send + 'static>(
        self,
        item: T,
    ) -> RegisterEntry<
        UnsyncEntry,
        T,
        impl FnMut(&mut Self, Receiver),
        impl FnMut(&mut Self, Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>),
        Self,
    > {
        let item = Arc::new(Mutex::new(item)) as Untyped;

        RegisterEntry {
            item,
            payload: self,
            builder: |p: &mut Self, r| { p.receivers.insert(r); },
            poller: |p: &mut Self, poller| p.pollings.push(poller),
            receivers: HashSet::new(),
            pollers: Vec::new(),
            _m: Default::default(),
        }
    }

    pub fn add_module(mut self, module: Module) -> Self {
        self.pollings.extend(module.pollings);
        self.receivers.extend(module.receivers);

        self
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

    pub fn register_relay<S: Relay + Send + Sync + 'static>(self, inner: S) -> Self {
        let inner = self.inner.register_relay(inner);

        BusBuilder { inner }
    }

    pub fn register<T: Send + Sync + 'static>(
        self,
        item: T,
    ) -> RegisterEntry<
        SyncEntry,
        T,
        impl FnMut(&mut Self, Receiver),
        impl FnMut(&mut Self, Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>),
        Self,
    > {
        RegisterEntry {
            item: Arc::new(item) as Untyped,
            payload: self,
            builder: |p: &mut Self, r| {  p.inner.receivers.insert(r); },
            poller: |p: &mut Self, poller| p.inner.pollings.push(poller),
            receivers: HashSet::new(),
            pollers: Vec::new(),
            _m: Default::default(),
        }
    }

    pub fn register_unsync<T: Send + 'static>(
        self,
        item: T,
    ) -> RegisterEntry<
        UnsyncEntry,
        T,
        impl FnMut(&mut Self, Receiver),
        impl FnMut(&mut Self, Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>),
        Self,
    > {
        RegisterEntry {
            item: Arc::new(Mutex::new(item)) as Untyped,
            payload: self,
            builder: |p: &mut Self, r| { p.inner.receivers.insert(r); },
            poller: |p: &mut Self, poller| p.inner.pollings.push(poller),
            receivers: HashSet::new(),
            pollers: Vec::new(),
            _m: Default::default(),
        }
    }

    pub fn add_module(mut self, module: Module) -> Self {
        self.inner = self.inner.add_module(module);

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

        bus.init();

        (bus, poller)
    }
}
