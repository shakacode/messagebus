use std::{collections::HashMap, marker::PhantomData, pin::Pin, sync::Arc};

use futures::{Future, FutureExt};
use smallvec::SmallVec;
use tokio::sync::Mutex;

use crate::{
    envelop::TypeTag,
    error::{Error, StdSyncSendError},
    receiver::{Receiver, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver},
    receivers, AsyncBatchHandler, AsyncBatchSynchronizedHandler, AsyncHandler,
    AsyncSynchronizedHandler, BatchHandler, BatchSynchronizedHandler, Bus, BusInner, Handler,
    IntoBoxedMessage, Message, Relay, SynchronizedHandler, Untyped,
};

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
pub struct RegisterEntry<K, T, F, P, B> {
    item: Untyped,
    payload: B,
    builder: F,
    poller: P,
    receivers: HashMap<TypeTag, Receiver>,
    pollers: Vec<Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>>,
    _m: PhantomData<(K, T)>,
}

impl<K, T: 'static, F, P, B> RegisterEntry<K, T, F, P, B>
where
    F: FnMut(&mut B, TypeTag, Receiver),
    P: FnMut(&mut B, Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>),
{
    pub fn done(mut self) -> B {
        for (tid, v) in self.receivers {
            (self.builder)(&mut self.payload, tid, v);
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

        let receiver = Receiver::new::<M, R, E, S>(queue, inner);
        let poller2 = receiver.start_polling();
        self.receivers.insert(M::type_tag_(), receiver);
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

        let receiver = Receiver::new::<M, R, E, S>(queue, inner);
        let poller2 = receiver.start_polling();
        self.receivers.insert(M::type_tag_(), receiver);
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

pub struct MessageTypeDescriptor {
    de: Box<
        dyn Fn(&mut dyn erased_serde::Deserializer<'_>) -> Result<Box<dyn Message>, Error>
            + Send
            + Sync,
    >,
}

impl MessageTypeDescriptor {
    #[inline]
    pub fn deserialize_boxed(
        &self,
        de: &mut dyn erased_serde::Deserializer<'_>,
    ) -> Result<Box<dyn Message>, Error> {
        (self.de)(de)
    }
}

pub struct Module {
    message_types: HashMap<TypeTag, MessageTypeDescriptor>,
    receivers: HashMap<TypeTag, SmallVec<[Receiver; 4]>>,
    pollings: Vec<Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>>,
}

impl Module {
    pub fn new() -> Self {
        Self {
            message_types: HashMap::new(),
            receivers: HashMap::new(),
            pollings: Vec::new(),
        }
    }

    pub fn register_shared_message<
        M: Message + Clone + serde::Serialize + serde::de::DeserializeOwned,
    >(
        mut self,
    ) -> Self {
        println!("insert {}", M::type_tag_());
        self.message_types.insert(
            M::type_tag_(),
            MessageTypeDescriptor {
                de: Box::new(move |de| Ok(M::deserialize(de)?.into_boxed())),
            },
        );

        self
    }

    pub fn register_relay<S: Relay + Send + Sync + 'static>(mut self, inner: S) -> Self {
        let receiver = Receiver::new_relay::<S>(inner);
        self.pollings.push(receiver.start_polling());

        let mut receiver_added = false;
        receiver.iter_types(&mut |msg, _, _| {
            self.receivers
                .entry(msg.clone())
                .or_insert_with(SmallVec::new)
                .push(receiver.clone());

            if !receiver_added {
                receiver_added = true;
                false
            } else {
                true
            }
        });

        if !receiver_added {
            self.receivers
                .entry("".into())
                .or_insert_with(SmallVec::new)
                .push(receiver);
        }

        self
    }

    pub fn register<T: Send + Sync + 'static>(
        self,
        item: T,
    ) -> RegisterEntry<
        SyncEntry,
        T,
        impl FnMut(&mut Self, TypeTag, Receiver),
        impl FnMut(&mut Self, Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>),
        Self,
    > {
        RegisterEntry {
            item: Arc::new(item) as Untyped,
            payload: self,
            builder: |p: &mut Self, tt, r| {
                p.receivers.entry(tt).or_insert_with(SmallVec::new).push(r);
            },
            poller: |p: &mut Self, poller| p.pollings.push(poller),
            receivers: HashMap::new(),
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
        impl FnMut(&mut Self, TypeTag, Receiver),
        impl FnMut(&mut Self, Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>),
        Self,
    > {
        let item = Arc::new(Mutex::new(item)) as Untyped;

        RegisterEntry {
            item,
            payload: self,
            builder: |p: &mut Self, tt, r| {
                p.receivers.entry(tt).or_insert_with(SmallVec::new).push(r);
            },
            poller: |p: &mut Self, poller| p.pollings.push(poller),
            receivers: HashMap::new(),
            pollers: Vec::new(),
            _m: Default::default(),
        }
    }

    pub fn add_module(mut self, module: Module) -> Self {
        self.message_types.extend(module.message_types);
        self.pollings.extend(module.pollings);

        for (k, v) in module.receivers {
            self.receivers
                .entry(k)
                .or_insert_with(Default::default)
                .extend(v);
        }

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

    pub fn register_shared_message<
        M: Message + Clone + serde::Serialize + serde::de::DeserializeOwned,
    >(
        self,
    ) -> Self {
        let inner = self.inner.register_shared_message::<M>();

        BusBuilder { inner }
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
        impl FnMut(&mut Self, TypeTag, Receiver),
        impl FnMut(&mut Self, Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>),
        Self,
    > {
        RegisterEntry {
            item: Arc::new(item) as Untyped,
            payload: self,
            builder: |p: &mut Self, tt, r| {
                p.inner
                    .receivers
                    .entry(tt)
                    .or_insert_with(SmallVec::new)
                    .push(r);
            },
            poller: |p: &mut Self, poller| p.inner.pollings.push(poller),
            receivers: HashMap::new(),
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
        impl FnMut(&mut Self, TypeTag, Receiver),
        impl FnMut(&mut Self, Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>>),
        Self,
    > {
        RegisterEntry {
            item: Arc::new(Mutex::new(item)) as Untyped,
            payload: self,
            builder: |p: &mut Self, tt, r| {
                p.inner
                    .receivers
                    .entry(tt)
                    .or_insert_with(SmallVec::new)
                    .push(r);
            },
            poller: |p: &mut Self, poller| p.inner.pollings.push(poller),
            receivers: HashMap::new(),
            pollers: Vec::new(),
            _m: Default::default(),
        }
    }

    pub fn add_module(mut self, module: Module) -> Self {
        self.inner = self.inner.add_module(module);

        self
    }

    pub fn build(self) -> (Bus, impl Future<Output = ()>) {
        let mut receivers = HashMap::new();

        for (key, values) in self.inner.receivers {
            for v in values {
                receivers
                    .entry(key.clone())
                    .or_insert_with(SmallVec::new)
                    .push(v);
            }
        }

        let bus = Bus {
            inner: Arc::new(BusInner::new(receivers, self.inner.message_types)),
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
