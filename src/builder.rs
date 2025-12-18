//! Builder pattern for constructing a message bus.
//!
//! This module provides [`BusBuilder`] and [`Module`] for registering handlers
//! and configuring subscriptions before building the bus.
//!
//! # Example
//!
//! ```rust,ignore
//! use messagebus::{Bus, derive::Message};
//!
//! #[derive(Debug, Clone, Message)]
//! struct MyMessage(String);
//!
//! // Build the bus with handlers
//! let (bus, poller) = Bus::build()
//!     .register(MyHandler)
//!     .subscribe_async::<MyMessage>(8, Default::default())
//!     .done()
//!     .build();
//!
//! // The poller future must be awaited to process messages
//! tokio::spawn(poller);
//! ```
//!
//! # Modular Registration
//!
//! Use [`Module`] to group related handlers:
//!
//! ```rust,ignore
//! fn create_user_module() -> Module {
//!     Module::new()
//!         .register(UserHandler::new())
//!         .subscribe_async::<CreateUser>(8, Default::default())
//!         .subscribe_async::<DeleteUser>(8, Default::default())
//!         .done()
//! }
//!
//! let (bus, poller) = Bus::build()
//!     .add_module(create_user_module())
//!     .build();
//! ```

use core::{marker::PhantomData, pin::Pin};

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use futures::{Future, FutureExt};
use tokio::sync::Mutex;

use crate::{
    error::StdSyncSendError,
    receiver::{
        BusPollerCallback, Receiver, ReciveTypedReceiver, SendTypedReceiver, SendUntypedReceiver,
        UntypedPollerCallback,
    },
    receivers, AsyncBatchHandler, AsyncBatchSynchronizedHandler, AsyncHandler,
    AsyncSynchronizedHandler, BatchHandler, BatchSynchronizedHandler, Bus, BusInner, Handler,
    Message, Relay, SynchronizedHandler, Untyped,
};

static RECEVIER_ID_SEQ: AtomicU64 = AtomicU64::new(1);

/// Trait for building receiver subscribers with specific configurations.
///
/// This is an internal trait used by the builder to create receivers
/// with the appropriate configuration and polling callbacks.
pub trait ReceiverSubscriberBuilder<T, M, R, E>:
    SendUntypedReceiver + SendTypedReceiver<M> + ReciveTypedReceiver<R, E>
where
    T: 'static,
    M: Message,
    R: Message,
    E: StdSyncSendError,
{
    /// The configuration type for this receiver.
    type Config: Default;

    /// Builds a receiver with the given configuration.
    fn build(cfg: Self::Config) -> (Self, UntypedPollerCallback)
    where
        Self: Sized;
}

/// Marker type for thread-safe (Send + Sync) handler entries.
pub struct SyncEntry;

/// Marker type for non-thread-safe (Send only) handler entries.
pub struct UnsyncEntry;

/// A builder entry for registering a handler and its subscriptions.
///
/// This type is returned by [`BusBuilder::register`] and [`Module::register`].
/// Use the `subscribe_*` methods to add message subscriptions, then call
/// [`done()`](Self::done) to finalize registration.
///
/// # Type Parameters
///
/// - `K` - Entry kind marker ([`SyncEntry`] or [`UnsyncEntry`])
/// - `T` - The handler type being registered
/// - `F`, `P`, `B` - Internal builder function types
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
    /// Finalizes handler registration and returns to the builder.
    ///
    /// Call this after adding all desired subscriptions.
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

/// Methods for unsync (mutable state) handler subscriptions.
impl<T, F, P, B> RegisterEntry<UnsyncEntry, T, F, P, B> {
    /// Low-level subscription method with custom receiver type.
    pub fn subscribe<M, S, R, E>(mut self, queue: u64, cfg: S::Config) -> Self
    where
        T: Send + 'static,
        M: Message,
        R: Message,
        E: StdSyncSendError,
        S: ReceiverSubscriberBuilder<T, M, R, E> + 'static,
    {
        let (inner, poller) = S::build(cfg);

        let receiver = Receiver::new::<M, R, E, S>(
            RECEVIER_ID_SEQ.fetch_add(1, Ordering::Relaxed),
            queue,
            true,
            inner,
        );

        let poller2 = receiver.start_polling();
        self.receivers.insert(receiver);
        self.pollers.push(poller(self.item.clone()));
        self.pollers.push(poller2);

        self
    }

    /// Subscribes to a message type using [`SynchronizedHandler`].
    ///
    /// # Parameters
    ///
    /// - `queue` - Queue capacity for buffering messages
    /// - `cfg` - Receiver configuration
    #[inline]
    pub fn subscribe_sync<M>(self, queue: u64, cfg: receivers::SynchronizedConfig) -> Self
    where
        T: SynchronizedHandler<M> + Send + 'static,
        M: Message,
        T::Response: Message,
    {
        self.subscribe::<M, receivers::SynchronizedSync<M, T::Response, T::Error>, T::Response, T::Error>(queue, cfg)
    }

    /// Subscribes to a message type using [`AsyncSynchronizedHandler`].
    ///
    /// # Parameters
    ///
    /// - `queue` - Queue capacity for buffering messages
    /// - `cfg` - Receiver configuration
    #[inline]
    pub fn subscribe_async<M>(self, queue: u64, cfg: receivers::SynchronizedConfig) -> Self
    where
        T: AsyncSynchronizedHandler<M> + Send + 'static,
        M: Message,
        T::Response: Message,
    {
        self.subscribe::<M, receivers::SynchronizedAsync<M, T::Response, T::Error>, T::Response, T::Error>(queue, cfg)
    }

    /// Subscribes to a message type using [`BatchSynchronizedHandler`].
    ///
    /// # Parameters
    ///
    /// - `queue` - Queue capacity for buffering messages
    /// - `cfg` - Batch configuration including batch size
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

    /// Subscribes to a message type using [`AsyncBatchSynchronizedHandler`].
    ///
    /// # Parameters
    ///
    /// - `queue` - Queue capacity for buffering messages
    /// - `cfg` - Batch configuration including batch size
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

/// Methods for sync (thread-safe) handler subscriptions.
impl<T, F, P, B> RegisterEntry<SyncEntry, T, F, P, B> {
    /// Low-level subscription method with custom receiver type.
    pub fn subscribe<M, S, R, E>(mut self, queue: u64, cfg: S::Config) -> Self
    where
        T: Send + Sync + 'static,
        M: Message,
        R: Message,
        E: StdSyncSendError,
        S: ReceiverSubscriberBuilder<T, M, R, E> + 'static,
    {
        let (inner, poller) = S::build(cfg);

        let receiver = Receiver::new::<M, R, E, S>(
            RECEVIER_ID_SEQ.fetch_add(1, Ordering::Relaxed),
            queue,
            true,
            inner,
        );
        let poller2 = receiver.start_polling();
        self.receivers.insert(receiver);
        self.pollers.push(poller(self.item.clone()));
        self.pollers.push(poller2);

        self
    }

    /// Subscribes to a message type using [`Handler`].
    ///
    /// # Parameters
    ///
    /// - `queue` - Queue capacity for buffering messages
    /// - `cfg` - Receiver configuration
    #[inline]
    pub fn subscribe_sync<M>(self, queue: u64, cfg: receivers::BufferUnorderedConfig) -> Self
    where
        T: Handler<M> + Send + Sync + 'static,
        M: Message,
        T::Response: Message,
    {
        self.subscribe::<M, receivers::BufferUnorderedSync<M, T::Response, T::Error>, T::Response, T::Error>(queue, cfg)
    }

    /// Subscribes to a message type using [`AsyncHandler`].
    ///
    /// # Parameters
    ///
    /// - `queue` - Queue capacity for buffering messages
    /// - `cfg` - Receiver configuration
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

    /// Subscribes to a message type using [`BatchHandler`].
    ///
    /// # Parameters
    ///
    /// - `queue` - Queue capacity for buffering messages
    /// - `cfg` - Batch configuration including batch size
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

    /// Subscribes to a message type using [`AsyncBatchHandler`].
    ///
    /// # Parameters
    ///
    /// - `queue` - Queue capacity for buffering messages
    /// - `cfg` - Batch configuration including batch size
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

/// A collection of handlers that can be added to a bus as a unit.
///
/// Modules allow you to organize related handlers together and compose
/// them into larger applications. Create a module, register handlers
/// with their subscriptions, then add the module to a `BusBuilder`.
///
/// # Example
///
/// ```rust,ignore
/// fn create_auth_module() -> Module {
///     Module::new()
///         .register(AuthHandler::new())
///         .subscribe_async::<LoginRequest>(8, Default::default())
///         .subscribe_async::<LogoutRequest>(8, Default::default())
///         .done()
/// }
///
/// let (bus, poller) = Bus::build()
///     .add_module(create_auth_module())
///     .build();
/// ```
#[derive(Default)]
pub struct Module {
    receivers: HashSet<Receiver>,
    pollings: Vec<BusPollerCallback>,
}

impl Module {
    /// Creates a new empty module.
    pub fn new() -> Self {
        Self {
            receivers: HashSet::new(),
            pollings: Vec::new(),
        }
    }

    /// Registers a relay for forwarding messages to remote buses.
    pub fn register_relay<S: Relay + Send + Sync + 'static>(mut self, inner: S) -> Self {
        let receiver =
            Receiver::new_relay::<S>(RECEVIER_ID_SEQ.fetch_add(1, Ordering::Relaxed), inner);
        self.pollings.push(receiver.start_polling());
        self.receivers.insert(receiver);

        self
    }

    /// Registers a thread-safe handler (Send + Sync).
    ///
    /// Use this for handlers that:
    /// - Are stateless or use interior mutability (e.g., `Arc<Mutex<T>>`)
    /// - Implement [`Handler`] or [`AsyncHandler`]
    ///
    /// Returns a `RegisterEntry` to configure message subscriptions.
    #[allow(clippy::type_complexity)]
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
            builder: |p: &mut Self, r| {
                p.receivers.insert(r);
            },
            poller: |p: &mut Self, poller| p.pollings.push(poller),
            receivers: HashSet::new(),
            pollers: Vec::new(),
            _m: Default::default(),
        }
    }

    /// Registers a handler with mutable state (Send only, not Sync).
    ///
    /// Use this for handlers that:
    /// - Maintain mutable state that needs direct `&mut self` access
    /// - Implement [`SynchronizedHandler`] or [`AsyncSynchronizedHandler`]
    ///
    /// Messages are processed sequentially to ensure exclusive access.
    /// Returns a `RegisterEntry` to configure message subscriptions.
    #[allow(clippy::type_complexity)]
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
            builder: |p: &mut Self, r| {
                p.receivers.insert(r);
            },
            poller: |p: &mut Self, poller| p.pollings.push(poller),
            receivers: HashSet::new(),
            pollers: Vec::new(),
            _m: Default::default(),
        }
    }

    /// Adds another module's handlers to this module.
    pub fn add_module(mut self, module: Module) -> Self {
        self.pollings.extend(module.pollings);
        self.receivers.extend(module.receivers);

        self
    }
}

/// Builder for constructing a [`Bus`] instance.
///
/// Obtained via [`Bus::build()`]. Use this to register handlers
/// and configure subscriptions before building the bus.
///
/// # Example
///
/// ```rust,ignore
/// let (bus, poller) = Bus::build()
///     .register(MyHandler)
///     .subscribe_async::<MyMessage>(8, Default::default())
///     .done()
///     .build();
///
/// // Spawn the poller to process messages
/// tokio::spawn(poller);
///
/// // Now you can send messages
/// bus.send(MyMessage { data: "hello".into() }).await?;
/// ```
pub struct BusBuilder {
    inner: Module,
}

impl BusBuilder {
    pub(crate) fn new() -> Self {
        Self {
            inner: Module::new(),
        }
    }

    /// Registers a relay for forwarding messages to remote buses.
    pub fn register_relay<S: Relay + Send + Sync + 'static>(self, inner: S) -> Self {
        let inner = self.inner.register_relay(inner);

        BusBuilder { inner }
    }

    /// Registers a thread-safe handler (Send + Sync).
    ///
    /// Use this for handlers that are stateless or use interior mutability.
    /// For handlers with mutable state, use [`register_unsync`](Self::register_unsync).
    ///
    /// Returns a [`RegisterEntry`] to configure message subscriptions.
    #[allow(clippy::type_complexity)]
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
            builder: |p: &mut Self, r| {
                p.inner.receivers.insert(r);
            },
            poller: |p: &mut Self, poller| p.inner.pollings.push(poller),
            receivers: HashSet::new(),
            pollers: Vec::new(),
            _m: Default::default(),
        }
    }

    /// Registers a handler with mutable state (Send only, not Sync).
    ///
    /// Use this for handlers that need `&mut self` access. Messages are
    /// processed sequentially to ensure exclusive mutable access.
    ///
    /// Returns a [`RegisterEntry`] to configure message subscriptions.
    #[allow(clippy::type_complexity)]
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
            builder: |p: &mut Self, r| {
                p.inner.receivers.insert(r);
            },
            poller: |p: &mut Self, poller| p.inner.pollings.push(poller),
            receivers: HashSet::new(),
            pollers: Vec::new(),
            _m: Default::default(),
        }
    }

    /// Adds a module's handlers to this builder.
    ///
    /// Modules allow grouping related handlers for better organization.
    pub fn add_module(mut self, module: Module) -> Self {
        self.inner = self.inner.add_module(module);

        self
    }

    /// Builds the bus and returns it along with a poller future.
    ///
    /// # Returns
    ///
    /// A tuple of:
    /// - `Bus` - The constructed message bus
    /// - `impl Future<Output = ()>` - A poller that must be awaited to process messages
    ///
    /// # Important
    ///
    /// The poller future **must** be spawned or awaited for the bus to function.
    /// Typically you'll spawn it as a background task:
    ///
    /// ```rust,ignore
    /// let (bus, poller) = Bus::build()
    ///     // ... register handlers ...
    ///     .build();
    ///
    /// tokio::spawn(poller);  // Required for message processing
    /// ```
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
