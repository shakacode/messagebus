//! # MessageBus
//!
//! An async message bus library for Rust, inspired by Actix. It enables actor-style
//! communication between components using typed messages routed through receivers.
//!
//! ## Overview
//!
//! MessageBus provides a flexible, type-safe message passing system that supports:
//!
//! - **Multiple handler types**: synchronous, asynchronous, batched, and synchronized handlers
//! - **Broadcast messaging**: send messages to all registered receivers
//! - **Request/Response patterns**: send a message and await a typed response
//! - **Serialization support**: messages can be serialized for remote transport
//! - **Backpressure handling**: built-in support for handling full queues
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use messagebus::{Bus, AsyncHandler, Message, derive::Message, error};
//! use async_trait::async_trait;
//!
//! // Define a message type
//! #[derive(Debug, Clone, Message)]
//! struct MyMessage(String);
//!
//! // Define a handler
//! struct MyHandler;
//!
//! #[async_trait]
//! impl AsyncHandler<MyMessage> for MyHandler {
//!     type Error = error::GenericError;
//!     type Response = ();
//!
//!     async fn handle(&self, msg: MyMessage, _bus: &Bus) -> Result<(), Self::Error> {
//!         println!("Received: {}", msg.0);
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let (bus, poller) = Bus::build()
//!         .register(MyHandler)
//!         .subscribe_async::<MyMessage>(8, Default::default())
//!         .done()
//!         .build();
//!
//!     bus.send(MyMessage("Hello!".to_string())).await.unwrap();
//!     bus.flush_all().await;
//!     bus.close().await;
//!     poller.await;
//! }
//! ```
//!
//! ## Handler Types
//!
//! | Handler | Thread-Safe | Batched | Async | Use Case |
//! |---------|-------------|---------|-------|----------|
//! | [`Handler`] | Yes | No | No | Simple sync processing |
//! | [`AsyncHandler`] | Yes | No | Yes | Async I/O operations |
//! | [`SynchronizedHandler`] | No | No | No | Mutable state (sync) |
//! | [`AsyncSynchronizedHandler`] | No | No | Yes | Mutable state (async) |
//! | [`BatchHandler`] | Yes | Yes | No | Bulk sync processing |
//! | [`AsyncBatchHandler`] | Yes | Yes | Yes | Bulk async processing |
//! | [`BatchSynchronizedHandler`] | No | Yes | No | Batched mutable state |
//! | [`AsyncBatchSynchronizedHandler`] | No | Yes | Yes | Batched async mutable state |
//!
//! ## Message Derive Macro
//!
//! Use `#[derive(Message)]` with optional attributes:
//!
//! - `#[message(clone)]` - Enable message cloning for broadcast
//! - `#[message(shared)]` - Enable serialization for remote transport
//! - `#[type_tag("custom::name")]` - Custom type identifier
//! - `#[namespace("my_namespace")]` - Type tag namespace prefix

mod builder;
mod envelop;
pub mod error;
mod handler;
mod receiver;
pub mod receivers;
mod relay;
mod stats;
mod trait_object;
pub mod type_tag;

pub mod __reexport {
    pub use ctor;
    pub use serde;
}

#[macro_use]
extern crate log;

pub mod derive {
    pub use messagebus_derive::*;
}

// privavte
use core::{
    any::Any,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};
use smallvec::SmallVec;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::Mutex;

use error::{Error, SendError, StdSyncSendError};
use receiver::{Permit, Receiver};

// public
pub use builder::{BusBuilder, Module, RegisterEntry, SyncEntry, UnsyncEntry};
pub use ctor;
pub use envelop::{IntoBoxedMessage, Message, MessageBounds, SharedMessage, TypeTag, TypeTagged};
pub use handler::*;
pub use receiver::{
    Action, Event, EventBoxed, ReciveTypedReceiver, ReciveUntypedReceiver, SendTypedReceiver,
    SendUntypedReceiver, TypeTagAccept, TypeTagAcceptItem,
};
pub use relay::Relay;
pub use stats::Stats;
pub use type_tag::{deserialize_shared_message, register_shared_message};
pub type Untyped = Arc<dyn Any + Send + Sync>;

type LookupQuery = (TypeTag, Option<TypeTag>, Option<TypeTag>);

static ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Options for controlling how messages are routed to receivers.
///
/// By default, messages are broadcast to all receivers that handle the message type.
/// Use these options to customize routing behavior.
///
/// # Examples
///
/// ```rust,no_run
/// use messagebus::{Bus, AsyncHandler, SendOptions, error};
/// use messagebus::derive::Message;
/// use async_trait::async_trait;
///
/// #[derive(Debug, Clone, Message)]
/// #[message(clone)]
/// struct Msg(String);
///
/// struct Handler;
///
/// #[async_trait]
/// impl AsyncHandler<Msg> for Handler {
///     type Error = error::GenericError;
///     type Response = ();
///     async fn handle(&self, _: Msg, _: &Bus) -> Result<(), Self::Error> { Ok(()) }
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let (bus, poller) = Bus::build()
///         .register(Handler)
///         .subscribe_async::<Msg>(8, Default::default())
///         .done()
///         .build();
///     tokio::spawn(poller);
///     bus.ready().await;
///
///     let msg = Msg("hello".into());
///     // Send to all receivers (default)
///     bus.send(msg.clone()).await?;
///     // Send with explicit options
///     bus.send_ext(msg, SendOptions::Broadcast).await?;
///
///     bus.close().await;
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum SendOptions {
    /// Send the message to all registered receivers for this message type.
    /// This is the default behavior.
    #[default]
    Broadcast,

    /// Send to all receivers except the one with the specified ID.
    /// Useful for avoiding message loops when forwarding.
    Except(u64),

    /// Send only to the receiver with the specified ID.
    /// The message will not be delivered if no receiver matches.
    Direct(u64),

    /// Select a random receiver from the available receivers.
    /// Useful for load distribution.
    Random,

    /// Select the receiver with the least load.
    /// Useful for load balancing across multiple receivers.
    Balanced,
}

/// Internal state of the message bus. Not intended for direct use.
pub struct BusInner {
    receivers: HashSet<Receiver>,
    lookup: HashMap<LookupQuery, SmallVec<[Receiver; 4]>>,
    closed: AtomicBool,
    maintain: Mutex<()>,
}

impl BusInner {
    pub(crate) fn new(receivers: HashSet<Receiver>) -> Self {
        let mut lookup = HashMap::new();
        for recv in receivers.iter() {
            for (msg, resp) in recv.iter_types() {
                lookup
                    .entry((msg.clone(), None, None))
                    .or_insert_with(HashSet::new)
                    .insert(recv.clone());

                if let Some((resp, err)) = resp {
                    lookup
                        .entry((msg.clone(), Some(resp.clone()), None))
                        .or_insert_with(HashSet::new)
                        .insert(recv.clone());

                    lookup
                        .entry((msg.clone(), None, Some(err.clone())))
                        .or_insert_with(HashSet::new)
                        .insert(recv.clone());

                    lookup
                        .entry((msg, Some(resp), Some(err)))
                        .or_insert_with(HashSet::new)
                        .insert(recv.clone());
                }
            }
        }

        let lookup = lookup
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().collect()))
            .collect();

        Self {
            receivers,
            lookup,
            closed: AtomicBool::new(false),
            maintain: Mutex::new(()),
        }
    }
}

/// The central message dispatcher that routes messages to receivers.
///
/// `Bus` is the main entry point for sending messages in the messagebus system.
/// It maintains a registry of receivers and routes messages to them based on
/// their type.
///
/// # Creating a Bus
///
/// Use [`Bus::build()`] to create a new bus with the builder pattern:
///
/// ```rust,no_run
/// use messagebus::{Bus, AsyncHandler, error};
/// use messagebus::derive::Message;
/// use async_trait::async_trait;
///
/// #[derive(Debug, Clone, Message)]
/// struct MyMessage(String);
///
/// struct MyHandler;
///
/// #[async_trait]
/// impl AsyncHandler<MyMessage> for MyHandler {
///     type Error = error::GenericError;
///     type Response = ();
///     async fn handle(&self, _: MyMessage, _: &Bus) -> Result<(), Self::Error> { Ok(()) }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     let (bus, poller) = Bus::build()
///         .register(MyHandler)
///         .subscribe_async::<MyMessage>(8, Default::default())
///         .done()
///         .build();
///
///     // The poller future must be awaited to process messages
///     tokio::spawn(poller);
/// }
/// ```
///
/// # Sending Messages
///
/// Messages can be sent in several ways:
///
/// - [`send()`](Bus::send) - Async send to all receivers (broadcast)
/// - [`send_one()`](Bus::send_one) - Async send to one receiver
/// - [`try_send()`](Bus::try_send) - Non-blocking send, fails if buffer full
/// - [`force_send()`](Bus::force_send) - Send without waiting for buffer space
/// - [`request()`](Bus::request) - Send and await a response
///
/// # Lifecycle
///
/// 1. Build the bus with receivers
/// 2. Spawn the poller future
/// 3. Send messages
/// 4. Call [`flush_all()`](Bus::flush_all) to ensure all messages are processed
/// 5. Call [`close()`](Bus::close) to shut down
/// 6. Await the poller to complete
#[derive(Clone)]
pub struct Bus {
    inner: Arc<BusInner>,
}

impl Bus {
    /// Creates a new [`BusBuilder`] for constructing a bus.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use messagebus::{Bus, AsyncHandler, error};
    /// use messagebus::derive::Message;
    /// use async_trait::async_trait;
    ///
    /// #[derive(Debug, Clone, Message)]
    /// struct Msg;
    ///
    /// struct MyHandler;
    /// #[async_trait]
    /// impl AsyncHandler<Msg> for MyHandler {
    ///     type Error = error::GenericError;
    ///     type Response = ();
    ///     async fn handle(&self, _: Msg, _: &Bus) -> Result<(), Self::Error> { Ok(()) }
    /// }
    ///
    /// let (bus, poller) = Bus::build()
    ///     .register(MyHandler)
    ///     .subscribe_async::<Msg>(8, Default::default())
    ///     .done()
    ///     .build();
    /// ```
    #[inline]
    pub fn build() -> BusBuilder {
        BusBuilder::new()
    }

    /// Returns `true` if the bus is in the process of closing.
    ///
    /// Once closing begins, new messages will be rejected with a `Closed` error.
    pub fn is_closing(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn init(&self) {
        for r in self.inner.receivers.iter() {
            r.init(self).unwrap();
        }
    }

    /// Waits for all receivers to be ready to accept messages.
    ///
    /// This is typically called automatically during bus initialization.
    pub async fn ready(&self) {
        for r in self.inner.receivers.iter() {
            r.ready().await;
        }
    }

    /// Closes the bus, stopping all receivers.
    ///
    /// After calling close:
    /// - No new messages will be accepted
    /// - Existing messages in queues will be processed
    /// - All receivers will be shut down
    ///
    /// You should await the poller future after closing to ensure clean shutdown.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use messagebus::{Bus, AsyncHandler, error};
    /// use messagebus::derive::Message;
    /// use async_trait::async_trait;
    ///
    /// #[derive(Debug, Clone, Message)]
    /// struct Msg;
    /// struct Handler;
    /// #[async_trait]
    /// impl AsyncHandler<Msg> for Handler {
    ///     type Error = error::GenericError;
    ///     type Response = ();
    ///     async fn handle(&self, _: Msg, _: &Bus) -> Result<(), Self::Error> { Ok(()) }
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let (bus, poller) = Bus::build()
    ///         .register(Handler)
    ///         .subscribe_async::<Msg>(8, Default::default())
    ///         .done()
    ///         .build();
    ///     let handle = tokio::spawn(poller);
    ///
    ///     bus.flush_all().await;  // Ensure all messages are processed
    ///     bus.close().await;       // Shut down the bus
    ///     handle.await.unwrap();   // Wait for complete shutdown
    /// }
    /// ```
    pub async fn close(&self) {
        let _handle = self.inner.maintain.lock().await;
        self.inner.closed.store(true, Ordering::SeqCst);

        for r in self.inner.receivers.iter() {
            let err = tokio::time::timeout(Duration::from_secs(20), r.close(self)).await;

            if let Err(err) = err {
                error!("Close timeout on {}: {}", r.name(), err);
            }
        }
    }

    /// Flushes all pending messages across all receivers.
    ///
    /// This method ensures that all messages currently in receiver queues are processed.
    /// It repeatedly flushes until no more messages need flushing, with a maximum of
    /// 32 iterations to prevent infinite loops.
    ///
    /// Call this before [`close()`](Bus::close) to ensure all messages are processed.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use messagebus::{Bus, AsyncHandler, error};
    /// use messagebus::derive::Message;
    /// use async_trait::async_trait;
    ///
    /// #[derive(Debug, Clone, Message)]
    /// #[message(clone)]
    /// struct Msg(i32);
    /// struct Handler;
    /// #[async_trait]
    /// impl AsyncHandler<Msg> for Handler {
    ///     type Error = error::GenericError;
    ///     type Response = ();
    ///     async fn handle(&self, _: Msg, _: &Bus) -> Result<(), Self::Error> { Ok(()) }
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let (bus, poller) = Bus::build()
    ///         .register(Handler)
    ///         .subscribe_async::<Msg>(8, Default::default())
    ///         .done()
    ///         .build();
    ///     tokio::spawn(poller);
    ///     bus.ready().await;
    ///
    ///     // Send some messages
    ///     bus.send(Msg(1)).await?;
    ///     bus.send(Msg(2)).await?;
    ///
    ///     // Wait for all messages to be processed
    ///     bus.flush_all().await;
    ///     bus.close().await;
    ///     Ok(())
    /// }
    /// ```
    pub async fn flush_all(&self) {
        let fuse_count = 32i32;
        let mut breaked = false;
        let mut iters = 0usize;

        for _ in 0..fuse_count {
            iters += 1;
            let mut flushed = false;
            for r in self.inner.receivers.iter() {
                if r.need_flush() {
                    flushed = true;

                    r.flush(self).await;
                }
            }

            if !flushed {
                breaked = true;
                break;
            }
        }

        if !breaked {
            warn!(
                "!!! WARNING: unable to reach equilibrium in {} iterations !!!",
                fuse_count
            );
        } else {
            info!("flushed in {} iterations !!!", iters);
        }
    }

    /// Flushes pending messages for a specific message type.
    ///
    /// Only receivers handling message type `M` will be flushed.
    pub async fn flush<M: Message>(&self) {
        let fuse_count = 32i32;
        let mut breaked = false;
        let mut iters = 0usize;

        for _ in 0..fuse_count {
            let receivers =
                self.select_receivers(M::type_tag_(), Default::default(), None, None, false);
            iters += 1;
            let mut flushed = false;
            for r in receivers {
                if r.need_flush() {
                    flushed = true;

                    r.flush(self).await;
                }
            }

            if !flushed {
                breaked = true;
                break;
            }
        }

        if !breaked {
            warn!(
                "!!! WARNING: unable to reach equilibrium in {} iterations !!!",
                fuse_count
            );
        } else {
            info!("flushed in {} iterations !!!", iters);
        }
    }

    pub async fn flush2<M1: Message, M2: Message>(&self) {
        let fuse_count = 32i32;
        let mut breaked = false;
        let mut iters = 0usize;
        for _ in 0..fuse_count {
            let receivers1 =
                self.select_receivers(M1::type_tag_(), Default::default(), None, None, false);

            let receivers2 =
                self.select_receivers(M2::type_tag_(), Default::default(), None, None, false);

            iters += 1;
            let mut flushed = false;
            for r in receivers1.chain(receivers2) {
                if r.need_flush() {
                    flushed = true;

                    r.flush(self).await;
                }
            }

            if !flushed {
                breaked = true;
                break;
            }
        }

        if !breaked {
            warn!(
                "!!! WARNING: unable to reach equilibrium in {} iterations !!!",
                fuse_count
            );
        } else {
            info!("flushed in {} iterations !!!", iters);
        }
    }

    pub async fn sync_all(&self) {
        for r in self.inner.receivers.iter() {
            r.sync(self).await;
        }
    }

    pub async fn sync<M: Message>(&self) {
        let receivers =
            self.select_receivers(M::type_tag_(), Default::default(), None, None, false);

        for r in receivers {
            r.sync(self).await;
        }
    }

    pub async fn sync2<M1: Message, M2: Message>(&self) {
        let receivers1 =
            self.select_receivers(M1::type_tag_(), Default::default(), None, None, false);

        let receivers2 =
            self.select_receivers(M2::type_tag_(), Default::default(), None, None, false);

        for r in receivers1.chain(receivers2) {
            r.sync(self).await;
        }
    }

    pub async fn idle_all(&self) {
        for r in self.inner.receivers.iter() {
            r.flush(self).await;
            r.idle().await;
        }
    }

    pub async fn idle<M: Message>(&self) {
        let receivers =
            self.select_receivers(M::type_tag_(), Default::default(), None, None, false);

        for r in receivers {
            r.flush(self).await;
            r.idle().await;
        }
    }

    pub async fn idle2<M1: Message, M2: Message>(&self) {
        let receivers1 =
            self.select_receivers(M1::type_tag_(), Default::default(), None, None, false);

        let receivers2 =
            self.select_receivers(M2::type_tag_(), Default::default(), None, None, false);

        for r in receivers1.chain(receivers2) {
            r.flush(self).await;
            r.idle().await;
        }
    }

    #[inline]
    pub async fn flush_and_sync_all(&self, force: bool) {
        if !force {
            self.idle_all().await;
        }

        log::info!("flushing all begin");
        self.flush_all().await;
        self.sync_all().await;
    }

    #[inline]
    pub async fn flush_and_sync<M: Message>(&self, force: bool) {
        if !force {
            self.idle::<M>().await;
        }

        log::info!("flushing 1 begin");
        self.flush::<M>().await;
        self.sync::<M>().await;
    }

    #[inline]
    pub async fn flush_and_sync2<M1: Message, M2: Message>(&self, force: bool) {
        if !force {
            self.idle2::<M1, M2>().await;
        }
        log::info!("flushing 2 begin");
        self.flush2::<M1, M2>().await;
        self.sync2::<M1, M2>().await;
    }

    fn try_reserve(&self, tt: &TypeTag, rs: &[Receiver]) -> Option<SmallVec<[Permit; 32]>> {
        let mut permits = SmallVec::<[Permit; 32]>::new();

        for r in rs {
            if let Some(prmt) = r.try_reserve(tt) {
                permits.push(prmt);
            } else {
                return None;
            };
        }

        Some(permits)
    }

    /// Attempts to send a message without blocking.
    ///
    /// Returns immediately with an error if the receiver's buffer is full.
    /// The message is broadcast to all receivers handling type `M`.
    ///
    /// # Errors
    ///
    /// - [`SendError::Full`] - Buffer is full, message not sent
    /// - [`SendError::Closed`] - Bus is closed
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use messagebus::{Bus, AsyncHandler, error};
    /// use messagebus::derive::Message;
    /// use async_trait::async_trait;
    ///
    /// #[derive(Debug, Clone, Message)]
    /// #[message(clone)]
    /// struct MyMessage(String);
    /// struct Handler;
    /// #[async_trait]
    /// impl AsyncHandler<MyMessage> for Handler {
    ///     type Error = error::GenericError;
    ///     type Response = ();
    ///     async fn handle(&self, _: MyMessage, _: &Bus) -> Result<(), Self::Error> { Ok(()) }
    /// }
    ///
    /// let (bus, _poller) = Bus::build()
    ///     .register(Handler)
    ///     .subscribe_async::<MyMessage>(8, Default::default())
    ///     .done()
    ///     .build();
    ///
    /// match bus.try_send(MyMessage("test".into())) {
    ///     Ok(()) => println!("Sent!"),
    ///     Err(e) => println!("Failed: {:?}", e),
    /// }
    /// ```
    #[inline]
    pub fn try_send<M: Message + Clone>(&self, msg: M) -> Result<(), Error<M>> {
        self.try_send_ext(msg, SendOptions::Broadcast)
    }

    /// Attempts to send a message with routing options without blocking.
    ///
    /// Like [`try_send()`](Bus::try_send) but with explicit [`SendOptions`].
    pub fn try_send_ext<M: Message + Clone>(
        &self,
        msg: M,
        _options: SendOptions,
    ) -> core::result::Result<(), Error<M>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        if let Some(rs) = self.inner.lookup.get(&(msg.type_tag(), None, None)) {
            let permits = if let Some(x) = self.try_reserve(&tt, rs) {
                x
            } else {
                return Err(SendError::Full(msg).into());
            };

            let mut iter = permits.into_iter().zip(rs.iter());
            let mut counter = 1;
            let total = rs.len();

            while counter < total {
                let (p, r) = iter.next().unwrap();
                let _ = r.send(self, mid, msg.clone(), false, p);

                counter += 1;
            }

            if let Some((p, r)) = iter.next() {
                let _ = r.send(self, mid, msg, false, p);
                return Ok(());
            }
        }

        warn!(
            "Unhandled message {:?}: no receivers",
            core::any::type_name::<M>()
        );

        Ok(())
    }

    /// Sends a message synchronously, blocking the current thread.
    ///
    /// This is useful when you need to send from a non-async context.
    /// The message is broadcast to all receivers handling type `M`.
    ///
    /// # Warning
    ///
    /// This blocks the current thread. Avoid using in async contexts.
    #[inline]
    pub fn send_blocking<M: Message + Clone>(&self, msg: M) -> Result<(), Error<M>> {
        self.send_blocking_ext(msg, SendOptions::Broadcast)
    }

    /// Sends a message synchronously with routing options.
    ///
    /// Like [`send_blocking()`](Bus::send_blocking) but with explicit [`SendOptions`].
    #[inline]
    pub fn send_blocking_ext<M: Message + Clone>(
        &self,
        msg: M,
        options: SendOptions,
    ) -> core::result::Result<(), Error<M>> {
        futures::executor::block_on(self.send_ext(msg, options))
    }

    /// Sends a message asynchronously to all receivers.
    ///
    /// This is the primary method for sending messages. It broadcasts the message
    /// to all receivers that handle message type `M`. The method waits for buffer
    /// space if needed.
    ///
    /// # Errors
    ///
    /// - [`SendError::Closed`] - Bus is closed
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use messagebus::{Bus, AsyncHandler, error};
    /// use messagebus::derive::Message;
    /// use async_trait::async_trait;
    ///
    /// #[derive(Debug, Clone, Message)]
    /// #[message(clone)]
    /// struct MyMessage(String);
    /// struct Handler;
    /// #[async_trait]
    /// impl AsyncHandler<MyMessage> for Handler {
    ///     type Error = error::GenericError;
    ///     type Response = ();
    ///     async fn handle(&self, _: MyMessage, _: &Bus) -> Result<(), Self::Error> { Ok(()) }
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let (bus, poller) = Bus::build()
    ///         .register(Handler)
    ///         .subscribe_async::<MyMessage>(8, Default::default())
    ///         .done()
    ///         .build();
    ///     tokio::spawn(poller);
    ///     bus.ready().await;
    ///
    ///     bus.send(MyMessage("hello".into())).await?;
    ///     bus.close().await;
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn send<M: Message + Clone>(&self, msg: M) -> core::result::Result<(), Error<M>> {
        self.send_ext(msg, SendOptions::Broadcast).await
    }

    /// Sends a message asynchronously with routing options.
    ///
    /// Like [`send()`](Bus::send) but with explicit [`SendOptions`].
    pub async fn send_ext<M: Message + Clone>(
        &self,
        msg: M,
        _options: SendOptions,
    ) -> core::result::Result<(), Error<M>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        if let Some(rs) = self.inner.lookup.get(&(msg.type_tag(), None, None)) {
            if let Some((last, head)) = rs.split_last() {
                for r in head {
                    let _ = r.send(self, mid, msg.clone(), false, r.reserve(&tt).await);
                }

                let _ = last.send(self, mid, msg, false, last.reserve(&tt).await);

                return Ok(());
            }
        }

        warn!(
            "Unhandled message {:?}: no receivers",
            core::any::type_name::<M>()
        );

        Ok(())
    }

    /// Forces a message to be sent, bypassing backpressure.
    ///
    /// Unlike [`try_send()`](Bus::try_send), this will send the message even if
    /// buffers are full. Use with caution as it can cause memory growth.
    ///
    /// # Errors
    ///
    /// - [`SendError::Closed`] - Bus is closed
    #[inline]
    pub fn force_send<M: Message + Clone>(&self, msg: M) -> Result<(), Error<M>> {
        self.force_send_ext(msg, SendOptions::Broadcast)
    }

    /// Forces a message to be sent with routing options, bypassing backpressure.
    ///
    /// Like [`force_send()`](Bus::force_send) but with explicit [`SendOptions`].
    pub fn force_send_ext<M: Message + Clone>(
        &self,
        msg: M,
        _options: SendOptions,
    ) -> core::result::Result<(), Error<M>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg).into());
        }

        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        if let Some(rs) = self.inner.lookup.get(&(msg.type_tag(), None, None)) {
            if let Some((last, head)) = rs.split_last() {
                for r in head {
                    let _ = r.force_send(self, mid, msg.clone(), false);
                }

                let _ = last.force_send(self, mid, msg, false);

                return Ok(());
            }
        }

        warn!(
            "Unhandled message {:?}: no receivers",
            core::any::type_name::<M>()
        );

        Ok(())
    }

    /// Attempts to send a message to a single receiver without blocking.
    ///
    /// Unlike [`try_send()`](Bus::try_send) which broadcasts, this sends to only
    /// one receiver. Returns an error if the buffer is full.
    ///
    /// # Errors
    ///
    /// - [`Error::NoReceivers`] - No receiver for this message type
    /// - [`SendError::Full`] - Buffer is full
    /// - [`SendError::Closed`] - Bus is closed
    #[inline]
    pub fn try_send_one<M: Message>(&self, msg: M) -> Result<(), Error<M>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        if let Some(rs) = self
            .inner
            .lookup
            .get(&(msg.type_tag(), None, None))
            .and_then(|rs| rs.first())
        {
            let permits = if let Some(x) = rs.try_reserve(&tt) {
                x
            } else {
                return Err(SendError::Full(msg).into());
            };

            Ok(rs.send(self, mid, msg, false, permits)?)
        } else {
            Err(Error::NoReceivers)
        }
    }

    /// Sends a message to a single receiver asynchronously.
    ///
    /// Unlike [`send()`](Bus::send) which broadcasts, this sends to only one receiver.
    /// Waits for buffer space if needed.
    ///
    /// # Errors
    ///
    /// - [`Error::NoReceivers`] - No receiver for this message type
    /// - [`SendError::Closed`] - Bus is closed
    pub async fn send_one<M: Message>(&self, msg: M) -> Result<(), Error<M>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        if let Some(rs) = self
            .inner
            .lookup
            .get(&(msg.type_tag(), None, None))
            .and_then(|rs| rs.first())
        {
            Ok(rs.send(self, mid, msg, false, rs.reserve(&tt).await)?)
        } else {
            Err(Error::NoReceivers)
        }
    }

    /// Sends a message to a single receiver synchronously, blocking the current thread.
    #[inline]
    pub fn send_one_blocking<M: Message>(&self, msg: M) -> Result<(), Error<M>> {
        futures::executor::block_on(self.send_one(msg))
    }

    /// Sends a request message and waits for a typed response.
    ///
    /// This implements the request/response pattern. The message is sent to a receiver,
    /// and the method waits for that receiver to return a response of type `R`.
    ///
    /// # Type Parameters
    ///
    /// - `M` - The request message type
    /// - `R` - The expected response type
    ///
    /// # Errors
    ///
    /// - [`Error::NoReceivers`] - No receiver for this message type
    /// - [`Error::NoResponse`] - Receiver did not respond
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use messagebus::{Bus, AsyncHandler, error};
    /// use messagebus::derive::Message;
    /// use async_trait::async_trait;
    ///
    /// #[derive(Debug, Clone, Message)]
    /// struct GetUser(u64);
    ///
    /// #[derive(Debug, Clone, Message)]
    /// struct User { name: String }
    ///
    /// struct UserHandler;
    /// #[async_trait]
    /// impl AsyncHandler<GetUser> for UserHandler {
    ///     type Error = error::GenericError;
    ///     type Response = User;
    ///     async fn handle(&self, msg: GetUser, _: &Bus) -> Result<User, Self::Error> {
    ///         Ok(User { name: format!("User{}", msg.0) })
    ///     }
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let (bus, poller) = Bus::build()
    ///         .register(UserHandler)
    ///         .subscribe_async::<GetUser>(8, Default::default())
    ///         .done()
    ///         .build();
    ///     tokio::spawn(poller);
    ///     bus.ready().await;
    ///
    ///     let user: User = bus.request(GetUser(123), Default::default()).await?;
    ///     println!("Got user: {}", user.name);
    ///     bus.close().await;
    ///     Ok(())
    /// }
    /// ```
    pub async fn request<M: Message, R: Message>(
        &self,
        req: M,
        options: SendOptions,
    ) -> Result<R, Error<M>> {
        let tid = M::type_tag_();
        let rid = R::type_tag_();

        let mut iter = self.select_receivers(tid.clone(), options, Some(rid), None, true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc
                .add_response_waiter::<R>()
                .map_err(|x| x.specify::<M>())?;

            let mid = mid | 1 << (u64::BITS - 1);

            rc.send(self, mid, req, true, rc.reserve(&tid).await)?;
            rx.await.map_err(|x| x.specify::<M>())
        } else {
            Err(Error::NoReceivers)
        }
    }

    /// Sends a request message and waits for a typed response with explicit error type.
    ///
    /// Like [`request()`](Bus::request) but allows specifying a custom error type `E`
    /// that the handler may return.
    ///
    /// # Type Parameters
    ///
    /// - `M` - The request message type
    /// - `R` - The expected response type
    /// - `E` - The error type the handler may return
    pub async fn request_we<M, R, E>(&self, req: M, options: SendOptions) -> Result<R, Error<M, E>>
    where
        M: Message,
        R: Message,
        E: StdSyncSendError,
    {
        let tid = M::type_tag_();
        let rid = R::type_tag_();
        let eid = E::type_tag_();

        let mut iter = self.select_receivers(tid.clone(), options, Some(rid), Some(eid), true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc.add_response_waiter_we::<R, E>().map_err(|x| {
                x.map_err(|_| unimplemented!())
                    .map_msg(|_| unimplemented!())
            })?;

            rc.send(
                self,
                mid | 1 << (u64::BITS - 1),
                req,
                true,
                rc.reserve(&tid).await,
            )
            .map_err(|x| x.map_err(|_| unimplemented!()))?;

            rx.await.map_err(|x| x.specify::<M>())
        } else {
            Err(Error::NoReceivers)
        }
    }

    pub async fn send_boxed(
        &self,
        msg: Box<dyn Message>,
        options: SendOptions,
    ) -> Result<(), Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        let mut iter = self.select_receivers(tt.clone(), options, None, None, false);
        let first = iter.next();

        for r in iter {
            let _ = r.send_boxed(
                self,
                mid,
                msg.try_clone_boxed().unwrap(),
                false,
                r.reserve(&tt).await,
            );
        }

        if let Some(r) = first {
            let _ = r.send_boxed(
                self,
                mid,
                msg.try_clone_boxed().unwrap(),
                false,
                r.reserve(&tt).await,
            );
        } else {
            warn!("Unhandled message: no receivers");
        }

        Ok(())
    }

    pub async fn send_boxed_one(
        &self,
        msg: Box<dyn Message>,
        options: SendOptions,
    ) -> Result<(), Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        let mut iter = self.select_receivers(tt.clone(), options, None, None, false);
        if let Some(rs) = iter.next() {
            Ok(rs.send_boxed(self, mid, msg, false, rs.reserve(&tt).await)?)
        } else {
            Err(Error::NoReceivers)
        }
    }

    pub async fn request_boxed(
        &self,
        req: Box<dyn Message>,
        options: SendOptions,
    ) -> Result<Box<dyn Message>, Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(req).into());
        }

        let tt = req.type_tag();

        let mut iter = self.select_receivers(tt.clone(), options, None, None, true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc.add_response_waiter_boxed().map_err(|x| {
                x.map_err(|_| unimplemented!())
                    .map_msg(|_| unimplemented!())
            })?;

            rc.send_boxed(
                self,
                mid | 1 << (usize::BITS - 1),
                req,
                true,
                rc.reserve(&tt).await,
            )?;

            rx.await.map_err(|x| x.specify::<Box<dyn Message>>())
        } else {
            Err(Error::NoReceivers)
        }
    }

    pub async fn request_boxed_we<E: StdSyncSendError>(
        &self,
        req: Box<dyn Message>,
        options: SendOptions,
    ) -> Result<Box<dyn Message>, Error<Box<dyn Message>, E>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(req).into());
        }

        let tt = req.type_tag();
        let eid = E::type_tag_();

        let mut iter = self.select_receivers(tt.clone(), options, None, Some(eid), true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc.add_response_waiter_boxed_we().map_err(|x| {
                x.map_err(|_| unimplemented!())
                    .map_msg(|_| unimplemented!())
            })?;

            rc.send_boxed(
                self,
                mid | 1 << (usize::BITS - 1),
                req,
                true,
                rc.reserve(&tt).await,
            )
            .map_err(|x| x.map_err(|_| unimplemented!()))?;

            rx.await.map_err(|x| x.specify::<Box<dyn Message>>())
        } else {
            Err(Error::NoReceivers)
        }
    }

    pub async fn send_deserialize_one<'a, 'b: 'a, 'c: 'a>(
        &'a self,
        tt: TypeTag,
        de: &'b mut dyn erased_serde::Deserializer<'c>,
        _options: SendOptions,
    ) -> Result<(), Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            warn!("closed message bus");
            return Err(Error::NoResponse);
        }

        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        if let Some(rs) = self
            .inner
            .lookup
            .get(&(tt.clone(), None, None))
            .and_then(|rs| rs.first())
        {
            let msg = deserialize_shared_message(tt.clone(), de)?;

            Ok(rs.send_boxed(self, mid, msg.upcast_box(), false, rs.reserve(&tt).await)?)
        } else {
            Err(Error::NoReceivers)
        }
    }

    pub async fn request_deserialize<'a, 'b: 'a, 'c: 'a>(
        &'a self,
        tt: TypeTag,
        de: &'b mut dyn erased_serde::Deserializer<'c>,
        options: SendOptions,
    ) -> Result<Box<dyn Message>, Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            warn!("closed message bus");
            return Err(Error::NoResponse);
        }

        let mut iter = self.select_receivers(tt.clone(), options, None, None, true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc.add_response_waiter_boxed().unwrap();
            let msg = deserialize_shared_message(tt.clone(), de)?;

            rc.send_boxed(
                self,
                mid | 1 << (usize::BITS - 1),
                msg.upcast_box(),
                true,
                rc.reserve(&tt).await,
            )?;

            rx.await.map_err(|x| x.specify::<Box<dyn Message>>())
        } else {
            Err(Error::NoReceivers)
        }
    }

    /// Returns statistics for all receivers in the bus.
    ///
    /// Useful for monitoring and debugging message throughput.
    pub fn stats(&self) -> impl Iterator<Item = Stats> + '_ {
        self.inner.receivers.iter().map(|x| x.stats())
    }

    #[inline]
    fn select_receivers(
        &self,
        tid: TypeTag,
        options: SendOptions,
        rid: Option<TypeTag>,
        eid: Option<TypeTag>,
        is_req: bool,
    ) -> impl Iterator<Item = &Receiver> + '_ {
        self.inner
            .lookup
            .get(&(tid.clone(), rid.clone(), eid.clone()))
            .into_iter()
            .flatten()
            .filter(move |r| r.accept(is_req, &tid, rid.as_ref(), eid.as_ref()))
            .filter(move |r| match options {
                SendOptions::Except(id) => id != r.id(),
                SendOptions::Direct(id) => id == r.id(),
                _ => true,
            })
    }
}
