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
//! - **Task grouping**: assign group IDs to messages and wait for group completion
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
//! ### Thread-Safe Handlers (Send+Sync)
//!
//! | Handler | Batched | Async | Use Case |
//! |---------|---------|-------|----------|
//! | [`Handler`] | No | No | Simple sync processing |
//! | [`AsyncHandler`] | No | Yes | Async I/O operations |
//! | [`BatchHandler`] | Yes | No | Bulk sync processing |
//! | [`AsyncBatchHandler`] | Yes | Yes | Bulk async processing |
//!
//! ### Synchronized Handlers (Send only)
//!
//! | Handler | Batched | Async | Use Case |
//! |---------|---------|-------|----------|
//! | [`SynchronizedHandler`] | No | No | Mutable state (sync) |
//! | [`AsyncSynchronizedHandler`] | No | Yes | Mutable state (async) |
//! | [`BatchSynchronizedHandler`] | Yes | No | Batched mutable state |
//! | [`AsyncBatchSynchronizedHandler`] | Yes | Yes | Batched async mutable state |
//!
//! ### Local Handlers (no Send/Sync)
//!
//! | Handler | Batched | Async | Use Case |
//! |---------|---------|-------|----------|
//! | [`LocalHandler`] | No | No | Thread-local sync |
//! | [`LocalAsyncHandler`] | No | Yes | Thread-local async |
//! | [`LocalBatchHandler`] | Yes | No | Thread-local batched sync |
//! | [`LocalAsyncBatchHandler`] | Yes | Yes | Thread-local batched async |
//!
//! ## Message Derive Macro
//!
//! Use `#[derive(Message)]` with optional attributes:
//!
//! - `#[message(clone)]` - Enable message cloning for broadcast
//! - `#[message(shared)]` - Enable serialization for remote transport
//! - `#[type_tag("custom::name")]` - Custom type identifier
//! - `#[namespace("my_namespace")]` - Type tag namespace prefix
//! - `#[group_id(expr)]` - Associate messages with a group for tracking
//!
//! ## Task Grouping
//!
//! Task grouping allows you to track related messages and wait for all tasks in a group
//! to complete. This is useful for job processing, batch operations, or any scenario
//! where you need to know when a set of related tasks has finished.
//!
//! ### Defining Grouped Messages
//!
//! Use the `#[group_id(expr)]` attribute to specify which field contains the group ID:
//!
//! ```rust,no_run
//! use messagebus::derive::Message;
//!
//! #[derive(Debug, Clone, Message)]
//! #[group_id(self.job_id)]
//! struct ProcessJob {
//!     job_id: i64,
//!     task_name: String,
//! }
//! ```
//!
//! ### Waiting for Group Completion
//!
//! ```rust,no_run
//! use messagebus::{Bus, GroupId};
//!
//! async fn process_job(bus: &Bus) {
//!     let job_id: GroupId = 1001;
//!
//!     // Send multiple messages with the same group_id
//!     // bus.send(ProcessJob { job_id, task_name: "Task A".into() }).await.unwrap();
//!     // bus.send(ProcessJob { job_id, task_name: "Task B".into() }).await.unwrap();
//!
//!     // Wait for all tasks in the group to complete
//!     bus.flush_group(job_id).await;
//!
//!     // Check if group is idle
//!     assert!(bus.is_group_idle(job_id));
//! }
//! ```
//!
//! ### Group Propagation
//!
//! When a handler sends new messages, they automatically inherit the parent task's
//! group ID (unless the child message has its own `#[group_id]` attribute).
//!
//! ### Group Management
//!
//! - [`Bus::flush_group`] - Wait for all tasks in a group to complete
//! - [`Bus::is_group_idle`] - Check if a group has no in-flight tasks
//! - [`Bus::current_group_id`] - Get the current task's group ID from within a handler
//! - [`Bus::remove_group`] - Remove an idle group from the registry
//! - [`Bus::tracked_group_count`] - Get the number of tracked groups

mod builder;
mod envelop;
pub mod error;
pub mod group;
pub mod handler;
mod receiver;
pub mod receivers;
mod relay;
mod stats;
mod trait_object;
pub mod type_tag;

#[doc(hidden)]
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
pub use envelop::{IntoBoxedMessage, Message, MessageBounds, SharedMessage, TypeTag, TypeTagged};
pub use group::{GroupId, GroupRemovalResult};
pub use handler::*;
pub use receiver::{
    Action, Event, EventBoxed, ReciveTypedReceiver, ReciveUntypedReceiver, SendTypedReceiver,
    SendUntypedReceiver, TypeTagAccept, TypeTagAcceptItem,
};
pub use relay::Relay;
pub use stats::Stats;
pub use type_tag::{deserialize_shared_message, register_shared_message};

use group::GroupRegistry;
pub type Untyped = Arc<dyn Any + Send + Sync>;

tokio::task_local! {
    /// Task-local storage for the current group ID.
    /// Used for propagating group context to child messages.
    static CURRENT_GROUP: Option<GroupId>;
}

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
    /// Registry for tracking task groups.
    group_registry: Arc<GroupRegistry>,
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
            group_registry: Arc::new(GroupRegistry::new()),
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
        self.inner.closed.load(Ordering::Acquire)
    }

    pub(crate) fn init(&self) {
        for r in self.inner.receivers.iter() {
            r.init(self).expect("failed to initialize receiver");
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
        self.inner.closed.store(true, Ordering::Release);

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
    /// # Important: Avoiding Deadlocks
    ///
    /// Do not call this method from within a message handler. Flushing waits for
    /// in-flight handlers to complete, so calling flush from inside a handler creates
    /// a circular dependency that will deadlock.
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
                // Use !is_idling() instead of need_flush() to avoid race condition
                // where need_flush flag could be cleared while messages are still
                // being processed. is_idling() checks the processing counter directly.
                if !r.is_idling() {
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
    ///
    /// # Important: Avoiding Deadlocks
    ///
    /// Do not call this method from within a handler that processes the same message
    /// type `M`. Flushing waits for in-flight handlers to complete, so calling
    /// `flush::<M>()` from inside a handler for `M` creates a circular dependency
    /// that will deadlock. Flushing a *different* message type is safe.
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
                // Use !is_idling() instead of need_flush() to avoid race condition
                if !r.is_idling() {
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

    /// Flushes pending messages for two message types.
    ///
    /// Convenience method that flushes receivers handling either `M1` or `M2`.
    /// See [`flush()`](Bus::flush) for details on flushing behavior.
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
                // Use !is_idling() instead of need_flush() to avoid race condition
                if !r.is_idling() {
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

    /// Calls the `sync` method on all receivers.
    ///
    /// The `sync` method allows handlers to perform cleanup or persistence operations
    /// after processing messages. This is useful for batched handlers that need to
    /// flush internal buffers.
    pub async fn sync_all(&self) {
        for r in self.inner.receivers.iter() {
            r.sync(self).await;
        }
    }

    /// Calls the `sync` method on receivers handling a specific message type.
    ///
    /// Only receivers that handle message type `M` will have their `sync` called.
    pub async fn sync<M: Message>(&self) {
        let receivers =
            self.select_receivers(M::type_tag_(), Default::default(), None, None, false);

        for r in receivers {
            r.sync(self).await;
        }
    }

    /// Calls the `sync` method on receivers handling two message types.
    ///
    /// Convenience method that syncs receivers handling either `M1` or `M2`.
    pub async fn sync2<M1: Message, M2: Message>(&self) {
        let receivers1 =
            self.select_receivers(M1::type_tag_(), Default::default(), None, None, false);

        let receivers2 =
            self.select_receivers(M2::type_tag_(), Default::default(), None, None, false);

        for r in receivers1.chain(receivers2) {
            r.sync(self).await;
        }
    }

    /// Waits for all receivers to become idle.
    ///
    /// A receiver is idle when it has no messages being processed and no pending
    /// messages in its queue. This flushes each receiver and then waits for it
    /// to complete all work.
    pub async fn idle_all(&self) {
        for r in self.inner.receivers.iter() {
            r.flush(self).await;
            r.idle().await;
        }
    }

    /// Waits for receivers handling a specific message type to become idle.
    ///
    /// Only receivers that handle message type `M` will be waited on.
    pub async fn idle<M: Message>(&self) {
        let receivers =
            self.select_receivers(M::type_tag_(), Default::default(), None, None, false);

        for r in receivers {
            r.flush(self).await;
            r.idle().await;
        }
    }

    /// Waits for receivers handling two message types to become idle.
    ///
    /// Convenience method that waits for receivers handling either `M1` or `M2`.
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

    /// Flushes all receivers and then calls sync on each.
    ///
    /// This combines [`flush_all()`](Bus::flush_all) and [`sync_all()`](Bus::sync_all)
    /// for convenience.
    ///
    /// # Arguments
    ///
    /// * `force` - If `false`, waits for all receivers to be idle before flushing.
    ///   If `true`, proceeds immediately to flush and sync.
    #[inline]
    pub async fn flush_and_sync_all(&self, force: bool) {
        if !force {
            self.idle_all().await;
        }

        log::info!("flushing all begin");
        self.flush_all().await;
        self.sync_all().await;
    }

    /// Flushes and syncs receivers handling a specific message type.
    ///
    /// This combines [`flush()`](Bus::flush) and [`sync()`](Bus::sync) for a single
    /// message type.
    ///
    /// # Arguments
    ///
    /// * `force` - If `false`, waits for receivers to be idle before flushing.
    ///   If `true`, proceeds immediately.
    #[inline]
    pub async fn flush_and_sync<M: Message>(&self, force: bool) {
        if !force {
            self.idle::<M>().await;
        }

        log::info!("flushing 1 begin");
        self.flush::<M>().await;
        self.sync::<M>().await;
    }

    /// Flushes and syncs receivers handling two message types.
    ///
    /// Convenience method combining flush and sync for types `M1` and `M2`.
    ///
    /// # Arguments
    ///
    /// * `force` - If `false`, waits for receivers to be idle before flushing.
    ///   If `true`, proceeds immediately.
    #[inline]
    pub async fn flush_and_sync2<M1: Message, M2: Message>(&self, force: bool) {
        if !force {
            self.idle2::<M1, M2>().await;
        }
        log::info!("flushing 2 begin");
        self.flush2::<M1, M2>().await;
        self.sync2::<M1, M2>().await;
    }

    // ==================== Group-based Operations ====================

    /// Waits for all tasks belonging to a specific group to complete.
    ///
    /// This flushes any partial batches for the group's receivers before waiting,
    /// ensuring that buffered messages are processed even if the batch isn't full.
    ///
    /// For complex scenarios with cascading messages (handlers that send other messages),
    /// consider using [`flush_and_sync_group`](Self::flush_and_sync_group) which handles
    /// those cases more robustly.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use messagebus::{Bus, GroupId};
    ///
    /// async fn process_job(bus: &Bus, job_id: GroupId) {
    ///     // Send multiple messages with the same group_id...
    ///
    ///     // Wait for all tasks in this group to complete
    ///     bus.flush_group(job_id).await;
    /// }
    /// ```
    pub async fn flush_group(&self, group_id: GroupId) {
        // Flush receivers that have messages from this group to handle partial batches
        let receiver_ids = self.inner.group_registry.receivers_for_group(group_id);
        for r in self.inner.receivers.iter() {
            if receiver_ids.contains(&r.id()) {
                r.flush(self).await;
            }
        }

        self.inner.group_registry.wait_idle(group_id).await;
    }

    /// Syncs only the receivers that have processed messages for a specific group.
    ///
    /// Unlike `sync_all()` which syncs every receiver, this method only syncs
    /// receivers that have handled messages belonging to the specified group.
    /// This prevents blocking between independent groups.
    ///
    /// # Arguments
    ///
    /// * `group_id` - The group ID whose receivers should be synced
    pub async fn sync_group(&self, group_id: GroupId) {
        let receiver_ids = self.inner.group_registry.receivers_for_group(group_id);
        for r in self.inner.receivers.iter() {
            if receiver_ids.contains(&r.id()) {
                r.sync(self).await;
            }
        }
    }

    /// Flushes and syncs all tasks belonging to a specific group.
    ///
    /// This combines waiting for group completion with targeted receiver sync operations.
    /// Only receivers that processed messages from this group will be synced,
    /// preventing blocking between independent groups.
    ///
    /// # Arguments
    ///
    /// * `group_id` - The group ID to flush and sync
    /// * `force` - If true, skips waiting for idle and proceeds directly to sync
    pub async fn flush_and_sync_group(&self, group_id: GroupId, force: bool) {
        log::debug!(
            "flush_and_sync_group: START group={} force={} processing_count={}",
            group_id,
            force,
            self.inner.group_registry.processing_count(group_id)
        );

        if !force {
            // Two-pass flush to handle backpressure correctly:
            // 1. First pass: non-blocking flush to trigger all receivers to start processing
            //    This prevents deadlock where upstream handlers are blocked waiting for
            //    downstream buffers to have space.
            // 2. Second pass: blocking flush to wait for processing to complete
            let fuse_count = 32i32;
            let mut iters = 0;

            loop {
                iters += 1;
                if iters > fuse_count {
                    log::warn!(
                        "flush_and_sync_group: group {} did not stabilize after {} iterations",
                        group_id,
                        fuse_count
                    );
                    break;
                }

                let receiver_ids = self.inner.group_registry.receivers_for_group(group_id);
                if receiver_ids.is_empty() {
                    log::debug!(
                        "flush_and_sync_group: group={} no receivers, done",
                        group_id
                    );
                    break;
                }

                // First pass: trigger all receivers to start processing (non-blocking)
                for r in self.inner.receivers.iter() {
                    if receiver_ids.contains(&r.id()) && !r.is_idling() {
                        log::debug!(
                            "flush_and_sync_group: group={} triggering flush on receiver id={}",
                            group_id,
                            r.id()
                        );
                        r.flush_nowait(self);
                    }
                }

                // Second pass: wait for processing to complete (blocking)
                let mut any_flushed = false;
                for r in self.inner.receivers.iter() {
                    if receiver_ids.contains(&r.id()) && !r.is_idling() {
                        log::debug!(
                            "flush_and_sync_group: group={} waiting for receiver id={}",
                            group_id,
                            r.id()
                        );
                        r.flush(self).await;
                        any_flushed = true;
                    }
                }

                if !any_flushed {
                    log::debug!(
                        "flush_and_sync_group: group={} all receivers idle after {} iterations",
                        group_id,
                        iters
                    );
                    break;
                }
            }
        }

        // Flush-then-idle loop: repeatedly flush partial batches and wait for
        // the group to become idle. A single flush is not sufficient because
        // handlers completing after the flush can send new messages to batched
        // receivers, creating new partial batches that keep the group counter
        // elevated. We loop until the group truly converges.
        let drain_fuse_count = 64i32;
        let mut drain_iters = 0;

        loop {
            drain_iters += 1;
            if drain_iters > drain_fuse_count {
                log::warn!(
                    "flush_and_sync_group: group {} drain loop did not converge after {} iterations, processing_count={}",
                    group_id,
                    drain_fuse_count,
                    self.inner.group_registry.processing_count(group_id)
                );
                break;
            }

            // Flush all receivers in the group to process any partial batches
            log::debug!(
                "flush_and_sync_group: group={} drain iteration {} flush, processing_count={}",
                group_id,
                drain_iters,
                self.inner.group_registry.processing_count(group_id)
            );
            let receiver_ids = self.inner.group_registry.receivers_for_group(group_id);
            for r in self.inner.receivers.iter() {
                if receiver_ids.contains(&r.id()) {
                    r.flush(self).await;
                }
            }

            // Check if the group is now idle (all handlers completed, all batches flushed)
            if self.inner.group_registry.is_idle(group_id) {
                log::debug!(
                    "flush_and_sync_group: group={} idle after {} drain iterations",
                    group_id,
                    drain_iters
                );
                break;
            }

            // Not yet idle — handlers are still in flight. Yield to let them
            // make progress, then flush again to catch any new partial batches.
            tokio::task::yield_now().await;
        }

        // Sync only receivers that handled messages from this group
        log::debug!(
            "flush_and_sync_group: group={} starting sync_group",
            group_id
        );
        self.sync_group(group_id).await;
        log::debug!("flush_and_sync_group: END group={}", group_id);
    }

    /// Flushes all receivers that have messages from the current group.
    ///
    /// This is designed to be called from within a handler to wait for all
    /// child messages (sent during this handler's execution) to complete.
    /// It automatically uses the group_id from task-local storage.
    ///
    /// Unlike type-specific `flush_and_sync::<T>` methods, this flushes ALL
    /// receivers that have messages from the current group, including intermediate
    /// batched receivers. This prevents hangs caused by unflushed batch buffers.
    ///
    /// If called outside a group context (no group_id in task-local storage),
    /// this method does nothing and returns immediately.
    ///
    /// # Example
    ///
    /// ```ignore
    /// async fn handle(&self, msg: MyMessage, bus: &Bus) -> Result<(), Error> {
    ///     // Send child messages
    ///     for item in items {
    ///         bus.send(ChildMessage { ... }).await?;
    ///     }
    ///
    ///     // Wait for all child messages to complete
    ///     bus.flush_current_group().await;
    ///
    ///     // Now safe to do cleanup or send final notifications
    ///     Ok(())
    /// }
    /// ```
    pub async fn flush_current_group(&self) {
        let Some(group_id) = Self::current_group_id() else {
            log::trace!("flush_current_group: no group_id in task-local storage, skipping");
            return;
        };

        log::debug!(
            "flush_current_group: group={} processing_count={}",
            group_id,
            self.inner.group_registry.processing_count(group_id)
        );

        // Flush all receivers that have messages from this group.
        // This triggers batched receivers to process their buffered messages.
        //
        // IMPORTANT: We use flush_nowait (non-blocking) because:
        // 1. The calling handler is itself counted in processing_count
        // 2. Sibling handlers may be running concurrently
        // 3. Blocking flush can cause deadlock when called from within a handler
        // The outer flush_and_sync_group will wait for all handlers to complete.
        let receiver_ids = self.inner.group_registry.receivers_for_group(group_id);

        for r in self.inner.receivers.iter() {
            if receiver_ids.contains(&r.id()) {
                r.flush_nowait(self);
            }
        }

        log::debug!(
            "flush_current_group: group={} complete, processing_count={}",
            group_id,
            self.inner.group_registry.processing_count(group_id)
        );
    }

    /// Returns `true` if the group has no in-flight tasks.
    ///
    /// Unknown groups are considered idle (they have no tasks by definition).
    #[inline]
    pub fn is_group_idle(&self, group_id: GroupId) -> bool {
        self.inner.group_registry.is_idle(group_id)
    }

    /// Returns the current number of in-flight tasks for a group.
    ///
    /// Returns 0 for unknown groups.
    #[inline]
    pub fn group_processing_count(&self, group_id: GroupId) -> u64 {
        self.inner.group_registry.processing_count(group_id)
    }

    /// Removes a group from the registry only if it's idle.
    ///
    /// Use this to safely clean up groups that are no longer needed.
    /// The check and removal are atomic, preventing race conditions.
    ///
    /// Returns a [`GroupRemovalResult`] indicating what happened:
    /// - `Removed` if the group was successfully removed (it was idle)
    /// - `NotIdle` if the group exists but has in-flight tasks
    /// - `NotFound` if the group doesn't exist
    ///
    /// # Example
    ///
    /// ```ignore
    /// use messagebus::GroupRemovalResult;
    ///
    /// match bus.remove_group(job_id) {
    ///     GroupRemovalResult::Removed => println!("Group cleaned up"),
    ///     GroupRemovalResult::NotIdle => println!("Group still has in-flight tasks"),
    ///     GroupRemovalResult::NotFound => println!("Group doesn't exist"),
    /// }
    /// ```
    #[inline]
    pub fn remove_group(&self, group_id: GroupId) -> GroupRemovalResult {
        self.inner.group_registry.remove_if_idle(group_id)
    }

    /// Removes a group from the registry unconditionally.
    ///
    /// # Warning
    ///
    /// This does not check if the group is idle. Removing a group with
    /// in-flight tasks will cause those tasks to not be tracked.
    /// Prefer [`remove_group`](Self::remove_group) for safe cleanup.
    ///
    /// Returns `true` if the group was removed, `false` if it didn't exist.
    #[inline]
    pub fn force_remove_group(&self, group_id: GroupId) -> bool {
        self.inner.group_registry.remove(group_id)
    }

    /// Returns the number of groups currently being tracked.
    ///
    /// Useful for monitoring memory usage in long-running applications.
    #[inline]
    pub fn tracked_group_count(&self) -> usize {
        self.inner.group_registry.group_count()
    }

    /// Returns the current task's group ID, if any.
    ///
    /// This reads from task-local storage and returns the group ID
    /// that was set when the current handler was invoked.
    #[inline]
    pub fn current_group_id() -> Option<GroupId> {
        CURRENT_GROUP.try_with(|g| *g).unwrap_or(None)
    }

    /// Executes a closure with a group ID set in task-local storage.
    ///
    /// This is used to propagate group context to sync handlers running
    /// in a blocking thread pool.
    #[inline]
    pub(crate) fn with_group_context<F, R>(group_id: GroupId, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        CURRENT_GROUP.sync_scope(Some(group_id), f)
    }

    /// Executes an async block with a group ID set in task-local storage.
    ///
    /// This is used to propagate group context to async handlers.
    #[inline]
    pub(crate) async fn with_group_context_async<F>(group_id: Option<GroupId>, f: F) -> F::Output
    where
        F: std::future::Future,
    {
        if let Some(gid) = group_id {
            CURRENT_GROUP.scope(Some(gid), f).await
        } else {
            f.await
        }
    }

    /// Returns a clone of the Arc-wrapped group registry.
    ///
    /// This is used by [`GroupGuard`](crate::group::GroupGuard) to ensure
    /// group counters are decremented even if a handler panics.
    pub(crate) fn group_registry(&self) -> Arc<GroupRegistry> {
        Arc::clone(&self.inner.group_registry)
    }

    // ==================== End Group-based Operations ====================

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
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        // Resolve group_id: message's group_id takes precedence, otherwise inherit from task-local
        let group_id = msg.group_id().or_else(Bus::current_group_id);

        if let Some(rs) = self.inner.lookup.get(&(msg.type_tag(), None, None)) {
            let permits = if let Some(x) = self.try_reserve(&tt, rs) {
                x
            } else {
                return Err(SendError::Full(msg).into());
            };

            // Increment group counter for each receiver and track receiver IDs
            if let Some(gid) = group_id {
                for r in rs.iter() {
                    self.inner.group_registry.increment(gid, r.id());
                }
            }

            let mut iter = permits.into_iter().zip(rs.iter());
            let mut counter = 1;
            let total = rs.len();

            while counter < total {
                let (p, r) = iter
                    .next()
                    .expect("iterator should have more elements based on counter");
                if r.send(self, mid, msg.clone(), false, p, group_id).is_err() {
                    // Decrement counter on send failure since no handler will run
                    if let Some(gid) = group_id {
                        self.inner.group_registry.decrement(gid);
                    }
                }

                counter += 1;
            }

            if let Some((p, r)) = iter.next() {
                if r.send(self, mid, msg, false, p, group_id).is_err() {
                    if let Some(gid) = group_id {
                        self.inner.group_registry.decrement(gid);
                    }
                }
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
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        // Resolve group_id: message's group_id takes precedence, otherwise inherit from task-local
        let group_id = msg.group_id().or_else(Bus::current_group_id);

        if let Some(rs) = self.inner.lookup.get(&(msg.type_tag(), None, None)) {
            if let Some((last, head)) = rs.split_last() {
                // Increment group counter for each receiver and track receiver IDs
                if let Some(gid) = group_id {
                    for r in head {
                        self.inner.group_registry.increment(gid, r.id());
                    }
                    self.inner.group_registry.increment(gid, last.id());
                }

                for r in head {
                    if r.send(
                        self,
                        mid,
                        msg.clone(),
                        false,
                        r.reserve(&tt).await,
                        group_id,
                    )
                    .is_err()
                    {
                        // Decrement counter on send failure since no handler will run
                        if let Some(gid) = group_id {
                            self.inner.group_registry.decrement(gid);
                        }
                    }
                }

                if last
                    .send(self, mid, msg, false, last.reserve(&tt).await, group_id)
                    .is_err()
                {
                    if let Some(gid) = group_id {
                        self.inner.group_registry.decrement(gid);
                    }
                }

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
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed(msg).into());
        }

        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        // Resolve group_id: message's group_id takes precedence, otherwise inherit from task-local
        let group_id = msg.group_id().or_else(Bus::current_group_id);

        if let Some(rs) = self.inner.lookup.get(&(msg.type_tag(), None, None)) {
            if let Some((last, head)) = rs.split_last() {
                // Increment group counter for each receiver and track receiver IDs
                if let Some(gid) = group_id {
                    for r in head {
                        self.inner.group_registry.increment(gid, r.id());
                    }
                    self.inner.group_registry.increment(gid, last.id());
                }

                for r in head {
                    if r.force_send(self, mid, msg.clone(), false, group_id)
                        .is_err()
                    {
                        // Decrement counter on send failure since no handler will run
                        if let Some(gid) = group_id {
                            self.inner.group_registry.decrement(gid);
                        }
                    }
                }

                if last.force_send(self, mid, msg, false, group_id).is_err() {
                    if let Some(gid) = group_id {
                        self.inner.group_registry.decrement(gid);
                    }
                }

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
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        // Resolve group_id: message's group_id takes precedence, otherwise inherit from task-local
        let group_id = msg.group_id().or_else(Bus::current_group_id);

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

            // Increment group counter for single receiver and track receiver ID
            if let Some(gid) = group_id {
                self.inner.group_registry.increment(gid, rs.id());
            }

            if let Err(e) = rs.send(self, mid, msg, false, permits, group_id) {
                // Decrement counter on send failure since no handler will run
                if let Some(gid) = group_id {
                    self.inner.group_registry.decrement(gid);
                }
                return Err(e);
            }
            Ok(())
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
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        // Resolve group_id: message's group_id takes precedence, otherwise inherit from task-local
        let group_id = msg.group_id().or_else(Bus::current_group_id);

        if let Some(rs) = self
            .inner
            .lookup
            .get(&(msg.type_tag(), None, None))
            .and_then(|rs| rs.first())
        {
            // Increment group counter for single receiver and track receiver ID
            if let Some(gid) = group_id {
                self.inner.group_registry.increment(gid, rs.id());
            }

            if let Err(e) = rs.send(self, mid, msg, false, rs.reserve(&tt).await, group_id) {
                // Decrement counter on send failure since no handler will run
                if let Some(gid) = group_id {
                    self.inner.group_registry.decrement(gid);
                }
                return Err(e);
            }
            Ok(())
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
        // Resolve group_id: message's group_id takes precedence, otherwise inherit from task-local
        let group_id = req.group_id().or_else(Bus::current_group_id);

        let mut iter = self.select_receivers(tid.clone(), options, Some(rid), None, true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc
                .add_response_waiter::<R>()
                .map_err(|x| x.specify::<M>())?;

            let mid = mid | 1 << (u64::BITS - 1);

            // Increment group counter for single receiver and track receiver ID
            if let Some(gid) = group_id {
                self.inner.group_registry.increment(gid, rc.id());
            }

            if let Err(e) = rc.send(self, mid, req, true, rc.reserve(&tid).await, group_id) {
                // Decrement counter on send failure since no handler will run
                if let Some(gid) = group_id {
                    self.inner.group_registry.decrement(gid);
                }
                return Err(e);
            }
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
        // Resolve group_id: message's group_id takes precedence, otherwise inherit from task-local
        let group_id = req.group_id().or_else(Bus::current_group_id);

        let mut iter = self.select_receivers(tid.clone(), options, Some(rid), Some(eid), true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc.add_response_waiter_we::<R, E>().map_err(|x| {
                x.map_err(|_| unimplemented!())
                    .map_msg(|_| unimplemented!())
            })?;

            // Increment group counter for single receiver and track receiver ID
            if let Some(gid) = group_id {
                self.inner.group_registry.increment(gid, rc.id());
            }

            if let Err(e) = rc.send(
                self,
                mid | 1 << (u64::BITS - 1),
                req,
                true,
                rc.reserve(&tid).await,
                group_id,
            ) {
                // Decrement counter on send failure since no handler will run
                if let Some(gid) = group_id {
                    self.inner.group_registry.decrement(gid);
                }
                return Err(e.map_err(|_| unimplemented!()));
            }

            rx.await.map_err(|x| x.specify::<M>())
        } else {
            Err(Error::NoReceivers)
        }
    }

    /// Sends a type-erased boxed message asynchronously.
    ///
    /// This is useful when you need to send messages without knowing their concrete
    /// type at compile time, such as in relay or forwarding scenarios.
    ///
    /// The message must implement `Clone` (via `#[message(clone)]`) for broadcast
    /// to multiple receivers.
    ///
    /// # Errors
    ///
    /// - [`SendError::Closed`] - Bus is closed
    pub async fn send_boxed(
        &self,
        msg: Box<dyn Message>,
        options: SendOptions,
    ) -> Result<(), Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        // Resolve group_id: message's group_id takes precedence, otherwise inherit from task-local
        let group_id = msg.group_id().or_else(Bus::current_group_id);

        let mut iter = self.select_receivers(tt.clone(), options, None, None, false);
        let first = iter.next();

        // Collect receivers to count them for group tracking
        let rest: Vec<_> = iter.collect();

        // Increment group counter for all receivers
        if let Some(gid) = group_id {
            if let Some(r) = &first {
                self.inner.group_registry.increment(gid, r.id());
            }
            for r in &rest {
                self.inner.group_registry.increment(gid, r.id());
            }
        }

        for r in rest {
            if r.send_boxed(
                self,
                mid,
                msg.try_clone_boxed()
                    .expect("message must implement clone for broadcast"),
                false,
                r.reserve(&tt).await,
            )
            .is_err()
            {
                if let Some(gid) = group_id {
                    self.inner.group_registry.decrement(gid);
                }
            }
        }

        if let Some(r) = first {
            if r.send_boxed(
                self,
                mid,
                msg.try_clone_boxed()
                    .expect("message must implement clone for broadcast"),
                false,
                r.reserve(&tt).await,
            )
            .is_err()
            {
                if let Some(gid) = group_id {
                    self.inner.group_registry.decrement(gid);
                }
            }
        } else {
            warn!("Unhandled message: no receivers");
        }

        Ok(())
    }

    /// Sends a type-erased boxed message to a single receiver.
    ///
    /// Like [`send_boxed()`](Bus::send_boxed) but sends to only one receiver
    /// instead of broadcasting.
    ///
    /// # Errors
    ///
    /// - [`Error::NoReceivers`] - No receiver for this message type
    /// - [`SendError::Closed`] - Bus is closed
    pub async fn send_boxed_one(
        &self,
        msg: Box<dyn Message>,
        options: SendOptions,
    ) -> Result<(), Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed(msg).into());
        }

        let tt = msg.type_tag();
        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        // Resolve group_id: message's group_id takes precedence, otherwise inherit from task-local
        let group_id = msg.group_id().or_else(Bus::current_group_id);

        let mut iter = self.select_receivers(tt.clone(), options, None, None, false);
        if let Some(rs) = iter.next() {
            // Increment group counter for single receiver
            if let Some(gid) = group_id {
                self.inner.group_registry.increment(gid, rs.id());
            }

            if let Err(e) = rs.send_boxed(self, mid, msg, false, rs.reserve(&tt).await) {
                if let Some(gid) = group_id {
                    self.inner.group_registry.decrement(gid);
                }
                return Err(e);
            }
            Ok(())
        } else {
            Err(Error::NoReceivers)
        }
    }

    /// Sends a type-erased boxed request and waits for a boxed response.
    ///
    /// This is the type-erased version of [`request()`](Bus::request), useful
    /// for relay or forwarding scenarios where message types aren't known at
    /// compile time.
    ///
    /// # Errors
    ///
    /// - [`Error::NoReceivers`] - No receiver for this message type
    /// - [`Error::NoResponse`] - Receiver did not respond
    /// - [`SendError::Closed`] - Bus is closed
    pub async fn request_boxed(
        &self,
        req: Box<dyn Message>,
        options: SendOptions,
    ) -> Result<Box<dyn Message>, Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed(req).into());
        }

        let tt = req.type_tag();
        // Resolve group_id: message's group_id takes precedence, otherwise inherit from task-local
        let group_id = req.group_id().or_else(Bus::current_group_id);

        let mut iter = self.select_receivers(tt.clone(), options, None, None, true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc.add_response_waiter_boxed().map_err(|x| {
                x.map_err(|_| unimplemented!())
                    .map_msg(|_| unimplemented!())
            })?;

            // Increment group counter for single receiver
            if let Some(gid) = group_id {
                self.inner.group_registry.increment(gid, rc.id());
            }

            if let Err(e) = rc.send_boxed(
                self,
                mid | 1 << (usize::BITS - 1),
                req,
                true,
                rc.reserve(&tt).await,
            ) {
                if let Some(gid) = group_id {
                    self.inner.group_registry.decrement(gid);
                }
                return Err(e);
            }

            rx.await.map_err(|x| x.specify::<Box<dyn Message>>())
        } else {
            Err(Error::NoReceivers)
        }
    }

    /// Sends a type-erased boxed request with explicit error type.
    ///
    /// Like [`request_boxed()`](Bus::request_boxed) but allows specifying a custom
    /// error type `E` that the handler may return.
    ///
    /// # Type Parameters
    ///
    /// - `E` - The error type the handler may return
    pub async fn request_boxed_we<E: StdSyncSendError>(
        &self,
        req: Box<dyn Message>,
        options: SendOptions,
    ) -> Result<Box<dyn Message>, Error<Box<dyn Message>, E>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(SendError::Closed(req).into());
        }

        let tt = req.type_tag();
        let eid = E::type_tag_();
        // Resolve group_id: message's group_id takes precedence, otherwise inherit from task-local
        let group_id = req.group_id().or_else(Bus::current_group_id);

        let mut iter = self.select_receivers(tt.clone(), options, None, Some(eid), true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc.add_response_waiter_boxed_we().map_err(|x| {
                x.map_err(|_| unimplemented!())
                    .map_msg(|_| unimplemented!())
            })?;

            // Increment group counter for single receiver
            if let Some(gid) = group_id {
                self.inner.group_registry.increment(gid, rc.id());
            }

            if let Err(e) = rc.send_boxed(
                self,
                mid | 1 << (usize::BITS - 1),
                req,
                true,
                rc.reserve(&tt).await,
            ) {
                if let Some(gid) = group_id {
                    self.inner.group_registry.decrement(gid);
                }
                return Err(e.map_err(|_| unimplemented!()));
            }

            rx.await.map_err(|x| x.specify::<Box<dyn Message>>())
        } else {
            Err(Error::NoReceivers)
        }
    }

    /// Deserializes and sends a message to a single receiver.
    ///
    /// This method is used for remote message transport. It deserializes the message
    /// from a type-erased deserializer using the registered message type for the
    /// given type tag.
    ///
    /// The message type must be registered with [`register_shared_message`] and
    /// marked with `#[message(shared)]`.
    ///
    /// # Arguments
    ///
    /// * `tt` - The type tag identifying the message type
    /// * `de` - The deserializer to read the message from
    /// * `options` - Send routing options
    ///
    /// # Errors
    ///
    /// - [`Error::NoReceivers`] - No receiver for this message type
    /// - [`Error::NoResponse`] - Bus is closed
    pub async fn send_deserialize_one<'a, 'b: 'a, 'c: 'a>(
        &'a self,
        tt: TypeTag,
        de: &'b mut dyn erased_serde::Deserializer<'c>,
        _options: SendOptions,
    ) -> Result<(), Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::Acquire) {
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

    /// Deserializes a request message and waits for a boxed response.
    ///
    /// This method is used for remote request/response transport. It deserializes
    /// the request from a type-erased deserializer and returns the response as a
    /// boxed message.
    ///
    /// The message type must be registered with [`register_shared_message`] and
    /// marked with `#[message(shared)]`.
    ///
    /// # Arguments
    ///
    /// * `tt` - The type tag identifying the request message type
    /// * `de` - The deserializer to read the request from
    /// * `options` - Send routing options
    ///
    /// # Errors
    ///
    /// - [`Error::NoReceivers`] - No receiver for this message type
    /// - [`Error::NoResponse`] - Receiver did not respond or bus is closed
    pub async fn request_deserialize<'a, 'b: 'a, 'c: 'a>(
        &'a self,
        tt: TypeTag,
        de: &'b mut dyn erased_serde::Deserializer<'c>,
        options: SendOptions,
    ) -> Result<Box<dyn Message>, Error<Box<dyn Message>>> {
        if self.inner.closed.load(Ordering::Acquire) {
            warn!("closed message bus");
            return Err(Error::NoResponse);
        }

        let mut iter = self.select_receivers(tt.clone(), options, None, None, true);
        if let Some(rc) = iter.next() {
            let (mid, rx) = rc
                .add_response_waiter_boxed()
                .expect("failed to add response waiter");
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
