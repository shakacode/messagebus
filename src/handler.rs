//! Handler traits for processing messages.
//!
//! This module defines the core handler traits that receivers implement to process messages.
//! There are 8 handler variants to cover different use cases:
//!
//! | Trait | Thread-Safe | Batched | Async |
//! |-------|-------------|---------|-------|
//! | [`Handler`] | Yes (Send+Sync) | No | No |
//! | [`AsyncHandler`] | Yes (Send+Sync) | No | Yes |
//! | [`SynchronizedHandler`] | No (Send only) | No | No |
//! | [`AsyncSynchronizedHandler`] | No (Send only) | No | Yes |
//! | [`BatchHandler`] | Yes (Send+Sync) | Yes | No |
//! | [`AsyncBatchHandler`] | Yes (Send+Sync) | Yes | Yes |
//! | [`BatchSynchronizedHandler`] | No (Send only) | Yes | No |
//! | [`AsyncBatchSynchronizedHandler`] | No (Send only) | Yes | Yes |

use core::iter::FromIterator;
use std::pin::Pin;

use crate::{error::StdSyncSendError, Bus, Message};
use async_trait::async_trait;
use futures::Stream;

/// Statistics from an async producer.
#[derive(Debug, Clone, Copy)]
pub struct ProducerStats {
    /// Number of items successfully produced.
    pub completed: usize,
    /// Number of items that failed to produce.
    pub failed: usize,
}

/// Async producer trait for streaming message generation.
///
/// Producers can generate multiple messages from a single input message.
#[async_trait]
pub trait AsyncProducer<M: Message>: Send + Sync {
    /// The type of items produced.
    type Item: Message;
    /// The response type returned after production completes.
    type Response: Message;
    /// The error type.
    type Error: StdSyncSendError;

    /// Produces a stream of items from an input message.
    async fn producer(
        &self,
        msg: M,
        bus: &Bus,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Self::Item, Self::Error>> + Send + '_>>, Self::Error>;

    /// Called when production is complete.
    async fn finish(&self, stats: ProducerStats, bus: &Bus) -> Result<Self::Response, Self::Error>;
}

/// Synchronous, thread-safe handler trait.
///
/// Use this handler when:
/// - Your handler is stateless or uses interior mutability (e.g., `Arc<Mutex<T>>`)
/// - You don't need async operations in the handler
/// - You want concurrent message processing
///
/// The handler must be `Send + Sync` to allow concurrent access from multiple tasks.
///
/// # Example
///
/// ```rust,ignore
/// struct MyHandler {
///     counter: Arc<AtomicU32>,
/// }
///
/// impl Handler<MyMessage> for MyHandler {
///     type Error = MyError;
///     type Response = ();
///
///     fn handle(&self, msg: MyMessage, bus: &Bus) -> Result<(), MyError> {
///         self.counter.fetch_add(1, Ordering::SeqCst);
///         Ok(())
///     }
/// }
/// ```
pub trait Handler<M: Message>: Send + Sync {
    /// The error type returned by this handler.
    type Error: StdSyncSendError;
    /// The response type returned by this handler.
    type Response: Message;

    /// Handles a single message.
    ///
    /// The `bus` parameter allows sending additional messages during handling.
    fn handle(&self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;

    /// Called to synchronize handler state.
    ///
    /// Override this to perform cleanup or persist state when the bus is syncing.
    fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Asynchronous, thread-safe handler trait.
///
/// Use this handler when:
/// - Your handler needs to perform async I/O (network, file, etc.)
/// - Your handler is stateless or uses interior mutability
/// - You want concurrent message processing
///
/// The handler must be `Send + Sync` to allow concurrent access.
///
/// # Example
///
/// ```rust,ignore
/// #[async_trait]
/// impl AsyncHandler<FetchUrl> for HttpClient {
///     type Error = MyError;
///     type Response = String;
///
///     async fn handle(&self, msg: FetchUrl, _bus: &Bus) -> Result<String, MyError> {
///         let response = self.client.get(&msg.url).await?;
///         Ok(response.text().await?)
///     }
/// }
/// ```
#[async_trait]
pub trait AsyncHandler<M: Message>: Send + Sync {
    /// The error type returned by this handler.
    type Error: StdSyncSendError;
    /// The response type returned by this handler.
    type Response: Message;

    /// Handles a single message asynchronously.
    async fn handle(&self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;

    /// Called to synchronize handler state.
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Synchronous handler with mutable access.
///
/// Use this handler when:
/// - Your handler maintains mutable state that needs direct access (not interior mutability)
/// - You don't need async operations
/// - Messages must be processed sequentially (no concurrent access)
///
/// Only requires `Send` (not `Sync`) because messages are processed one at a time.
/// Register with [`register_unsync()`](crate::builder::Module::register_unsync).
///
/// # Example
///
/// ```rust,ignore
/// struct Counter {
///     count: u64,
/// }
///
/// impl SynchronizedHandler<Increment> for Counter {
///     type Error = MyError;
///     type Response = ();
///
///     fn handle(&mut self, _msg: Increment, _bus: &Bus) -> Result<(), MyError> {
///         self.count += 1;
///         Ok(())
///     }
/// }
///
/// // Register with register_unsync
/// let bus = Bus::build()
///     .register_unsync(Counter { count: 0 })
///     .subscribe_sync::<Increment>(8, Default::default())
///     .done()
///     .build();
/// ```
pub trait SynchronizedHandler<M: Message>: Send {
    /// The error type returned by this handler.
    type Error: StdSyncSendError;
    /// The response type returned by this handler.
    type Response: Message;

    /// Handles a single message with mutable access to the handler.
    fn handle(&mut self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;

    /// Called to synchronize handler state.
    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Asynchronous handler with mutable access.
///
/// Use this handler when:
/// - Your handler maintains mutable state
/// - You need async operations in the handler
/// - Messages must be processed sequentially
///
/// Combines the benefits of [`AsyncHandler`] and [`SynchronizedHandler`].
/// Register with [`register_unsync()`](crate::builder::Module::register_unsync).
#[async_trait]
pub trait AsyncSynchronizedHandler<M: Message>: Send {
    /// The error type returned by this handler.
    type Error: StdSyncSendError;
    /// The response type returned by this handler.
    type Response: Message;

    /// Handles a single message asynchronously with mutable access.
    async fn handle(&mut self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;

    /// Called to synchronize handler state.
    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Synchronous, thread-safe batch handler trait.
///
/// Use this handler when:
/// - You want to process multiple messages at once for efficiency
/// - Your handler is stateless or uses interior mutability
/// - You don't need async operations
///
/// Batch handlers are useful for reducing per-message overhead, such as database
/// batch inserts or bulk API calls.
///
/// # Type Parameters
///
/// - `InBatch` - The collection type for incoming messages (usually `Vec<M>`)
/// - `OutBatch` - The collection type for responses (usually `Vec<Response>`)
///
/// # Example
///
/// ```rust,ignore
/// impl BatchHandler<LogEntry> for BatchLogger {
///     type Error = MyError;
///     type Response = ();
///     type InBatch = Vec<LogEntry>;
///     type OutBatch = Vec<()>;
///
///     fn handle(&self, msgs: Vec<LogEntry>, _bus: &Bus) -> Result<Vec<()>, MyError> {
///         // Process all log entries at once
///         self.write_batch(&msgs)?;
///         Ok(vec![(); msgs.len()])
///     }
/// }
///
/// // Register with batch subscription
/// let bus = Bus::build()
///     .register(BatchLogger::new())
///     .subscribe_batch_sync::<LogEntry>(64, BufferUnorderedBatchedConfig {
///         batch_size: 100,
///         ..Default::default()
///     })
///     .done()
///     .build();
/// ```
pub trait BatchHandler<M: Message>: Send + Sync {
    /// The error type returned by this handler.
    type Error: StdSyncSendError + Clone;
    /// The response type for individual messages.
    type Response: Message;
    /// The input batch collection type.
    type InBatch: FromIterator<M> + Send;
    /// The output batch collection type.
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    /// Handles a batch of messages.
    fn handle(&self, msg: Self::InBatch, bus: &Bus) -> Result<Self::OutBatch, Self::Error>;

    /// Called to synchronize handler state.
    fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Asynchronous, thread-safe batch handler trait.
///
/// Use this handler when:
/// - You want to process multiple messages at once for efficiency
/// - Your handler needs async I/O operations
/// - Your handler is stateless or uses interior mutability
///
/// Ideal for async batch operations like bulk database writes or parallel API calls.
///
/// # Example
///
/// ```rust,ignore
/// #[async_trait]
/// impl AsyncBatchHandler<DbRecord> for AsyncBatchWriter {
///     type Error = MyError;
///     type Response = ();
///     type InBatch = Vec<DbRecord>;
///     type OutBatch = Vec<()>;
///
///     async fn handle(&self, msgs: Vec<DbRecord>, _bus: &Bus) -> Result<Vec<()>, MyError> {
///         // Async bulk insert
///         self.db.bulk_insert(&msgs).await?;
///         Ok(vec![(); msgs.len()])
///     }
/// }
/// ```
#[async_trait]
pub trait AsyncBatchHandler<M: Message>: Send + Sync {
    /// The error type returned by this handler.
    type Error: StdSyncSendError + Clone;
    /// The response type for individual messages.
    type Response: Message;
    /// The input batch collection type.
    type InBatch: FromIterator<M> + Send;
    /// The output batch collection type.
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    /// Handles a batch of messages asynchronously.
    async fn handle(&self, msg: Self::InBatch, bus: &Bus) -> Result<Self::OutBatch, Self::Error>;

    /// Called to synchronize handler state.
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Synchronous batch handler with mutable access.
///
/// Use this handler when:
/// - You want to process batches of messages for efficiency
/// - Your handler maintains mutable state
/// - You don't need async operations
/// - Messages must be processed sequentially
///
/// Combines the benefits of [`BatchHandler`] and [`SynchronizedHandler`].
/// Register with [`register_unsync()`](crate::builder::Module::register_unsync).
///
/// # Example
///
/// ```rust,ignore
/// struct BatchCounter {
///     total: u64,
///     batch_count: u32,
/// }
///
/// impl BatchSynchronizedHandler<CountMsg> for BatchCounter {
///     type Error = MyError;
///     type Response = ();
///     type InBatch = Vec<CountMsg>;
///     type OutBatch = Vec<()>;
///
///     fn handle(&mut self, msgs: Vec<CountMsg>, _bus: &Bus) -> Result<Vec<()>, MyError> {
///         self.batch_count += 1;
///         for msg in &msgs {
///             self.total += msg.value;
///         }
///         Ok(vec![(); msgs.len()])
///     }
/// }
/// ```
pub trait BatchSynchronizedHandler<M: Message>: Send {
    /// The error type returned by this handler.
    type Error: StdSyncSendError + Clone;
    /// The response type for individual messages.
    type Response: Message;
    /// The input batch collection type.
    type InBatch: FromIterator<M> + Send;
    /// The output batch collection type.
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    /// Handles a batch of messages with mutable access.
    fn handle(&mut self, msg: Self::InBatch, bus: &Bus) -> Result<Self::OutBatch, Self::Error>;

    /// Called to synchronize handler state.
    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Asynchronous batch handler with mutable access.
///
/// Use this handler when:
/// - You want to process batches of messages for efficiency
/// - Your handler maintains mutable state
/// - You need async operations
/// - Messages must be processed sequentially
///
/// The most feature-complete handler, combining batching, async, and mutable state.
/// Register with [`register_unsync()`](crate::builder::Module::register_unsync).
///
/// # Example
///
/// ```rust,ignore
/// #[async_trait]
/// impl AsyncBatchSynchronizedHandler<EventMsg> for AsyncEventProcessor {
///     type Error = MyError;
///     type Response = ();
///     type InBatch = Vec<EventMsg>;
///     type OutBatch = Vec<()>;
///
///     async fn handle(&mut self, msgs: Vec<EventMsg>, _bus: &Bus) -> Result<Vec<()>, MyError> {
///         self.batch_count += 1;
///         // Async batch processing
///         self.db.insert_events(&msgs).await?;
///         Ok(vec![(); msgs.len()])
///     }
/// }
/// ```
#[async_trait]
pub trait AsyncBatchSynchronizedHandler<M: Message>: Send {
    /// The error type returned by this handler.
    type Error: StdSyncSendError + Clone;
    /// The response type for individual messages.
    type Response: Message;
    /// The input batch collection type.
    type InBatch: FromIterator<M> + Send;
    /// The output batch collection type.
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    /// Handles a batch of messages asynchronously with mutable access.
    async fn handle(
        &mut self,
        msg: Self::InBatch,
        bus: &Bus,
    ) -> Result<Self::OutBatch, Self::Error>;

    /// Called to synchronize handler state.
    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Local synchronous handler for single-threaded contexts.
///
/// Unlike other handlers, this trait does not require `Send` or `Sync`,
/// making it suitable for thread-local or single-threaded use cases.
///
/// Note: Messages are delivered as a `Vec<M>` for batching purposes.
pub trait LocalHandler<M: Message> {
    /// The error type returned by this handler.
    type Error: StdSyncSendError;
    /// The response type returned by this handler.
    type Response: Message;

    /// Handles messages with mutable access.
    fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> Result<Self::Response, Self::Error>;

    /// Called to synchronize handler state.
    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Local asynchronous handler for single-threaded contexts.
///
/// Like [`LocalHandler`] but supports async operations.
/// Does not require `Send` or `Sync`.
#[async_trait]
pub trait LocalAsyncHandler<M: Message> {
    /// The error type returned by this handler.
    type Error: StdSyncSendError;
    /// The response type returned by this handler.
    type Response: Message;

    /// Handles messages asynchronously with mutable access.
    async fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> Result<Self::Response, Self::Error>;

    /// Called to synchronize handler state.
    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Local synchronous batch handler for single-threaded contexts.
///
/// Combines batching with local (non-Send/Sync) handling.
pub trait LocalBatchHandler<M: Message> {
    /// The error type returned by this handler.
    type Error: StdSyncSendError + Clone;
    /// The response type for individual messages.
    type Response: Message;
    /// The input batch collection type.
    type InBatch: FromIterator<M> + Send;
    /// The output batch collection type.
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    /// Handles a batch of messages with mutable access.
    fn handle(&mut self, msg: Self::InBatch, bus: &Bus) -> Result<Self::OutBatch, Self::Error>;

    /// Called to synchronize handler state.
    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Local asynchronous batch handler for single-threaded contexts.
///
/// Combines async batching with local (non-Send/Sync) handling.
#[async_trait]
pub trait LocalAsyncBatchHandler<M: Message> {
    /// The error type returned by this handler.
    type Error: StdSyncSendError + Clone;
    /// The response type for individual messages.
    type Response: Message;
    /// The input batch collection type.
    type InBatch: FromIterator<M> + Send;
    /// The output batch collection type.
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    /// Handles a batch of messages asynchronously with mutable access.
    async fn handle(
        &mut self,
        msg: Self::InBatch,
        bus: &Bus,
    ) -> Result<Self::OutBatch, Self::Error>;

    /// Called to synchronize handler state.
    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}
