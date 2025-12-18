use std::sync::Arc;

use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error,
    receivers::{BufferUnorderedBatchedConfig, SynchronizedBatchedConfig},
    AsyncBatchSynchronizedHandler, BatchHandler, BatchSynchronizedHandler, Bus, Message,
};
use parking_lot::Mutex;
use thiserror::Error;

#[derive(Debug, Error, Clone, MbError)]
enum Error {
    #[error("Error({0})")]
    Error(Arc<anyhow::Error>),
}

impl<M: Message> From<error::Error<M>> for Error {
    fn from(err: error::Error<M>) -> Self {
        Self::Error(Arc::new(err.into()))
    }
}

// ============================================================================
// BatchHandler (sync, thread-safe) tests
// ============================================================================

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct BatchSyncMsg(i32);

struct SyncBatchReceiver {
    batches: Arc<Mutex<Vec<Vec<i32>>>>,
}

impl BatchHandler<BatchSyncMsg> for SyncBatchReceiver {
    type Error = Error;
    type Response = ();
    type InBatch = Vec<BatchSyncMsg>;
    type OutBatch = Vec<()>;

    fn handle(
        &self,
        msg: Vec<BatchSyncMsg>,
        _bus: &Bus,
    ) -> Result<Vec<Self::Response>, Self::Error> {
        let values: Vec<i32> = msg.into_iter().map(|m| m.0).collect();
        self.batches.lock().push(values);
        Ok(vec![])
    }
}

/// Test BatchHandler with basic batching
#[tokio::test]
async fn test_batch_handler_basic() {
    let batches = Arc::new(Mutex::new(Vec::new()));

    let (b, poller) = Bus::build()
        .register(SyncBatchReceiver {
            batches: batches.clone(),
        })
        .subscribe_batch_sync::<BatchSyncMsg>(
            16,
            BufferUnorderedBatchedConfig {
                batch_size: 5,
                ..Default::default()
            },
        )
        .done()
        .build();

    // Send 12 messages - should create batches of 5, 5, 2
    for i in 1..=12 {
        b.send(BatchSyncMsg(i)).await.unwrap();
    }

    b.flush_all().await;

    let lock = batches.lock();
    let total_items: usize = lock.iter().map(|batch| batch.len()).sum();
    assert_eq!(total_items, 12);

    b.close().await;
    poller.await;
}

/// Test BatchHandler with exact batch size
#[tokio::test]
async fn test_batch_handler_exact_batches() {
    let batches = Arc::new(Mutex::new(Vec::new()));

    let (b, poller) = Bus::build()
        .register(SyncBatchReceiver {
            batches: batches.clone(),
        })
        .subscribe_batch_sync::<BatchSyncMsg>(
            16,
            BufferUnorderedBatchedConfig {
                batch_size: 4,
                ..Default::default()
            },
        )
        .done()
        .build();

    // Send exactly 8 messages - should create 2 batches of 4
    for i in 1..=8 {
        b.send(BatchSyncMsg(i)).await.unwrap();
    }

    b.flush_all().await;

    let lock = batches.lock();
    let total_items: usize = lock.iter().map(|batch| batch.len()).sum();
    assert_eq!(total_items, 8);

    // Verify all values are present
    let all_values: Vec<i32> = lock.iter().flatten().copied().collect();
    for i in 1..=8 {
        assert!(all_values.contains(&i), "Missing value {}", i);
    }

    b.close().await;
    poller.await;
}

/// Test BatchHandler with single message
#[tokio::test]
async fn test_batch_handler_single_message() {
    let batches = Arc::new(Mutex::new(Vec::new()));

    let (b, poller) = Bus::build()
        .register(SyncBatchReceiver {
            batches: batches.clone(),
        })
        .subscribe_batch_sync::<BatchSyncMsg>(
            16,
            BufferUnorderedBatchedConfig {
                batch_size: 10,
                ..Default::default()
            },
        )
        .done()
        .build();

    // Send single message
    b.send(BatchSyncMsg(42)).await.unwrap();

    b.flush_all().await;

    let lock = batches.lock();
    let total_items: usize = lock.iter().map(|batch| batch.len()).sum();
    assert_eq!(total_items, 1);

    b.close().await;
    poller.await;
}

// ============================================================================
// BatchSynchronizedHandler (sync, mutable state) tests
// ============================================================================

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct BatchSyncStatefulMsg(i32);

#[derive(Debug, Clone, Message)]
struct GetBatchState;

#[derive(Debug, Clone, Message)]
struct BatchStateResult {
    total: i64,
    batch_count: u32,
}

struct SyncBatchStatefulReceiver {
    total: i64,
    batch_count: u32,
}

impl SyncBatchStatefulReceiver {
    fn new() -> Self {
        Self {
            total: 0,
            batch_count: 0,
        }
    }
}

impl BatchSynchronizedHandler<BatchSyncStatefulMsg> for SyncBatchStatefulReceiver {
    type Error = Error;
    type Response = ();
    type InBatch = Vec<BatchSyncStatefulMsg>;
    type OutBatch = Vec<()>;

    fn handle(
        &mut self,
        msg: Vec<BatchSyncStatefulMsg>,
        _bus: &Bus,
    ) -> Result<Vec<Self::Response>, Self::Error> {
        self.batch_count += 1;
        for m in msg {
            self.total += m.0 as i64;
        }
        Ok(vec![])
    }
}

impl messagebus::SynchronizedHandler<GetBatchState> for SyncBatchStatefulReceiver {
    type Error = Error;
    type Response = BatchStateResult;

    fn handle(&mut self, _: GetBatchState, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(BatchStateResult {
            total: self.total,
            batch_count: self.batch_count,
        })
    }
}

/// Test BatchSynchronizedHandler with mutable state
#[tokio::test]
async fn test_batch_synchronized_handler_basic() {
    let (b, poller) = Bus::build()
        .register_unsync(SyncBatchStatefulReceiver::new())
        .subscribe_batch_sync::<BatchSyncStatefulMsg>(
            16,
            SynchronizedBatchedConfig {
                batch_size: 5,
                ..Default::default()
            },
        )
        .subscribe_sync::<GetBatchState>(8, Default::default())
        .done()
        .build();

    // Send 10 messages (values 1-10)
    for i in 1..=10 {
        b.send(BatchSyncStatefulMsg(i)).await.unwrap();
    }

    b.flush_all().await;

    // Get the state
    let result = b
        .request_we::<_, BatchStateResult, Error>(GetBatchState, Default::default())
        .await
        .unwrap();

    // Total should be 1+2+3+...+10 = 55
    assert_eq!(result.total, 55);
    // Should have processed at least 1 batch
    assert!(result.batch_count >= 1);

    b.close().await;
    poller.await;
}

/// Test BatchSynchronizedHandler maintains state across batches
#[tokio::test]
async fn test_batch_synchronized_handler_state_accumulation() {
    let (b, poller) = Bus::build()
        .register_unsync(SyncBatchStatefulReceiver::new())
        .subscribe_batch_sync::<BatchSyncStatefulMsg>(
            32,
            SynchronizedBatchedConfig {
                batch_size: 3,
                ..Default::default()
            },
        )
        .subscribe_sync::<GetBatchState>(8, Default::default())
        .done()
        .build();

    // Send messages in groups to ensure multiple batches
    for i in 1..=15 {
        b.send(BatchSyncStatefulMsg(i)).await.unwrap();
    }

    b.flush_all().await;

    // Get the state
    let result = b
        .request_we::<_, BatchStateResult, Error>(GetBatchState, Default::default())
        .await
        .unwrap();

    // Total should be 1+2+...+15 = 120
    assert_eq!(result.total, 120);

    b.close().await;
    poller.await;
}

// ============================================================================
// AsyncBatchSynchronizedHandler tests
// ============================================================================

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct AsyncBatchStatefulMsg(i32);

#[derive(Debug, Clone, Message)]
struct GetAsyncBatchState;

#[derive(Debug, Clone, Message)]
struct AsyncBatchStateResult {
    total: i64,
    batch_count: u32,
}

struct AsyncBatchStatefulReceiver {
    total: i64,
    batch_count: u32,
}

impl AsyncBatchStatefulReceiver {
    fn new() -> Self {
        Self {
            total: 0,
            batch_count: 0,
        }
    }
}

#[async_trait]
impl AsyncBatchSynchronizedHandler<AsyncBatchStatefulMsg> for AsyncBatchStatefulReceiver {
    type Error = Error;
    type Response = ();
    type InBatch = Vec<AsyncBatchStatefulMsg>;
    type OutBatch = Vec<()>;

    async fn handle(
        &mut self,
        msg: Vec<AsyncBatchStatefulMsg>,
        _bus: &Bus,
    ) -> Result<Vec<Self::Response>, Self::Error> {
        // Simulate async work
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;

        self.batch_count += 1;
        for m in msg {
            self.total += m.0 as i64;
        }
        Ok(vec![])
    }
}

#[async_trait]
impl messagebus::AsyncSynchronizedHandler<GetAsyncBatchState> for AsyncBatchStatefulReceiver {
    type Error = Error;
    type Response = AsyncBatchStateResult;

    async fn handle(
        &mut self,
        _: GetAsyncBatchState,
        _bus: &Bus,
    ) -> Result<Self::Response, Self::Error> {
        Ok(AsyncBatchStateResult {
            total: self.total,
            batch_count: self.batch_count,
        })
    }
}

/// Test AsyncBatchSynchronizedHandler basic functionality
#[tokio::test]
async fn test_async_batch_synchronized_handler_basic() {
    let (b, poller) = Bus::build()
        .register_unsync(AsyncBatchStatefulReceiver::new())
        .subscribe_batch_async::<AsyncBatchStatefulMsg>(
            16,
            SynchronizedBatchedConfig {
                batch_size: 5,
                ..Default::default()
            },
        )
        .subscribe_async::<GetAsyncBatchState>(8, Default::default())
        .done()
        .build();

    // Send 10 messages
    for i in 1..=10 {
        b.send(AsyncBatchStatefulMsg(i)).await.unwrap();
    }

    b.flush_all().await;

    // Get the state
    let result = b
        .request_we::<_, AsyncBatchStateResult, Error>(GetAsyncBatchState, Default::default())
        .await
        .unwrap();

    // Total should be 1+2+...+10 = 55
    assert_eq!(result.total, 55);
    assert!(result.batch_count >= 1);

    b.close().await;
    poller.await;
}

/// Test AsyncBatchSynchronizedHandler with many messages
#[tokio::test]
async fn test_async_batch_synchronized_handler_many_messages() {
    let (b, poller) = Bus::build()
        .register_unsync(AsyncBatchStatefulReceiver::new())
        .subscribe_batch_async::<AsyncBatchStatefulMsg>(
            64,
            SynchronizedBatchedConfig {
                batch_size: 10,
                ..Default::default()
            },
        )
        .subscribe_async::<GetAsyncBatchState>(8, Default::default())
        .done()
        .build();

    // Send 100 messages
    for i in 1..=100 {
        b.send(AsyncBatchStatefulMsg(i)).await.unwrap();
    }

    b.flush_all().await;

    // Get the state
    let result = b
        .request_we::<_, AsyncBatchStateResult, Error>(GetAsyncBatchState, Default::default())
        .await
        .unwrap();

    // Total should be 1+2+...+100 = 5050
    assert_eq!(result.total, 5050);

    b.close().await;
    poller.await;
}

// ============================================================================
// Batch with responses tests
// ============================================================================

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct BatchWithResponse(i32);

#[derive(Debug, Clone, Message)]
struct BatchResponseResult(#[allow(dead_code)] i32);

struct BatchResponseReceiver;

impl BatchHandler<BatchWithResponse> for BatchResponseReceiver {
    type Error = Error;
    type Response = BatchResponseResult;
    type InBatch = Vec<BatchWithResponse>;
    type OutBatch = Vec<BatchResponseResult>;

    fn handle(
        &self,
        msg: Vec<BatchWithResponse>,
        _bus: &Bus,
    ) -> Result<Vec<Self::Response>, Self::Error> {
        // Return doubled values wrapped in BatchResponseResult
        Ok(msg
            .into_iter()
            .map(|m| BatchResponseResult(m.0 * 2))
            .collect())
    }
}

/// Test BatchHandler that returns responses
#[tokio::test]
async fn test_batch_handler_with_responses() {
    let (b, poller) = Bus::build()
        .register(BatchResponseReceiver)
        .subscribe_batch_sync::<BatchWithResponse>(
            16,
            BufferUnorderedBatchedConfig {
                batch_size: 5,
                ..Default::default()
            },
        )
        .done()
        .build();

    // Send messages
    for i in 1..=10 {
        b.send(BatchWithResponse(i)).await.unwrap();
    }

    b.flush_all().await;

    b.close().await;
    poller.await;
}

// ============================================================================
// Edge cases
// ============================================================================

/// Test empty batch handling
#[tokio::test]
async fn test_batch_handler_no_messages() {
    let batches = Arc::new(Mutex::new(Vec::new()));

    let (b, poller) = Bus::build()
        .register(SyncBatchReceiver {
            batches: batches.clone(),
        })
        .subscribe_batch_sync::<BatchSyncMsg>(
            16,
            BufferUnorderedBatchedConfig {
                batch_size: 5,
                ..Default::default()
            },
        )
        .done()
        .build();

    // Don't send any messages, just flush
    b.flush_all().await;

    let lock = batches.lock();
    assert_eq!(lock.len(), 0);

    b.close().await;
    poller.await;
}

/// Test batch size of 1
#[tokio::test]
async fn test_batch_handler_size_one() {
    let batches = Arc::new(Mutex::new(Vec::new()));

    let (b, poller) = Bus::build()
        .register(SyncBatchReceiver {
            batches: batches.clone(),
        })
        .subscribe_batch_sync::<BatchSyncMsg>(
            16,
            BufferUnorderedBatchedConfig {
                batch_size: 1,
                ..Default::default()
            },
        )
        .done()
        .build();

    // Send 5 messages - should create 5 batches of 1
    for i in 1..=5 {
        b.send(BatchSyncMsg(i)).await.unwrap();
    }

    b.flush_all().await;

    let lock = batches.lock();
    let total_items: usize = lock.iter().map(|batch| batch.len()).sum();
    assert_eq!(total_items, 5);

    b.close().await;
    poller.await;
}

/// Test large batch size
#[tokio::test]
async fn test_batch_handler_large_batch_size() {
    let batches = Arc::new(Mutex::new(Vec::new()));

    let (b, poller) = Bus::build()
        .register(SyncBatchReceiver {
            batches: batches.clone(),
        })
        .subscribe_batch_sync::<BatchSyncMsg>(
            256,
            BufferUnorderedBatchedConfig {
                batch_size: 100,
                ..Default::default()
            },
        )
        .done()
        .build();

    // Send 250 messages
    for i in 1..=250 {
        b.send(BatchSyncMsg(i)).await.unwrap();
    }

    b.flush_all().await;

    let lock = batches.lock();
    let total_items: usize = lock.iter().map(|batch| batch.len()).sum();
    assert_eq!(total_items, 250);

    b.close().await;
    poller.await;
}
