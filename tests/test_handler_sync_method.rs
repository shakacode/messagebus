//! Tests for the handler sync() method across all receiver types.
//!
//! The sync() method is called when Bus::sync_all() is invoked. These tests verify
//! that the sync() method is correctly called for each handler type.

use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};

use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error,
    receivers::{BufferUnorderedBatchedConfig, BufferUnorderedConfig, SynchronizedBatchedConfig},
    AsyncBatchHandler, AsyncBatchSynchronizedHandler, AsyncHandler, AsyncSynchronizedHandler,
    BatchHandler, BatchSynchronizedHandler, Bus, Handler, Message, SynchronizedHandler,
};
use thiserror::Error;

#[derive(Debug, Error, Clone, MbError)]
enum TestError {
    #[error("Error({0})")]
    Error(Arc<anyhow::Error>),
}

impl<M: Message> From<error::Error<M>> for TestError {
    fn from(err: error::Error<M>) -> Self {
        Self::Error(Arc::new(err.into()))
    }
}

// ============================================================================
// Handler (sync, thread-safe) - BufferUnorderedSync
// ============================================================================

#[allow(dead_code)]
#[derive(Debug, Clone, Message)]
#[message(clone)]
struct HandlerMsg(i32);

struct TestHandler {
    handle_count: Arc<AtomicU32>,
    sync_called: Arc<AtomicBool>,
    sync_called_after_handle: Arc<AtomicBool>,
}

impl Handler<HandlerMsg> for TestHandler {
    type Error = TestError;
    type Response = ();

    fn handle(&self, _msg: HandlerMsg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.handle_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        // Verify that handle was called before sync
        if self.handle_count.load(Ordering::SeqCst) > 0 {
            self.sync_called_after_handle.store(true, Ordering::SeqCst);
        }
        self.sync_called.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn test_handler_sync_method_called() {
    let handle_count = Arc::new(AtomicU32::new(0));
    let sync_called = Arc::new(AtomicBool::new(false));
    let sync_called_after_handle = Arc::new(AtomicBool::new(false));

    let (bus, poller) = Bus::build()
        .register(TestHandler {
            handle_count: handle_count.clone(),
            sync_called: sync_called.clone(),
            sync_called_after_handle: sync_called_after_handle.clone(),
        })
        .subscribe_sync::<HandlerMsg>(8, BufferUnorderedConfig::default())
        .done()
        .build();

    // Send some messages
    for i in 1..=5 {
        bus.send(HandlerMsg(i)).await.unwrap();
    }

    // Flush to ensure all messages are processed
    bus.flush_all().await;

    // Call sync_all
    bus.sync_all().await;

    // Verify sync was called
    assert!(sync_called.load(Ordering::SeqCst), "sync() was not called");
    assert!(
        sync_called_after_handle.load(Ordering::SeqCst),
        "sync() was called before handle()"
    );
    assert_eq!(handle_count.load(Ordering::SeqCst), 5);

    bus.close().await;
    poller.await;
}

// ============================================================================
// AsyncHandler - BufferUnorderedAsync
// ============================================================================

#[allow(dead_code)]
#[derive(Debug, Clone, Message)]
#[message(clone)]
struct AsyncHandlerMsg(i32);

struct TestAsyncHandler {
    handle_count: Arc<AtomicU32>,
    sync_called: Arc<AtomicBool>,
}

#[async_trait]
impl AsyncHandler<AsyncHandlerMsg> for TestAsyncHandler {
    type Error = TestError;
    type Response = ();

    async fn handle(
        &self,
        _msg: AsyncHandlerMsg,
        _bus: &Bus,
    ) -> Result<Self::Response, Self::Error> {
        self.handle_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        self.sync_called.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn test_async_handler_sync_method_called() {
    let handle_count = Arc::new(AtomicU32::new(0));
    let sync_called = Arc::new(AtomicBool::new(false));

    let (bus, poller) = Bus::build()
        .register(TestAsyncHandler {
            handle_count: handle_count.clone(),
            sync_called: sync_called.clone(),
        })
        .subscribe_async::<AsyncHandlerMsg>(8, BufferUnorderedConfig::default())
        .done()
        .build();

    for i in 1..=5 {
        bus.send(AsyncHandlerMsg(i)).await.unwrap();
    }

    bus.flush_all().await;
    bus.sync_all().await;

    assert!(sync_called.load(Ordering::SeqCst), "sync() was not called");
    assert_eq!(handle_count.load(Ordering::SeqCst), 5);

    bus.close().await;
    poller.await;
}

// ============================================================================
// SynchronizedHandler - SynchronizedSync
// ============================================================================

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct SyncHandlerMsg(i32);

struct TestSynchronizedHandler {
    state: i64,
    sync_called: bool,
    sync_state_snapshot: i64,
}

impl TestSynchronizedHandler {
    fn new() -> Self {
        Self {
            state: 0,
            sync_called: false,
            sync_state_snapshot: 0,
        }
    }
}

impl SynchronizedHandler<SyncHandlerMsg> for TestSynchronizedHandler {
    type Error = TestError;
    type Response = ();

    fn handle(&mut self, msg: SyncHandlerMsg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.state += msg.0 as i64;
        Ok(())
    }

    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        self.sync_called = true;
        self.sync_state_snapshot = self.state;
        Ok(())
    }
}

#[derive(Debug, Clone, Message)]
struct GetSyncState;

#[derive(Debug, Clone, Message)]
struct SyncStateResult {
    sync_called: bool,
    sync_state_snapshot: i64,
    current_state: i64,
}

impl SynchronizedHandler<GetSyncState> for TestSynchronizedHandler {
    type Error = TestError;
    type Response = SyncStateResult;

    fn handle(&mut self, _: GetSyncState, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(SyncStateResult {
            sync_called: self.sync_called,
            sync_state_snapshot: self.sync_state_snapshot,
            current_state: self.state,
        })
    }
}

#[tokio::test]
async fn test_synchronized_handler_sync_method_called() {
    let (bus, poller) = Bus::build()
        .register_unsync(TestSynchronizedHandler::new())
        .subscribe_sync::<SyncHandlerMsg>(8, Default::default())
        .subscribe_sync::<GetSyncState>(8, Default::default())
        .done()
        .build();

    // Send messages (1+2+3+4+5 = 15)
    for i in 1..=5 {
        bus.send(SyncHandlerMsg(i)).await.unwrap();
    }

    bus.flush_all().await;
    bus.sync_all().await;

    // Get state to verify sync was called
    let result = bus
        .request_we::<_, SyncStateResult, TestError>(GetSyncState, Default::default())
        .await
        .unwrap();

    assert!(result.sync_called, "sync() was not called");
    assert_eq!(
        result.sync_state_snapshot, 15,
        "sync() captured wrong state"
    );
    assert_eq!(result.current_state, 15);

    bus.close().await;
    poller.await;
}

// ============================================================================
// AsyncSynchronizedHandler - SynchronizedAsync
// ============================================================================

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct AsyncSyncHandlerMsg(i32);

struct TestAsyncSynchronizedHandler {
    state: i64,
    sync_called: bool,
}

impl TestAsyncSynchronizedHandler {
    fn new() -> Self {
        Self {
            state: 0,
            sync_called: false,
        }
    }
}

#[async_trait]
impl AsyncSynchronizedHandler<AsyncSyncHandlerMsg> for TestAsyncSynchronizedHandler {
    type Error = TestError;
    type Response = ();

    async fn handle(
        &mut self,
        msg: AsyncSyncHandlerMsg,
        _bus: &Bus,
    ) -> Result<Self::Response, Self::Error> {
        self.state += msg.0 as i64;
        Ok(())
    }

    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        self.sync_called = true;
        Ok(())
    }
}

#[derive(Debug, Clone, Message)]
struct GetAsyncSyncState;

#[derive(Debug, Clone, Message)]
struct AsyncSyncStateResult {
    sync_called: bool,
    state: i64,
}

#[async_trait]
impl AsyncSynchronizedHandler<GetAsyncSyncState> for TestAsyncSynchronizedHandler {
    type Error = TestError;
    type Response = AsyncSyncStateResult;

    async fn handle(
        &mut self,
        _: GetAsyncSyncState,
        _bus: &Bus,
    ) -> Result<Self::Response, Self::Error> {
        Ok(AsyncSyncStateResult {
            sync_called: self.sync_called,
            state: self.state,
        })
    }
}

#[tokio::test]
async fn test_async_synchronized_handler_sync_method_called() {
    let (bus, poller) = Bus::build()
        .register_unsync(TestAsyncSynchronizedHandler::new())
        .subscribe_async::<AsyncSyncHandlerMsg>(8, Default::default())
        .subscribe_async::<GetAsyncSyncState>(8, Default::default())
        .done()
        .build();

    for i in 1..=5 {
        bus.send(AsyncSyncHandlerMsg(i)).await.unwrap();
    }

    bus.flush_all().await;
    bus.sync_all().await;

    let result = bus
        .request_we::<_, AsyncSyncStateResult, TestError>(GetAsyncSyncState, Default::default())
        .await
        .unwrap();

    assert!(result.sync_called, "sync() was not called");
    assert_eq!(result.state, 15);

    bus.close().await;
    poller.await;
}

// ============================================================================
// BatchHandler - BufferUnorderedBatchedSync
// ============================================================================

#[allow(dead_code)]
#[derive(Debug, Clone, Message)]
#[message(clone)]
struct BatchHandlerMsg(i32);

struct TestBatchHandler {
    batch_count: Arc<AtomicU32>,
    sync_called: Arc<AtomicBool>,
}

impl BatchHandler<BatchHandlerMsg> for TestBatchHandler {
    type Error = TestError;
    type Response = ();
    type InBatch = Vec<BatchHandlerMsg>;
    type OutBatch = Vec<()>;

    fn handle(
        &self,
        msgs: Vec<BatchHandlerMsg>,
        _bus: &Bus,
    ) -> Result<Vec<Self::Response>, Self::Error> {
        self.batch_count.fetch_add(1, Ordering::SeqCst);
        Ok(vec![(); msgs.len()])
    }

    fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        self.sync_called.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn test_batch_handler_sync_method_called() {
    let batch_count = Arc::new(AtomicU32::new(0));
    let sync_called = Arc::new(AtomicBool::new(false));

    let (bus, poller) = Bus::build()
        .register(TestBatchHandler {
            batch_count: batch_count.clone(),
            sync_called: sync_called.clone(),
        })
        .subscribe_batch_sync::<BatchHandlerMsg>(
            16,
            BufferUnorderedBatchedConfig {
                batch_size: 3,
                ..Default::default()
            },
        )
        .done()
        .build();

    for i in 1..=10 {
        bus.send(BatchHandlerMsg(i)).await.unwrap();
    }

    bus.flush_all().await;
    bus.sync_all().await;

    assert!(sync_called.load(Ordering::SeqCst), "sync() was not called");
    assert!(
        batch_count.load(Ordering::SeqCst) >= 1,
        "no batches processed"
    );

    bus.close().await;
    poller.await;
}

// ============================================================================
// AsyncBatchHandler - BufferUnorderedBatchedAsync
// ============================================================================

#[allow(dead_code)]
#[derive(Debug, Clone, Message)]
#[message(clone)]
struct AsyncBatchHandlerMsg(i32);

struct TestAsyncBatchHandler {
    batch_count: Arc<AtomicU32>,
    sync_called: Arc<AtomicBool>,
}

#[async_trait]
impl AsyncBatchHandler<AsyncBatchHandlerMsg> for TestAsyncBatchHandler {
    type Error = TestError;
    type Response = ();
    type InBatch = Vec<AsyncBatchHandlerMsg>;
    type OutBatch = Vec<()>;

    async fn handle(
        &self,
        msgs: Vec<AsyncBatchHandlerMsg>,
        _bus: &Bus,
    ) -> Result<Vec<Self::Response>, Self::Error> {
        self.batch_count.fetch_add(1, Ordering::SeqCst);
        Ok(vec![(); msgs.len()])
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        self.sync_called.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn test_async_batch_handler_sync_method_called() {
    let batch_count = Arc::new(AtomicU32::new(0));
    let sync_called = Arc::new(AtomicBool::new(false));

    let (bus, poller) = Bus::build()
        .register(TestAsyncBatchHandler {
            batch_count: batch_count.clone(),
            sync_called: sync_called.clone(),
        })
        .subscribe_batch_async::<AsyncBatchHandlerMsg>(
            16,
            BufferUnorderedBatchedConfig {
                batch_size: 3,
                ..Default::default()
            },
        )
        .done()
        .build();

    for i in 1..=10 {
        bus.send(AsyncBatchHandlerMsg(i)).await.unwrap();
    }

    bus.flush_all().await;
    bus.sync_all().await;

    assert!(sync_called.load(Ordering::SeqCst), "sync() was not called");
    assert!(
        batch_count.load(Ordering::SeqCst) >= 1,
        "no batches processed"
    );

    bus.close().await;
    poller.await;
}

// ============================================================================
// BatchSynchronizedHandler - SynchronizedBatchedSync
// ============================================================================

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct BatchSyncHandlerMsg(i32);

struct TestBatchSynchronizedHandler {
    total: i64,
    sync_called: bool,
}

impl TestBatchSynchronizedHandler {
    fn new() -> Self {
        Self {
            total: 0,
            sync_called: false,
        }
    }
}

impl BatchSynchronizedHandler<BatchSyncHandlerMsg> for TestBatchSynchronizedHandler {
    type Error = TestError;
    type Response = ();
    type InBatch = Vec<BatchSyncHandlerMsg>;
    type OutBatch = Vec<()>;

    fn handle(
        &mut self,
        msgs: Vec<BatchSyncHandlerMsg>,
        _bus: &Bus,
    ) -> Result<Vec<Self::Response>, Self::Error> {
        for msg in &msgs {
            self.total += msg.0 as i64;
        }
        Ok(vec![(); msgs.len()])
    }

    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        self.sync_called = true;
        Ok(())
    }
}

#[derive(Debug, Clone, Message)]
struct GetBatchSyncState;

#[derive(Debug, Clone, Message)]
struct BatchSyncStateResult {
    sync_called: bool,
    total: i64,
}

impl SynchronizedHandler<GetBatchSyncState> for TestBatchSynchronizedHandler {
    type Error = TestError;
    type Response = BatchSyncStateResult;

    fn handle(&mut self, _: GetBatchSyncState, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(BatchSyncStateResult {
            sync_called: self.sync_called,
            total: self.total,
        })
    }
}

#[tokio::test]
async fn test_batch_synchronized_handler_sync_method_called() {
    let (bus, poller) = Bus::build()
        .register_unsync(TestBatchSynchronizedHandler::new())
        .subscribe_batch_sync::<BatchSyncHandlerMsg>(
            16,
            SynchronizedBatchedConfig {
                batch_size: 3,
                ..Default::default()
            },
        )
        .subscribe_sync::<GetBatchSyncState>(8, Default::default())
        .done()
        .build();

    // 1+2+3+4+5+6+7+8+9+10 = 55
    for i in 1..=10 {
        bus.send(BatchSyncHandlerMsg(i)).await.unwrap();
    }

    bus.flush_all().await;
    bus.sync_all().await;

    let result = bus
        .request_we::<_, BatchSyncStateResult, TestError>(GetBatchSyncState, Default::default())
        .await
        .unwrap();

    assert!(result.sync_called, "sync() was not called");
    assert_eq!(result.total, 55);

    bus.close().await;
    poller.await;
}

// ============================================================================
// AsyncBatchSynchronizedHandler - SynchronizedBatchedAsync
// ============================================================================

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct AsyncBatchSyncHandlerMsg(i32);

struct TestAsyncBatchSynchronizedHandler {
    total: i64,
    sync_called: bool,
}

impl TestAsyncBatchSynchronizedHandler {
    fn new() -> Self {
        Self {
            total: 0,
            sync_called: false,
        }
    }
}

#[async_trait]
impl AsyncBatchSynchronizedHandler<AsyncBatchSyncHandlerMsg> for TestAsyncBatchSynchronizedHandler {
    type Error = TestError;
    type Response = ();
    type InBatch = Vec<AsyncBatchSyncHandlerMsg>;
    type OutBatch = Vec<()>;

    async fn handle(
        &mut self,
        msgs: Vec<AsyncBatchSyncHandlerMsg>,
        _bus: &Bus,
    ) -> Result<Vec<Self::Response>, Self::Error> {
        for msg in &msgs {
            self.total += msg.0 as i64;
        }
        Ok(vec![(); msgs.len()])
    }

    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        self.sync_called = true;
        Ok(())
    }
}

#[derive(Debug, Clone, Message)]
struct GetAsyncBatchSyncState;

#[derive(Debug, Clone, Message)]
struct AsyncBatchSyncStateResult {
    sync_called: bool,
    total: i64,
}

#[async_trait]
impl AsyncSynchronizedHandler<GetAsyncBatchSyncState> for TestAsyncBatchSynchronizedHandler {
    type Error = TestError;
    type Response = AsyncBatchSyncStateResult;

    async fn handle(
        &mut self,
        _: GetAsyncBatchSyncState,
        _bus: &Bus,
    ) -> Result<Self::Response, Self::Error> {
        Ok(AsyncBatchSyncStateResult {
            sync_called: self.sync_called,
            total: self.total,
        })
    }
}

#[tokio::test]
async fn test_async_batch_synchronized_handler_sync_method_called() {
    let (bus, poller) = Bus::build()
        .register_unsync(TestAsyncBatchSynchronizedHandler::new())
        .subscribe_batch_async::<AsyncBatchSyncHandlerMsg>(
            16,
            SynchronizedBatchedConfig {
                batch_size: 3,
                ..Default::default()
            },
        )
        .subscribe_async::<GetAsyncBatchSyncState>(8, Default::default())
        .done()
        .build();

    for i in 1..=10 {
        bus.send(AsyncBatchSyncHandlerMsg(i)).await.unwrap();
    }

    bus.flush_all().await;
    bus.sync_all().await;

    let result = bus
        .request_we::<_, AsyncBatchSyncStateResult, TestError>(
            GetAsyncBatchSyncState,
            Default::default(),
        )
        .await
        .unwrap();

    assert!(result.sync_called, "sync() was not called");
    assert_eq!(result.total, 55);

    bus.close().await;
    poller.await;
}

// ============================================================================
// Test sync() is called after all in-flight messages complete
// ============================================================================

#[allow(dead_code)]
#[derive(Debug, Clone, Message)]
#[message(clone)]
struct SlowMsg(i32);

struct SlowHandler {
    processed: Arc<AtomicU32>,
    sync_called_with_count: Arc<AtomicU32>,
}

#[async_trait]
impl AsyncHandler<SlowMsg> for SlowHandler {
    type Error = TestError;
    type Response = ();

    async fn handle(&self, _msg: SlowMsg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        // Simulate slow processing
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        self.processed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        // Record how many messages were processed when sync was called
        self.sync_called_with_count
            .store(self.processed.load(Ordering::SeqCst), Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn test_sync_waits_for_inflight_messages() {
    let processed = Arc::new(AtomicU32::new(0));
    let sync_called_with_count = Arc::new(AtomicU32::new(0));

    let (bus, poller) = Bus::build()
        .register(SlowHandler {
            processed: processed.clone(),
            sync_called_with_count: sync_called_with_count.clone(),
        })
        .subscribe_async::<SlowMsg>(
            16,
            BufferUnorderedConfig {
                max_parallel: 4,
                ..Default::default()
            },
        )
        .done()
        .build();

    // Send messages
    for i in 1..=8 {
        bus.send(SlowMsg(i)).await.unwrap();
    }

    // Flush waits for messages to be processed
    bus.flush_all().await;

    // Sync should see all messages processed
    bus.sync_all().await;

    // Verify sync was called after all messages were processed
    assert_eq!(
        sync_called_with_count.load(Ordering::SeqCst),
        8,
        "sync() was called before all messages were processed"
    );

    bus.close().await;
    poller.await;
}
