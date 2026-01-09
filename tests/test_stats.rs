//! Tests for verifying stats accuracy.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error, receivers, AsyncBatchHandler, AsyncHandler, Bus, Message,
};
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::Barrier;

#[derive(Debug, Error, MbError)]
enum TestError {
    #[error("Test error")]
    Error,
}

impl<M: Message> From<error::Error<M>> for TestError {
    fn from(_err: error::Error<M>) -> Self {
        Self::Error
    }
}

#[derive(Debug, Clone, Error, MbError)]
enum BatchError {
    #[error("Batch error")]
    Error,
}

impl<M: Message> From<error::Error<M>> for BatchError {
    fn from(_err: error::Error<M>) -> Self {
        Self::Error
    }
}

// Message types
#[allow(dead_code)]
#[derive(Debug, Clone, Message)]
#[message(clone)]
struct StatsTestMsg(pub i32);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct BatchStatsMsg(pub i32);

// Handler that blocks until signaled, allowing us to observe in-flight stats
struct BlockingHandler {
    barrier: Arc<Barrier>,
    processed_count: Arc<AtomicUsize>,
}

#[async_trait]
impl AsyncHandler<StatsTestMsg> for BlockingHandler {
    type Error = TestError;
    type Response = ();

    async fn handle(&self, _msg: StatsTestMsg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        // Wait at barrier - this keeps messages "in flight"
        self.barrier.wait().await;
        self.processed_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// Handler that processes immediately (for testing stats after completion)
struct FastHandler {
    processed_count: Arc<AtomicUsize>,
}

#[async_trait]
impl AsyncHandler<StatsTestMsg> for FastHandler {
    type Error = TestError;
    type Response = ();

    async fn handle(&self, _msg: StatsTestMsg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.processed_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// Batch handler for testing batch stats
struct BatchHandler {
    batches: Arc<Mutex<Vec<Vec<i32>>>>,
    barrier: Option<Arc<Barrier>>,
}

#[async_trait]
impl AsyncBatchHandler<BatchStatsMsg> for BatchHandler {
    type Error = BatchError;
    type Response = ();
    type InBatch = Vec<BatchStatsMsg>;
    type OutBatch = Vec<()>;

    async fn handle(
        &self,
        msgs: Vec<BatchStatsMsg>,
        _bus: &Bus,
    ) -> Result<Vec<Self::Response>, Self::Error> {
        self.batches
            .lock()
            .push(msgs.iter().map(|m| m.0).collect());

        if let Some(ref barrier) = self.barrier {
            barrier.wait().await;
        }

        Ok(vec![(); msgs.len()])
    }
}

#[tokio::test]
async fn test_stats_queue_size_increases_on_send() {
    // Use barrier that needs 3 parties: 2 handler tasks + 1 test task
    let barrier = Arc::new(Barrier::new(3));
    let processed = Arc::new(AtomicUsize::new(0));

    let (bus, poller) = Bus::build()
        .register(BlockingHandler {
            barrier: barrier.clone(),
            processed_count: processed.clone(),
        })
        .subscribe_async::<StatsTestMsg>(
            8,
            receivers::BufferUnorderedConfig {
                buffer_size: 8,
                max_parallel: 2,
            },
        )
        .done()
        .build();

    let poller_handle = tokio::spawn(poller);
    bus.ready().await;

    // Initial stats should show queue_size = 0
    let stats: Vec<_> = bus.stats().collect();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].queue_size, 0);

    // Send 2 messages - they'll block at the barrier
    bus.send(StatsTestMsg(1)).await.unwrap();
    bus.send(StatsTestMsg(2)).await.unwrap();

    // Give time for messages to be picked up by handler tasks
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Stats should show 2 messages in flight
    let stats: Vec<_> = bus.stats().collect();
    assert_eq!(stats[0].queue_size, 2, "Expected 2 messages in flight");

    // Release the barrier so handlers can complete
    barrier.wait().await;

    // Wait for processing to complete
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Stats should show 0 messages after completion
    let stats: Vec<_> = bus.stats().collect();
    assert_eq!(
        stats[0].queue_size, 0,
        "Expected 0 messages after processing"
    );

    assert_eq!(processed.load(Ordering::SeqCst), 2);

    bus.close().await;
    poller_handle.await.unwrap();
}

#[tokio::test]
async fn test_stats_queue_capacity_matches_config() {
    let processed = Arc::new(AtomicUsize::new(0));

    let (bus, poller) = Bus::build()
        .register(FastHandler {
            processed_count: processed.clone(),
        })
        .subscribe_async::<StatsTestMsg>(
            16, // buffer size
            receivers::BufferUnorderedConfig {
                buffer_size: 16,
                max_parallel: 4,
            },
        )
        .done()
        .build();

    let poller_handle = tokio::spawn(poller);
    bus.ready().await;

    let stats: Vec<_> = bus.stats().collect();
    assert_eq!(stats.len(), 1);
    assert_eq!(
        stats[0].queue_capacity, 16,
        "Queue capacity should match configured buffer_size"
    );

    bus.close().await;
    poller_handle.await.unwrap();
}

#[tokio::test]
async fn test_stats_decreases_after_flush() {
    let barrier = Arc::new(Barrier::new(5)); // 4 handlers + 1 test
    let processed = Arc::new(AtomicUsize::new(0));

    let (bus, poller) = Bus::build()
        .register(BlockingHandler {
            barrier: barrier.clone(),
            processed_count: processed.clone(),
        })
        .subscribe_async::<StatsTestMsg>(
            8,
            receivers::BufferUnorderedConfig {
                buffer_size: 8,
                max_parallel: 4,
            },
        )
        .done()
        .build();

    let poller_handle = tokio::spawn(poller);
    bus.ready().await;

    // Send 4 messages
    for i in 0..4 {
        bus.send(StatsTestMsg(i)).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    let stats: Vec<_> = bus.stats().collect();
    assert_eq!(stats[0].queue_size, 4, "Expected 4 messages in flight");

    // Release barrier
    barrier.wait().await;

    // Flush to ensure all messages are processed
    bus.flush_all().await;

    let stats: Vec<_> = bus.stats().collect();
    assert_eq!(
        stats[0].queue_size, 0,
        "Expected 0 messages after flush_all"
    );

    bus.close().await;
    poller_handle.await.unwrap();
}

#[tokio::test]
async fn test_stats_batch_handler() {
    let batches = Arc::new(Mutex::new(Vec::new()));

    let (bus, poller) = Bus::build()
        .register(BatchHandler {
            batches: batches.clone(),
            barrier: None,
        })
        .subscribe_batch_async::<BatchStatsMsg>(
            32,
            receivers::BufferUnorderedBatchedConfig {
                buffer_size: 32,
                max_parallel: 2,
                batch_size: 4,
                when_ready: false,
            },
        )
        .done()
        .build();

    let poller_handle = tokio::spawn(poller);
    bus.ready().await;

    // Check initial stats
    let stats: Vec<_> = bus.stats().collect();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].queue_capacity, 32);
    assert_eq!(stats[0].queue_size, 0);

    // Send 8 messages (2 full batches)
    for i in 0..8 {
        bus.send(BatchStatsMsg(i)).await.unwrap();
    }

    bus.flush_all().await;

    // After flush, all messages should be processed
    let stats: Vec<_> = bus.stats().collect();
    assert_eq!(
        stats[0].queue_size, 0,
        "Expected 0 messages after flush_all"
    );

    // Verify batches were processed correctly
    {
        let batches_lock = batches.lock();
        assert_eq!(batches_lock.len(), 2, "Expected 2 batches");
        assert_eq!(batches_lock[0].len(), 4, "First batch should have 4 items");
        assert_eq!(batches_lock[1].len(), 4, "Second batch should have 4 items");
    }

    bus.close().await;
    poller_handle.await.unwrap();
}

#[tokio::test]
async fn test_stats_has_queue_flag() {
    let processed = Arc::new(AtomicUsize::new(0));

    let (bus, poller) = Bus::build()
        .register(FastHandler {
            processed_count: processed.clone(),
        })
        .subscribe_async::<StatsTestMsg>(8, Default::default())
        .done()
        .build();

    let poller_handle = tokio::spawn(poller);
    bus.ready().await;

    let stats: Vec<_> = bus.stats().collect();
    assert!(stats[0].has_queue, "BufferUnordered should have a queue");

    bus.close().await;
    poller_handle.await.unwrap();
}

#[tokio::test]
#[allow(dead_code)]
async fn test_stats_multiple_message_types() {
    #[derive(Debug, Clone, Message)]
    #[message(clone)]
    struct MsgTypeA(i32);

    #[derive(Debug, Clone, Message)]
    #[message(clone)]
    struct MsgTypeB(i32);

    struct HandlerA;
    struct HandlerB;

    #[async_trait]
    impl AsyncHandler<MsgTypeA> for HandlerA {
        type Error = TestError;
        type Response = ();

        async fn handle(&self, _msg: MsgTypeA, _bus: &Bus) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl AsyncHandler<MsgTypeB> for HandlerB {
        type Error = TestError;
        type Response = ();

        async fn handle(&self, _msg: MsgTypeB, _bus: &Bus) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    let (bus, poller) = Bus::build()
        .register(HandlerA)
        .subscribe_async::<MsgTypeA>(
            4,
            receivers::BufferUnorderedConfig {
                buffer_size: 4,
                max_parallel: 2,
            },
        )
        .done()
        .register(HandlerB)
        .subscribe_async::<MsgTypeB>(
            8,
            receivers::BufferUnorderedConfig {
                buffer_size: 8,
                max_parallel: 4,
            },
        )
        .done()
        .build();

    let poller_handle = tokio::spawn(poller);
    bus.ready().await;

    let stats: Vec<_> = bus.stats().collect();
    assert_eq!(stats.len(), 2, "Should have stats for 2 receivers");

    // Find stats for each type
    let stats_a = stats
        .iter()
        .find(|s| s.msg_type_tag.contains("MsgTypeA"))
        .expect("Should have stats for MsgTypeA");
    let stats_b = stats
        .iter()
        .find(|s| s.msg_type_tag.contains("MsgTypeB"))
        .expect("Should have stats for MsgTypeB");

    assert_eq!(stats_a.queue_capacity, 4);
    assert_eq!(stats_b.queue_capacity, 8);

    bus.close().await;
    poller_handle.await.unwrap();
}
