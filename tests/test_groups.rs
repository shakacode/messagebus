use async_trait::async_trait;
use messagebus::derive::Message;
use messagebus::error::GenericError;
use messagebus::{AsyncHandler, Bus, GroupId};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// A message with an explicit group_id derived from a field.
#[derive(Debug, Clone, Message)]
#[group_id(self.job_id)]
struct JobMessage {
    job_id: i64,
    #[allow(dead_code)]
    data: String,
}

/// A message without a group_id (defaults to None).
#[derive(Debug, Clone, Message)]
struct UngroupedMessage {
    #[allow(dead_code)]
    data: String,
}

/// Handler that tracks which group_id messages have.
struct GroupTrackingHandler {
    messages_received: Arc<AtomicI64>,
}

#[async_trait]
impl AsyncHandler<JobMessage> for GroupTrackingHandler {
    type Error = GenericError;
    type Response = ();

    async fn handle(&self, msg: JobMessage, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        // Verify the current group_id matches the message's group_id
        let current = Bus::current_group_id();
        assert_eq!(
            current,
            Some(msg.job_id),
            "Task-local group_id should match message's group_id"
        );
        self.messages_received.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<UngroupedMessage> for GroupTrackingHandler {
    type Error = GenericError;
    type Response = ();

    async fn handle(
        &self,
        _msg: UngroupedMessage,
        _bus: &Bus,
    ) -> Result<Self::Response, Self::Error> {
        // For ungrouped messages, current_group_id should be None
        let current = Bus::current_group_id();
        assert_eq!(
            current, None,
            "Task-local group_id should be None for ungrouped messages"
        );
        self.messages_received.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Handler that sends nested messages to test group propagation.
#[allow(dead_code)]
struct NestedSendHandler;

/// A message that triggers nested send.
#[allow(dead_code)]
#[derive(Debug, Clone, Message)]
#[group_id(self.job_id)]
struct ParentMessage {
    job_id: i64,
}

/// A child message that inherits group from parent context.
#[allow(dead_code)]
#[derive(Debug, Clone, Message)]
struct ChildMessage {
    from_parent: bool,
}

#[async_trait]
impl AsyncHandler<ParentMessage> for NestedSendHandler {
    type Error = GenericError;
    type Response = ();

    async fn handle(&self, msg: ParentMessage, bus: &Bus) -> Result<Self::Response, Self::Error> {
        // Current group_id should be set from the parent message
        assert_eq!(
            Bus::current_group_id(),
            Some(msg.job_id),
            "Parent handler should have group_id set"
        );

        // Send a child message - it should inherit the group_id from context
        // Note: We don't use ? here to avoid complex error conversion
        let _ = bus.send(ChildMessage { from_parent: true }).await;
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<ChildMessage> for NestedSendHandler {
    type Error = GenericError;
    type Response = ();

    async fn handle(&self, msg: ChildMessage, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        if msg.from_parent {
            // Child message sent from parent handler should inherit group_id
            // Note: This depends on the task-local propagation working correctly
            // The child handler runs in its own task, so it should have the group_id propagated
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_message_group_id_derived_from_field() {
    let messages_received = Arc::new(AtomicI64::new(0));
    let handler = GroupTrackingHandler {
        messages_received: messages_received.clone(),
    };

    let (bus, poller) = Bus::build()
        .register(handler)
        .subscribe_async::<JobMessage>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    // Send messages with different group_ids
    bus.send(JobMessage {
        job_id: 100,
        data: "job 100".into(),
    })
    .await
    .unwrap();

    bus.send(JobMessage {
        job_id: 200,
        data: "job 200".into(),
    })
    .await
    .unwrap();

    bus.flush_all().await;
    bus.close().await;

    assert_eq!(messages_received.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_ungrouped_message_has_none_group_id() {
    let messages_received = Arc::new(AtomicI64::new(0));
    let handler = GroupTrackingHandler {
        messages_received: messages_received.clone(),
    };

    let (bus, poller) = Bus::build()
        .register(handler)
        .subscribe_async::<UngroupedMessage>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    bus.send(UngroupedMessage {
        data: "no group".into(),
    })
    .await
    .unwrap();

    bus.flush_all().await;
    bus.close().await;

    assert_eq!(messages_received.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_group_registry_tracking() {
    struct CountingHandler;

    #[async_trait]
    impl AsyncHandler<JobMessage> for CountingHandler {
        type Error = GenericError;
        type Response = ();

        async fn handle(
            &self,
            _msg: JobMessage,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            // Simulate some work
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            Ok(())
        }
    }

    let (bus, poller) = Bus::build()
        .register(CountingHandler)
        .subscribe_async::<JobMessage>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id: GroupId = 42;

    // Initially the group should be idle
    assert!(bus.is_group_idle(job_id));

    // Send some messages
    for i in 0..5 {
        bus.send(JobMessage {
            job_id,
            data: format!("msg {}", i),
        })
        .await
        .unwrap();
    }

    // Flush and wait for completion
    bus.flush_all().await;

    // After flush, all messages should be processed
    assert!(bus.is_group_idle(job_id));

    bus.close().await;
}

#[tokio::test]
async fn test_current_group_id_outside_handler() {
    // Outside of a handler, current_group_id should be None
    assert_eq!(Bus::current_group_id(), None);
}

// ============================================================================
// Batch handler group_id tests
// ============================================================================

use messagebus::derive::Error as MbError;
use messagebus::receivers::BufferUnorderedBatchedConfig;
use messagebus::BatchHandler;
use parking_lot::Mutex;
use std::collections::HashMap;
use thiserror::Error;

/// Cloneable error type for batch handlers.
#[derive(Debug, Error, Clone, MbError)]
#[allow(dead_code)]
enum BatchError {
    #[error("Batch error: {0}")]
    Error(String),
}

/// A message with a group_id for batch testing.
#[derive(Debug, Clone, Message)]
#[message(clone)]
#[group_id(self.job_id)]
struct BatchJobMessage {
    job_id: i64,
    value: i32,
}

/// Handler that tracks which group_ids are seen in each batch.
/// This verifies that batches contain only messages from the same group.
struct BatchGroupTrackingHandler {
    /// Maps group_id -> list of batches (each batch is a list of values)
    batches_by_group: Arc<Mutex<HashMap<Option<GroupId>, Vec<Vec<i32>>>>>,
    /// Tracks the group_id observed via Bus::current_group_id() for each batch
    observed_group_ids: Arc<Mutex<Vec<Option<GroupId>>>>,
}

impl BatchHandler<BatchJobMessage> for BatchGroupTrackingHandler {
    type Error = BatchError;
    type Response = ();
    type InBatch = Vec<BatchJobMessage>;
    type OutBatch = Vec<()>;

    fn handle(
        &self,
        msgs: Vec<BatchJobMessage>,
        _bus: &Bus,
    ) -> Result<Vec<Self::Response>, Self::Error> {
        // Record the current group_id from task-local storage
        let current_group = Bus::current_group_id();
        self.observed_group_ids.lock().push(current_group);

        // Extract values and verify all messages have the same group_id
        let values: Vec<i32> = msgs.iter().map(|m| m.value).collect();
        let group_ids: Vec<i64> = msgs.iter().map(|m| m.job_id).collect();

        // All messages in a batch should have the same group_id
        if !group_ids.is_empty() {
            let first = group_ids[0];
            for gid in &group_ids {
                assert_eq!(
                    *gid, first,
                    "All messages in a batch should have the same group_id"
                );
            }
        }

        // Record this batch under the group_id
        let group_key = group_ids.first().copied();
        self.batches_by_group
            .lock()
            .entry(group_key)
            .or_default()
            .push(values);

        Ok(vec![(); msgs.len()])
    }
}

/// Test that batch handler receives correct group_id via Bus::current_group_id()
#[tokio::test]
async fn test_batch_handler_group_id_propagation() {
    let batches_by_group = Arc::new(Mutex::new(HashMap::new()));
    let observed_group_ids = Arc::new(Mutex::new(Vec::new()));

    let handler = BatchGroupTrackingHandler {
        batches_by_group: batches_by_group.clone(),
        observed_group_ids: observed_group_ids.clone(),
    };

    let (bus, poller) = Bus::build()
        .register(handler)
        .subscribe_batch_sync::<BatchJobMessage>(
            32,
            BufferUnorderedBatchedConfig {
                batch_size: 3,
                ..Default::default()
            },
        )
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    // Send messages for group 100
    for i in 1..=6 {
        bus.send(BatchJobMessage {
            job_id: 100,
            value: i,
        })
        .await
        .unwrap();
    }

    bus.flush_all().await;

    // Verify all observed group_ids were Some(100)
    {
        let observed = observed_group_ids.lock();
        for gid in observed.iter() {
            assert_eq!(
                *gid,
                Some(100),
                "Batch handler should see group_id 100 via Bus::current_group_id()"
            );
        }
    }

    // Verify all 6 messages were processed
    {
        let batches = batches_by_group.lock();
        let group_100_batches = batches
            .get(&Some(100))
            .expect("Should have batches for group 100");
        let total: usize = group_100_batches.iter().map(|b| b.len()).sum();
        assert_eq!(total, 6, "All 6 messages should be processed");
    }

    bus.close().await;
}

/// Test that messages from different groups are batched separately.
#[tokio::test]
async fn test_batch_handler_per_group_batching() {
    let batches_by_group = Arc::new(Mutex::new(HashMap::new()));
    let observed_group_ids = Arc::new(Mutex::new(Vec::new()));

    let handler = BatchGroupTrackingHandler {
        batches_by_group: batches_by_group.clone(),
        observed_group_ids: observed_group_ids.clone(),
    };

    let (bus, poller) = Bus::build()
        .register(handler)
        .subscribe_batch_sync::<BatchJobMessage>(
            32,
            BufferUnorderedBatchedConfig {
                batch_size: 3,
                ..Default::default()
            },
        )
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    // Interleave messages from two different groups
    for i in 1..=6 {
        bus.send(BatchJobMessage {
            job_id: 100,
            value: i,
        })
        .await
        .unwrap();

        bus.send(BatchJobMessage {
            job_id: 200,
            value: i * 10,
        })
        .await
        .unwrap();
    }

    bus.flush_all().await;

    // Verify messages were batched separately by group
    {
        let batches = batches_by_group.lock();

        // Group 100 should have its own batches
        let group_100_batches = batches
            .get(&Some(100))
            .expect("Should have batches for group 100");
        let group_100_values: Vec<i32> = group_100_batches.iter().flatten().copied().collect();
        assert_eq!(group_100_values.len(), 6);
        for i in 1..=6 {
            assert!(
                group_100_values.contains(&i),
                "Group 100 should contain value {i}"
            );
        }

        // Group 200 should have its own batches
        let group_200_batches = batches
            .get(&Some(200))
            .expect("Should have batches for group 200");
        let group_200_values: Vec<i32> = group_200_batches.iter().flatten().copied().collect();
        assert_eq!(group_200_values.len(), 6);
        for i in 1..=6 {
            assert!(
                group_200_values.contains(&(i * 10)),
                "Group 200 should contain value {}",
                i * 10
            );
        }
    }

    bus.close().await;
}

/// Test that group tracking works correctly with batch handlers.
#[tokio::test]
async fn test_batch_handler_group_tracking() {
    let batches_by_group = Arc::new(Mutex::new(HashMap::new()));
    let observed_group_ids = Arc::new(Mutex::new(Vec::new()));

    let handler = BatchGroupTrackingHandler {
        batches_by_group: batches_by_group.clone(),
        observed_group_ids: observed_group_ids.clone(),
    };

    let (bus, poller) = Bus::build()
        .register(handler)
        .subscribe_batch_sync::<BatchJobMessage>(
            32,
            BufferUnorderedBatchedConfig {
                batch_size: 2,
                ..Default::default()
            },
        )
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id: GroupId = 42;

    // Initially the group should be idle
    assert!(bus.is_group_idle(job_id));

    // Send messages
    for i in 1..=5 {
        bus.send(BatchJobMessage { job_id, value: i })
            .await
            .unwrap();
    }

    // Flush and wait for completion
    bus.flush_all().await;

    // After flush, the group should be idle
    assert!(
        bus.is_group_idle(job_id),
        "Group should be idle after flush_all"
    );

    // Verify all messages were processed
    {
        let batches = batches_by_group.lock();
        let group_batches = batches.get(&Some(job_id)).expect("Should have batches");
        let total: usize = group_batches.iter().map(|b| b.len()).sum();
        assert_eq!(total, 5, "All 5 messages should be processed");
    }

    bus.close().await;
}

/// Test that multiple groups can be flushed independently with batch handlers.
#[tokio::test]
async fn test_batch_handler_multiple_groups_independent() {
    use std::sync::atomic::AtomicU64;

    let processed_count = Arc::new(AtomicU64::new(0));
    let processed_count_clone = processed_count.clone();

    struct CountingBatchHandler {
        count: Arc<AtomicU64>,
    }

    impl BatchHandler<BatchJobMessage> for CountingBatchHandler {
        type Error = BatchError;
        type Response = ();
        type InBatch = Vec<BatchJobMessage>;
        type OutBatch = Vec<()>;

        fn handle(
            &self,
            msgs: Vec<BatchJobMessage>,
            _bus: &Bus,
        ) -> Result<Vec<Self::Response>, Self::Error> {
            // Simulate some work
            std::thread::sleep(std::time::Duration::from_millis(10));
            self.count.fetch_add(msgs.len() as u64, Ordering::SeqCst);
            Ok(vec![(); msgs.len()])
        }
    }

    let (bus, poller) = Bus::build()
        .register(CountingBatchHandler {
            count: processed_count_clone,
        })
        .subscribe_batch_sync::<BatchJobMessage>(
            32,
            BufferUnorderedBatchedConfig {
                batch_size: 3,
                ..Default::default()
            },
        )
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id_1: GroupId = 100;
    let job_id_2: GroupId = 200;

    // Send messages to two groups
    for i in 1..=6 {
        bus.send(BatchJobMessage {
            job_id: job_id_1,
            value: i,
        })
        .await
        .unwrap();
    }

    for i in 1..=3 {
        bus.send(BatchJobMessage {
            job_id: job_id_2,
            value: i,
        })
        .await
        .unwrap();
    }

    // Flush all to process any remaining buffered messages
    bus.flush_all().await;

    // Both groups should be idle after flush_all
    assert!(bus.is_group_idle(job_id_1), "Group 1 should be idle");
    assert!(bus.is_group_idle(job_id_2), "Group 2 should be idle");

    // All 9 messages should be processed (6 + 3)
    assert_eq!(processed_count.load(Ordering::SeqCst), 9);

    bus.close().await;
}

/// Test flush_group with batch handlers when batches are exactly full.
///
/// When the number of messages equals the batch size, `flush_group` should
/// work correctly since all messages are processed immediately.
#[tokio::test]
async fn test_flush_group_with_full_batches() {
    use std::sync::atomic::AtomicU64;

    let processed_count = Arc::new(AtomicU64::new(0));
    let processed_count_clone = processed_count.clone();

    struct CountingBatchHandler {
        count: Arc<AtomicU64>,
    }

    impl BatchHandler<BatchJobMessage> for CountingBatchHandler {
        type Error = BatchError;
        type Response = ();
        type InBatch = Vec<BatchJobMessage>;
        type OutBatch = Vec<()>;

        fn handle(
            &self,
            msgs: Vec<BatchJobMessage>,
            _bus: &Bus,
        ) -> Result<Vec<Self::Response>, Self::Error> {
            self.count.fetch_add(msgs.len() as u64, Ordering::SeqCst);
            Ok(vec![(); msgs.len()])
        }
    }

    let (bus, poller) = Bus::build()
        .register(CountingBatchHandler {
            count: processed_count_clone,
        })
        .subscribe_batch_sync::<BatchJobMessage>(
            32,
            BufferUnorderedBatchedConfig {
                batch_size: 3,
                ..Default::default()
            },
        )
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id: GroupId = 100;

    // Send exactly 3 messages (= batch_size), so batch is processed immediately
    for i in 1..=3 {
        bus.send(BatchJobMessage { job_id, value: i })
            .await
            .unwrap();
    }

    // flush_group should complete since the full batch was processed
    bus.flush_group(job_id).await;

    assert!(bus.is_group_idle(job_id), "Group should be idle");
    assert_eq!(processed_count.load(Ordering::SeqCst), 3);

    bus.close().await;
}

/// Test sync_group only syncs receivers that handled the specific group.
#[tokio::test]
async fn test_sync_group_targets_correct_receivers() {
    use messagebus::AsyncHandler;
    use std::sync::atomic::AtomicU64;

    let sync_count = Arc::new(AtomicU64::new(0));
    let sync_count_clone = sync_count.clone();

    struct SyncTrackingHandler {
        sync_count: Arc<AtomicU64>,
    }

    #[async_trait]
    impl AsyncHandler<JobMessage> for SyncTrackingHandler {
        type Error = GenericError;
        type Response = ();

        async fn handle(
            &self,
            _msg: JobMessage,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            Ok(())
        }

        async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
            self.sync_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let (bus, poller) = Bus::build()
        .register(SyncTrackingHandler {
            sync_count: sync_count_clone,
        })
        .subscribe_async::<JobMessage>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id: GroupId = 100;
    let other_job_id: GroupId = 200;

    // Send message to group 100
    bus.send(JobMessage {
        job_id,
        data: "test".into(),
    })
    .await
    .unwrap();

    bus.flush_all().await;

    // sync_group for group 100 should trigger sync
    bus.sync_group(job_id).await;
    assert_eq!(sync_count.load(Ordering::SeqCst), 1);

    // sync_group for group 200 (which has no messages) should NOT trigger sync
    bus.sync_group(other_job_id).await;
    assert_eq!(
        sync_count.load(Ordering::SeqCst),
        1,
        "sync_group for unrelated group should not trigger sync"
    );

    bus.close().await;
}

/// Test flush_and_sync_group with batch handlers using full batches.
#[tokio::test]
async fn test_flush_and_sync_group_with_batches() {
    use std::sync::atomic::AtomicU64;

    let processed_count = Arc::new(AtomicU64::new(0));
    let sync_count = Arc::new(AtomicU64::new(0));
    let processed_clone = processed_count.clone();
    let sync_clone = sync_count.clone();

    struct TrackingBatchHandler {
        processed: Arc<AtomicU64>,
        synced: Arc<AtomicU64>,
    }

    impl BatchHandler<BatchJobMessage> for TrackingBatchHandler {
        type Error = BatchError;
        type Response = ();
        type InBatch = Vec<BatchJobMessage>;
        type OutBatch = Vec<()>;

        fn handle(
            &self,
            msgs: Vec<BatchJobMessage>,
            _bus: &Bus,
        ) -> Result<Vec<Self::Response>, Self::Error> {
            self.processed
                .fetch_add(msgs.len() as u64, Ordering::SeqCst);
            Ok(vec![(); msgs.len()])
        }

        fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
            self.synced.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let (bus, poller) = Bus::build()
        .register(TrackingBatchHandler {
            processed: processed_clone,
            synced: sync_clone,
        })
        .subscribe_batch_sync::<BatchJobMessage>(
            32,
            BufferUnorderedBatchedConfig {
                batch_size: 3,
                ..Default::default()
            },
        )
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id_1: GroupId = 100;
    let job_id_2: GroupId = 200;

    // Send 3 messages to group 100 (exactly 1 batch)
    for i in 1..=3 {
        bus.send(BatchJobMessage {
            job_id: job_id_1,
            value: i,
        })
        .await
        .unwrap();
    }

    // Send 3 messages to group 200 (exactly 1 batch)
    for i in 1..=3 {
        bus.send(BatchJobMessage {
            job_id: job_id_2,
            value: i,
        })
        .await
        .unwrap();
    }

    // flush_and_sync_group for group 100
    bus.flush_and_sync_group(job_id_1, false).await;

    assert!(bus.is_group_idle(job_id_1), "Group 1 should be idle");
    assert_eq!(
        sync_count.load(Ordering::SeqCst),
        1,
        "sync should have been called once for group 1"
    );

    // Now flush_and_sync_group for group 200
    bus.flush_and_sync_group(job_id_2, false).await;

    assert!(bus.is_group_idle(job_id_2), "Group 2 should be idle");
    // Note: sync is called again because both groups used the same receiver
    assert_eq!(sync_count.load(Ordering::SeqCst), 2);

    // All 6 messages should be processed
    assert_eq!(processed_count.load(Ordering::SeqCst), 6);

    bus.close().await;
}
