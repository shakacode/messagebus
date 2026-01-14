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

/// A message that triggers nested send.
#[derive(Debug, Clone, Message)]
#[group_id(self.job_id)]
struct ParentMessage {
    job_id: i64,
}

/// A child message that inherits group from parent context (no explicit group_id).
#[derive(Debug, Clone, Message)]
struct ChildMessage {
    parent_job_id: i64,
}

/// A child message with its own explicit group_id.
#[derive(Debug, Clone, Message)]
#[group_id(self.child_group_id)]
struct ChildWithGroupMessage {
    child_group_id: i64,
}

/// A grandchild message for multi-level nesting tests.
#[derive(Debug, Clone, Message)]
struct GrandchildMessage {
    original_job_id: i64,
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

/// Type alias for tracking batches per group for test verification.
type BatchesByGroup = Arc<Mutex<HashMap<Option<GroupId>, Vec<Vec<i32>>>>>;

/// Handler that tracks which group_ids are seen in each batch.
/// This verifies that batches contain only messages from the same group.
struct BatchGroupTrackingHandler {
    /// Maps group_id -> list of batches (each batch is a list of values)
    batches_by_group: BatchesByGroup,
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

// ============================================================================
// Nested send group propagation tests
// ============================================================================

/// Test that child messages without explicit group_id inherit from parent's task-local context.
#[tokio::test]
async fn test_nested_send_inherits_parent_group_id() {
    use std::sync::atomic::AtomicU64;

    let parent_received = Arc::new(AtomicU64::new(0));
    let child_received = Arc::new(AtomicU64::new(0));
    let child_group_ids = Arc::new(Mutex::new(Vec::new()));

    let parent_received_clone = parent_received.clone();
    let child_received_clone = child_received.clone();
    let child_group_ids_clone = child_group_ids.clone();

    struct NestedSendHandler {
        parent_received: Arc<AtomicU64>,
        child_received: Arc<AtomicU64>,
        child_group_ids: Arc<Mutex<Vec<Option<GroupId>>>>,
    }

    #[async_trait]
    impl AsyncHandler<ParentMessage> for NestedSendHandler {
        type Error = GenericError;
        type Response = ();

        async fn handle(
            &self,
            msg: ParentMessage,
            bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            // Verify parent has correct group_id in task-local
            assert_eq!(
                Bus::current_group_id(),
                Some(msg.job_id),
                "Parent handler should have group_id set"
            );

            self.parent_received.fetch_add(1, Ordering::SeqCst);

            // Send child message WITHOUT explicit group_id - should inherit from context
            bus.send(ChildMessage {
                parent_job_id: msg.job_id,
            })
            .await
            .unwrap();

            Ok(())
        }
    }

    #[async_trait]
    impl AsyncHandler<ChildMessage> for NestedSendHandler {
        type Error = GenericError;
        type Response = ();

        async fn handle(
            &self,
            msg: ChildMessage,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            // Record the group_id that the child handler sees
            let current_group = Bus::current_group_id();
            self.child_group_ids.lock().push(current_group);
            self.child_received.fetch_add(1, Ordering::SeqCst);

            // Child should have inherited the parent's group_id
            assert_eq!(
                current_group,
                Some(msg.parent_job_id),
                "Child should inherit parent's group_id from task-local"
            );

            Ok(())
        }
    }

    let (bus, poller) = Bus::build()
        .register(NestedSendHandler {
            parent_received: parent_received_clone,
            child_received: child_received_clone,
            child_group_ids: child_group_ids_clone,
        })
        .subscribe_async::<ParentMessage>(8, Default::default())
        .subscribe_async::<ChildMessage>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id: GroupId = 42;

    // Send parent message
    bus.send(ParentMessage { job_id }).await.unwrap();

    // Wait for both parent and child to be processed
    bus.flush_all().await;

    assert_eq!(parent_received.load(Ordering::SeqCst), 1);
    assert_eq!(child_received.load(Ordering::SeqCst), 1);

    // Verify child received the correct inherited group_id
    {
        let observed = child_group_ids.lock();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0], Some(job_id));
    }

    bus.close().await;
}

/// Test that child's explicit group_id takes precedence over inherited parent group_id.
#[tokio::test]
async fn test_nested_send_child_explicit_group_id_wins() {
    let child_group_ids = Arc::new(Mutex::new(Vec::new()));
    let child_group_ids_clone = child_group_ids.clone();

    struct ExplicitGroupHandler {
        child_group_ids: Arc<Mutex<Vec<Option<GroupId>>>>,
    }

    #[async_trait]
    impl AsyncHandler<ParentMessage> for ExplicitGroupHandler {
        type Error = GenericError;
        type Response = ();

        async fn handle(
            &self,
            msg: ParentMessage,
            bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            assert_eq!(Bus::current_group_id(), Some(msg.job_id));

            // Send child WITH explicit group_id that differs from parent
            let different_group_id = msg.job_id + 1000;
            bus.send(ChildWithGroupMessage {
                child_group_id: different_group_id,
            })
            .await
            .unwrap();

            Ok(())
        }
    }

    #[async_trait]
    impl AsyncHandler<ChildWithGroupMessage> for ExplicitGroupHandler {
        type Error = GenericError;
        type Response = ();

        async fn handle(
            &self,
            msg: ChildWithGroupMessage,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            let current_group = Bus::current_group_id();
            self.child_group_ids.lock().push(current_group);

            // Child's explicit group_id should win over parent's context
            assert_eq!(
                current_group,
                Some(msg.child_group_id),
                "Child's explicit group_id should take precedence over inherited"
            );

            Ok(())
        }
    }

    let (bus, poller) = Bus::build()
        .register(ExplicitGroupHandler {
            child_group_ids: child_group_ids_clone,
        })
        .subscribe_async::<ParentMessage>(8, Default::default())
        .subscribe_async::<ChildWithGroupMessage>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let parent_job_id: GroupId = 100;
    let expected_child_group_id = parent_job_id + 1000; // 1100

    bus.send(ParentMessage {
        job_id: parent_job_id,
    })
    .await
    .unwrap();

    bus.flush_all().await;

    // Verify child had its own explicit group_id, not the parent's
    {
        let observed = child_group_ids.lock();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0], Some(expected_child_group_id));
    }

    bus.close().await;
}

/// Test that group counter tracks both parent and child messages.
#[tokio::test]
async fn test_nested_send_group_counter_includes_children() {
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    let processing_started = Arc::new(AtomicU64::new(0));
    let processing_started_clone = processing_started.clone();

    struct SlowNestedHandler {
        processing_started: Arc<AtomicU64>,
    }

    #[async_trait]
    impl AsyncHandler<ParentMessage> for SlowNestedHandler {
        type Error = GenericError;
        type Response = ();

        async fn handle(
            &self,
            msg: ParentMessage,
            bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            self.processing_started.fetch_add(1, Ordering::SeqCst);

            // Send child message that inherits group
            bus.send(ChildMessage {
                parent_job_id: msg.job_id,
            })
            .await
            .unwrap();

            Ok(())
        }
    }

    #[async_trait]
    impl AsyncHandler<ChildMessage> for SlowNestedHandler {
        type Error = GenericError;
        type Response = ();

        async fn handle(
            &self,
            _msg: ChildMessage,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            self.processing_started.fetch_add(1, Ordering::SeqCst);
            // Simulate slow processing
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(())
        }
    }

    let (bus, poller) = Bus::build()
        .register(SlowNestedHandler {
            processing_started: processing_started_clone,
        })
        .subscribe_async::<ParentMessage>(8, Default::default())
        .subscribe_async::<ChildMessage>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id: GroupId = 42;

    // Send parent
    bus.send(ParentMessage { job_id }).await.unwrap();

    // flush_group should wait for BOTH parent AND child to complete
    bus.flush_group(job_id).await;

    // Both parent and child should have been processed
    assert_eq!(
        processing_started.load(Ordering::SeqCst),
        2,
        "Both parent and child should have been processed"
    );
    assert!(
        bus.is_group_idle(job_id),
        "Group should be idle after flush_group completes"
    );

    bus.close().await;
}

/// Test multi-level nesting: parent → child → grandchild all share same group.
#[tokio::test]
async fn test_nested_send_multi_level_propagation() {
    let grandchild_group_ids = Arc::new(Mutex::new(Vec::new()));
    let grandchild_group_ids_clone = grandchild_group_ids.clone();

    struct MultiLevelHandler {
        grandchild_group_ids: Arc<Mutex<Vec<Option<GroupId>>>>,
    }

    #[async_trait]
    impl AsyncHandler<ParentMessage> for MultiLevelHandler {
        type Error = GenericError;
        type Response = ();

        async fn handle(
            &self,
            msg: ParentMessage,
            bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            assert_eq!(Bus::current_group_id(), Some(msg.job_id));

            // Send child (which will send grandchild)
            bus.send(ChildMessage {
                parent_job_id: msg.job_id,
            })
            .await
            .unwrap();

            Ok(())
        }
    }

    #[async_trait]
    impl AsyncHandler<ChildMessage> for MultiLevelHandler {
        type Error = GenericError;
        type Response = ();

        async fn handle(
            &self,
            msg: ChildMessage,
            bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            // Child should have inherited parent's group
            assert_eq!(Bus::current_group_id(), Some(msg.parent_job_id));

            // Send grandchild (no explicit group_id)
            bus.send(GrandchildMessage {
                original_job_id: msg.parent_job_id,
            })
            .await
            .unwrap();

            Ok(())
        }
    }

    #[async_trait]
    impl AsyncHandler<GrandchildMessage> for MultiLevelHandler {
        type Error = GenericError;
        type Response = ();

        async fn handle(
            &self,
            msg: GrandchildMessage,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            let current_group = Bus::current_group_id();
            self.grandchild_group_ids.lock().push(current_group);

            // Grandchild should have inherited the original parent's group_id
            assert_eq!(
                current_group,
                Some(msg.original_job_id),
                "Grandchild should inherit original parent's group_id through multi-level nesting"
            );

            Ok(())
        }
    }

    let (bus, poller) = Bus::build()
        .register(MultiLevelHandler {
            grandchild_group_ids: grandchild_group_ids_clone,
        })
        .subscribe_async::<ParentMessage>(8, Default::default())
        .subscribe_async::<ChildMessage>(8, Default::default())
        .subscribe_async::<GrandchildMessage>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id: GroupId = 999;

    bus.send(ParentMessage { job_id }).await.unwrap();
    bus.flush_all().await;

    // Verify grandchild received the original group_id
    {
        let observed = grandchild_group_ids.lock();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0], Some(job_id));
    }

    // Group should be idle after all three levels processed
    assert!(bus.is_group_idle(job_id));

    bus.close().await;
}

// ============================================================================
// Error handling tests for group_id
// ============================================================================

/// Error type for error handling tests.
#[derive(Debug, Clone, Error, MbError)]
enum TestError {
    #[error("Test error: {0}")]
    Handler(String),
}

/// A message for error handling tests.
#[derive(Debug, Clone, Message)]
#[group_id(self.job_id)]
struct ErrorTestMessage {
    job_id: i64,
    should_fail: bool,
}

/// A message for panic tests.
#[derive(Debug, Clone, Message)]
#[group_id(self.job_id)]
struct PanicTestMessage {
    job_id: i64,
    should_panic: bool,
}

/// Test that group counter is properly decremented when handler returns an error.
#[tokio::test]
async fn test_handler_error_decrements_group_counter() {
    use std::sync::atomic::AtomicU64;

    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    let success_count_clone = success_count.clone();
    let error_count_clone = error_count.clone();

    struct ErroringHandler {
        success_count: Arc<AtomicU64>,
        error_count: Arc<AtomicU64>,
    }

    #[async_trait]
    impl AsyncHandler<ErrorTestMessage> for ErroringHandler {
        type Error = TestError;
        type Response = ();

        async fn handle(
            &self,
            msg: ErrorTestMessage,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            if msg.should_fail {
                self.error_count.fetch_add(1, Ordering::SeqCst);
                Err(TestError::Handler("Intentional test error".into()))
            } else {
                self.success_count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
    }

    let (bus, poller) = Bus::build()
        .register(ErroringHandler {
            success_count: success_count_clone,
            error_count: error_count_clone,
        })
        .subscribe_async::<ErrorTestMessage>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id: GroupId = 42;

    // Send a mix of succeeding and failing messages
    bus.send(ErrorTestMessage {
        job_id,
        should_fail: false,
    })
    .await
    .unwrap();

    bus.send(ErrorTestMessage {
        job_id,
        should_fail: true,
    })
    .await
    .unwrap();

    bus.send(ErrorTestMessage {
        job_id,
        should_fail: false,
    })
    .await
    .unwrap();

    // flush_group should complete even with errors - counter should still decrement
    bus.flush_group(job_id).await;

    assert_eq!(success_count.load(Ordering::SeqCst), 2);
    assert_eq!(error_count.load(Ordering::SeqCst), 1);
    assert!(
        bus.is_group_idle(job_id),
        "Group should be idle after flush_group, even with errors"
    );

    bus.close().await;
}

/// Test that group counter is properly decremented when handler panics.
///
/// Note: This test verifies the current behavior. If panics don't decrement
/// the counter, flush_group would hang - which would indicate a bug to fix.
#[tokio::test]
async fn test_handler_panic_decrements_group_counter() {
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    let success_count = Arc::new(AtomicU64::new(0));
    let success_count_clone = success_count.clone();

    struct PanickingHandler {
        success_count: Arc<AtomicU64>,
    }

    #[async_trait]
    impl AsyncHandler<PanicTestMessage> for PanickingHandler {
        type Error = TestError;
        type Response = ();

        async fn handle(
            &self,
            msg: PanicTestMessage,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            if msg.should_panic {
                panic!("Intentional test panic");
            }
            self.success_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let (bus, poller) = Bus::build()
        .register(PanickingHandler {
            success_count: success_count_clone,
        })
        .subscribe_async::<PanicTestMessage>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id: GroupId = 42;

    // Send messages - one will panic, one will succeed
    bus.send(PanicTestMessage {
        job_id,
        should_panic: false,
    })
    .await
    .unwrap();

    bus.send(PanicTestMessage {
        job_id,
        should_panic: true,
    })
    .await
    .unwrap();

    bus.send(PanicTestMessage {
        job_id,
        should_panic: false,
    })
    .await
    .unwrap();

    // Use flush_all with timeout to detect if counter isn't decremented on panic
    // If panic doesn't decrement counter, this would hang forever
    let flush_result = tokio::time::timeout(Duration::from_secs(2), bus.flush_group(job_id)).await;

    // Check if flush completed or timed out
    match flush_result {
        Ok(()) => {
            // Counter was properly decremented even after panic
            assert!(
                bus.is_group_idle(job_id),
                "Group should be idle after flush_group"
            );
        }
        Err(_) => {
            // Timeout - this indicates the panic didn't decrement the counter
            // This is the expected behavior based on current implementation
            // The group counter is stuck because panic prevented decrement
            panic!(
                "flush_group timed out - handler panic did not decrement group counter. \
                 This is a bug: panicking handlers should still decrement the group counter."
            );
        }
    }

    // Non-panicking messages should have succeeded
    assert_eq!(success_count.load(Ordering::SeqCst), 2);

    bus.close().await;
}

/// Test that batch handler errors properly decrement group counters.
#[tokio::test]
async fn test_batch_handler_error_decrements_group_counter() {
    use std::sync::atomic::AtomicU64;

    let batch_count = Arc::new(AtomicU64::new(0));
    let error_batch_count = Arc::new(AtomicU64::new(0));
    let batch_count_clone = batch_count.clone();
    let error_batch_count_clone = error_batch_count.clone();

    /// A message for batch error tests.
    #[derive(Debug, Clone, Message)]
    #[message(clone)]
    #[group_id(self.job_id)]
    struct BatchErrorMessage {
        job_id: i64,
        should_fail: bool,
    }

    struct ErroringBatchHandler {
        batch_count: Arc<AtomicU64>,
        error_batch_count: Arc<AtomicU64>,
    }

    impl BatchHandler<BatchErrorMessage> for ErroringBatchHandler {
        type Error = BatchError;
        type Response = ();
        type InBatch = Vec<BatchErrorMessage>;
        type OutBatch = Vec<()>;

        fn handle(
            &self,
            msgs: Vec<BatchErrorMessage>,
            _bus: &Bus,
        ) -> Result<Vec<Self::Response>, Self::Error> {
            // Check if any message in batch should trigger error
            let has_error = msgs.iter().any(|m| m.should_fail);

            if has_error {
                self.error_batch_count.fetch_add(1, Ordering::SeqCst);
                Err(BatchError::Error("Intentional batch error".into()))
            } else {
                self.batch_count.fetch_add(1, Ordering::SeqCst);
                Ok(vec![(); msgs.len()])
            }
        }
    }

    let (bus, poller) = Bus::build()
        .register(ErroringBatchHandler {
            batch_count: batch_count_clone,
            error_batch_count: error_batch_count_clone,
        })
        .subscribe_batch_sync::<BatchErrorMessage>(
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

    // Send 4 messages - 2 batches of 2
    // First batch: both succeed
    bus.send(BatchErrorMessage {
        job_id,
        should_fail: false,
    })
    .await
    .unwrap();
    bus.send(BatchErrorMessage {
        job_id,
        should_fail: false,
    })
    .await
    .unwrap();

    // Second batch: one fails, triggering batch error
    bus.send(BatchErrorMessage {
        job_id,
        should_fail: false,
    })
    .await
    .unwrap();
    bus.send(BatchErrorMessage {
        job_id,
        should_fail: true,
    })
    .await
    .unwrap();

    // flush_group should complete even with batch errors
    bus.flush_group(job_id).await;

    assert_eq!(
        batch_count.load(Ordering::SeqCst),
        1,
        "One batch should succeed"
    );
    assert_eq!(
        error_batch_count.load(Ordering::SeqCst),
        1,
        "One batch should error"
    );
    assert!(
        bus.is_group_idle(job_id),
        "Group should be idle after flush_group, even with batch errors"
    );

    bus.close().await;
}

/// Test multiple groups with mixed success/error handling.
#[tokio::test]
async fn test_multiple_groups_with_errors() {
    use std::sync::atomic::AtomicU64;

    let processed = Arc::new(AtomicU64::new(0));
    let processed_clone = processed.clone();

    struct MixedHandler {
        processed: Arc<AtomicU64>,
    }

    #[async_trait]
    impl AsyncHandler<ErrorTestMessage> for MixedHandler {
        type Error = TestError;
        type Response = ();

        async fn handle(
            &self,
            msg: ErrorTestMessage,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            self.processed.fetch_add(1, Ordering::SeqCst);
            if msg.should_fail {
                Err(TestError::Handler("Error in handler".into()))
            } else {
                Ok(())
            }
        }
    }

    let (bus, poller) = Bus::build()
        .register(MixedHandler {
            processed: processed_clone,
        })
        .subscribe_async::<ErrorTestMessage>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let group_1: GroupId = 100;
    let group_2: GroupId = 200;

    // Group 1: 2 success, 1 error
    bus.send(ErrorTestMessage {
        job_id: group_1,
        should_fail: false,
    })
    .await
    .unwrap();
    bus.send(ErrorTestMessage {
        job_id: group_1,
        should_fail: true,
    })
    .await
    .unwrap();
    bus.send(ErrorTestMessage {
        job_id: group_1,
        should_fail: false,
    })
    .await
    .unwrap();

    // Group 2: all success
    bus.send(ErrorTestMessage {
        job_id: group_2,
        should_fail: false,
    })
    .await
    .unwrap();
    bus.send(ErrorTestMessage {
        job_id: group_2,
        should_fail: false,
    })
    .await
    .unwrap();

    // Flush each group independently
    bus.flush_group(group_1).await;
    assert!(bus.is_group_idle(group_1), "Group 1 should be idle");

    bus.flush_group(group_2).await;
    assert!(bus.is_group_idle(group_2), "Group 2 should be idle");

    // All 5 messages should have been processed
    assert_eq!(processed.load(Ordering::SeqCst), 5);

    bus.close().await;
}

// ============================================================================
// Request-response group tests
// ============================================================================

/// Request message with group_id.
#[derive(Debug, Clone, Message)]
#[group_id(self.job_id)]
struct GroupedRequest {
    job_id: i64,
    value: i32,
}

/// Response message.
#[derive(Debug, Clone, Message)]
struct GroupedResponse {
    result: i32,
}

/// Request message without group_id (for inheritance tests).
#[derive(Debug, Clone, Message)]
struct UngroupedRequest {
    value: i32,
}

/// Test that request with group_id properly tracks the group.
#[tokio::test]
async fn test_request_with_group_id_tracks_group() {
    struct RequestHandler;

    #[async_trait]
    impl AsyncHandler<GroupedRequest> for RequestHandler {
        type Error = TestError;
        type Response = GroupedResponse;

        async fn handle(
            &self,
            msg: GroupedRequest,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            // Verify group_id is set in task-local
            assert_eq!(
                Bus::current_group_id(),
                Some(msg.job_id),
                "Request handler should have group_id set"
            );
            Ok(GroupedResponse {
                result: msg.value * 2,
            })
        }
    }

    let (bus, poller) = Bus::build()
        .register(RequestHandler)
        .subscribe_async::<GroupedRequest>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id: GroupId = 42;

    // Initially idle
    assert!(bus.is_group_idle(job_id));

    // Send request
    let response = bus
        .request::<_, GroupedResponse>(GroupedRequest { job_id, value: 21 }, Default::default())
        .await
        .unwrap();

    assert_eq!(response.result, 42);

    // After request completes, group should be idle
    assert!(bus.is_group_idle(job_id));

    bus.close().await;
}

/// Test that flush_group waits for in-flight requests.
#[tokio::test]
async fn test_request_flush_group_waits_for_response() {
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    let handler_started = Arc::new(AtomicBool::new(false));
    let handler_completed = Arc::new(AtomicBool::new(false));
    let handler_started_clone = handler_started.clone();
    let handler_completed_clone = handler_completed.clone();

    struct SlowRequestHandler {
        started: Arc<AtomicBool>,
        completed: Arc<AtomicBool>,
    }

    #[async_trait]
    impl AsyncHandler<GroupedRequest> for SlowRequestHandler {
        type Error = TestError;
        type Response = GroupedResponse;

        async fn handle(
            &self,
            msg: GroupedRequest,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            self.started.store(true, Ordering::SeqCst);
            // Simulate slow processing
            tokio::time::sleep(Duration::from_millis(100)).await;
            self.completed.store(true, Ordering::SeqCst);
            Ok(GroupedResponse {
                result: msg.value * 2,
            })
        }
    }

    let (bus, poller) = Bus::build()
        .register(SlowRequestHandler {
            started: handler_started_clone,
            completed: handler_completed_clone,
        })
        .subscribe_async::<GroupedRequest>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id: GroupId = 42;

    // Send request without awaiting response (fire and forget via send)
    bus.send(GroupedRequest { job_id, value: 21 })
        .await
        .unwrap();

    // Group should not be idle while request is processing
    // Give it a moment to start
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        handler_started.load(Ordering::SeqCst),
        "Handler should have started"
    );
    assert!(
        !bus.is_group_idle(job_id),
        "Group should not be idle during processing"
    );

    // flush_group should wait for the request to complete
    bus.flush_group(job_id).await;

    assert!(
        handler_completed.load(Ordering::SeqCst),
        "Handler should have completed"
    );
    assert!(
        bus.is_group_idle(job_id),
        "Group should be idle after flush"
    );

    bus.close().await;
}

/// Test that nested requests inherit group_id from parent context.
#[tokio::test]
async fn test_nested_request_inherits_group() {
    let nested_group_ids = Arc::new(Mutex::new(Vec::new()));
    let nested_group_ids_clone = nested_group_ids.clone();

    struct OuterHandler;

    struct InnerHandler {
        nested_group_ids: Arc<Mutex<Vec<Option<GroupId>>>>,
    }

    #[async_trait]
    impl AsyncHandler<GroupedRequest> for OuterHandler {
        type Error = TestError;
        type Response = GroupedResponse;

        async fn handle(
            &self,
            msg: GroupedRequest,
            bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            // Verify outer handler has group_id
            assert_eq!(Bus::current_group_id(), Some(msg.job_id));

            // Send nested request WITHOUT group_id - should inherit from context
            let inner_response = bus
                .request::<_, GroupedResponse>(
                    UngroupedRequest { value: msg.value },
                    Default::default(),
                )
                .await
                .map_err(|_| TestError::Handler("Inner request failed".into()))?;

            Ok(GroupedResponse {
                result: inner_response.result + 1,
            })
        }
    }

    #[async_trait]
    impl AsyncHandler<UngroupedRequest> for InnerHandler {
        type Error = TestError;
        type Response = GroupedResponse;

        async fn handle(
            &self,
            msg: UngroupedRequest,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            // Record the group_id seen by inner handler
            let current_group = Bus::current_group_id();
            self.nested_group_ids.lock().push(current_group);

            Ok(GroupedResponse {
                result: msg.value * 2,
            })
        }
    }

    let (bus, poller) = Bus::build()
        .register(OuterHandler)
        .subscribe_async::<GroupedRequest>(8, Default::default())
        .done()
        .register(InnerHandler {
            nested_group_ids: nested_group_ids_clone,
        })
        .subscribe_async::<UngroupedRequest>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id: GroupId = 999;

    let response = bus
        .request::<_, GroupedResponse>(GroupedRequest { job_id, value: 10 }, Default::default())
        .await
        .unwrap();

    // Outer processes inner (10*2=20), then adds 1 = 21
    assert_eq!(response.result, 21);

    // Verify inner handler inherited the group_id
    {
        let observed = nested_group_ids.lock();
        assert_eq!(observed.len(), 1);
        assert_eq!(
            observed[0],
            Some(job_id),
            "Nested request should inherit group_id from parent"
        );
    }

    // Group should be idle after all requests complete
    assert!(bus.is_group_idle(job_id));

    bus.close().await;
}

/// Test that multiple concurrent requests in same group are tracked correctly.
#[tokio::test]
async fn test_multiple_concurrent_requests_same_group() {
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    let request_count = Arc::new(AtomicU64::new(0));
    let request_count_clone = request_count.clone();

    struct ConcurrentRequestHandler {
        count: Arc<AtomicU64>,
    }

    #[async_trait]
    impl AsyncHandler<GroupedRequest> for ConcurrentRequestHandler {
        type Error = TestError;
        type Response = GroupedResponse;

        async fn handle(
            &self,
            msg: GroupedRequest,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            self.count.fetch_add(1, Ordering::SeqCst);
            // Small delay to ensure concurrent execution
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(GroupedResponse {
                result: msg.value * 2,
            })
        }
    }

    let (bus, poller) = Bus::build()
        .register(ConcurrentRequestHandler {
            count: request_count_clone,
        })
        .subscribe_async::<GroupedRequest>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id: GroupId = 42;

    // Send multiple requests concurrently (don't await individually)
    for i in 0..5 {
        bus.send(GroupedRequest { job_id, value: i }).await.unwrap();
    }

    // flush_group should wait for ALL requests
    bus.flush_group(job_id).await;

    assert_eq!(request_count.load(Ordering::SeqCst), 5);
    assert!(bus.is_group_idle(job_id));

    bus.close().await;
}

/// Test that boxed messages preserve group_id for untyped dispatch.
/// This verifies fix #4: extract group_id from boxed_msg before downcasting.
#[tokio::test]
async fn test_boxed_message_preserves_group_id() {
    let observed_group_id = Arc::new(Mutex::new(None::<Option<GroupId>>));
    let observed_group_id_clone = observed_group_id.clone();

    struct BoxedHandler {
        observed_group_id: Arc<Mutex<Option<Option<GroupId>>>>,
    }

    #[async_trait]
    impl AsyncHandler<GroupedRequest> for BoxedHandler {
        type Error = TestError;
        type Response = GroupedResponse;

        async fn handle(
            &self,
            msg: GroupedRequest,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            // Record the group_id from task-local context
            *self.observed_group_id.lock() = Some(Bus::current_group_id());
            Ok(GroupedResponse {
                result: msg.value * 2,
            })
        }
    }

    let (bus, poller) = Bus::build()
        .register(BoxedHandler {
            observed_group_id: observed_group_id_clone,
        })
        .subscribe_async::<GroupedRequest>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    let job_id: GroupId = 777;

    // Verify group counter starts at 0
    assert!(bus.is_group_idle(job_id));

    // Send via request_boxed - this uses the untyped dispatch path
    let boxed_msg: Box<dyn messagebus::Message> = Box::new(GroupedRequest { job_id, value: 21 });

    let response = bus
        .request_boxed(boxed_msg, Default::default())
        .await
        .unwrap();

    // Verify response
    let result = response
        .as_any_ref()
        .downcast_ref::<GroupedResponse>()
        .unwrap();
    assert_eq!(result.result, 42);

    // Verify the handler saw the correct group_id via task-local context
    {
        let observed = observed_group_id.lock();
        assert_eq!(
            *observed,
            Some(Some(job_id)),
            "Boxed message should propagate group_id to handler context"
        );
    }

    // Group should be idle after request completes
    assert!(bus.is_group_idle(job_id));

    bus.close().await;
}

/// Test that groups can be removed to free memory.
#[tokio::test]
async fn test_group_cleanup() {
    struct SimpleHandler;

    #[async_trait]
    impl AsyncHandler<JobMessage> for SimpleHandler {
        type Error = GenericError;
        type Response = ();

        async fn handle(
            &self,
            _msg: JobMessage,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            Ok(())
        }
    }

    let (bus, poller) = Bus::build()
        .register(SimpleHandler)
        .subscribe_async::<JobMessage>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    // Initially no groups tracked
    assert_eq!(bus.tracked_group_count(), 0);

    // Send messages to multiple groups
    for group in 1..=5 {
        bus.send(JobMessage {
            job_id: group,
            data: format!("job {}", group),
        })
        .await
        .unwrap();
    }

    // Wait for all to complete
    for group in 1..=5 {
        bus.flush_group(group).await;
    }

    // Groups are tracked even after completion
    assert_eq!(bus.tracked_group_count(), 5);

    // Remove completed groups (should succeed since they're idle)
    for group in 1..=5 {
        assert!(bus.is_group_idle(group));
        assert_eq!(
            bus.remove_group(group),
            Some(true),
            "Group {} should be removed (idle)",
            group
        );
    }

    // All groups cleaned up
    assert_eq!(bus.tracked_group_count(), 0);

    // Removing non-existent group returns None
    assert_eq!(bus.remove_group(999), None);

    bus.close().await;
}

/// Test that remove_group refuses to remove groups with in-flight tasks.
#[tokio::test]
async fn test_remove_group_rejects_active_group() {
    use std::time::Duration;
    use tokio::sync::Barrier;

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = barrier.clone();

    struct SlowHandler {
        barrier: Arc<Barrier>,
    }

    #[async_trait]
    impl AsyncHandler<JobMessage> for SlowHandler {
        type Error = GenericError;
        type Response = ();

        async fn handle(
            &self,
            _msg: JobMessage,
            _bus: &Bus,
        ) -> Result<Self::Response, Self::Error> {
            // Wait for test to try removing the group
            self.barrier.wait().await;
            // Give test time to attempt removal
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(())
        }
    }

    let (bus, poller) = Bus::build()
        .register(SlowHandler {
            barrier: barrier_clone,
        })
        .subscribe_async::<JobMessage>(8, Default::default())
        .done()
        .build();

    tokio::spawn(poller);
    bus.ready().await;

    // Send a message that will block in handler
    let bus_clone = bus.clone();
    let handle = tokio::spawn(async move {
        bus_clone
            .send(JobMessage {
                job_id: 42,
                data: "test".into(),
            })
            .await
            .unwrap();
    });

    // Wait for handler to start
    barrier.wait().await;

    // Group should not be idle
    assert!(!bus.is_group_idle(42));

    // Attempt to remove should fail (returns Some(false))
    assert_eq!(
        bus.remove_group(42),
        Some(false),
        "Should refuse to remove active group"
    );

    // Group should still be tracked
    assert_eq!(bus.tracked_group_count(), 1);

    // Wait for handler to complete
    handle.await.unwrap();
    bus.flush_group(42).await;

    // Now removal should succeed
    assert_eq!(bus.remove_group(42), Some(true));
    assert_eq!(bus.tracked_group_count(), 0);

    bus.close().await;
}
