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
