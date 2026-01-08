use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error, AsyncHandler, Bus, Message, SendOptions,
};
use thiserror::Error;

#[derive(Debug, Error, MbError)]
enum Error {
    #[error("Error({0})")]
    Error(anyhow::Error),
}

impl<M: Message> From<error::Error<M>> for Error {
    fn from(err: error::Error<M>) -> Self {
        Self::Error(err.into())
    }
}

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct TestMsg(#[allow(dead_code)] pub u32);

#[derive(Debug, Clone, Message)]
struct GetCount;

#[derive(Debug, Clone, Message)]
struct CountResult(#[allow(dead_code)] pub u32);

struct ReceiverA {
    counter: Arc<AtomicU32>,
}

struct ReceiverB {
    counter: Arc<AtomicU32>,
}

#[async_trait]
impl AsyncHandler<TestMsg> for ReceiverA {
    type Error = Error;
    type Response = ();

    async fn handle(&self, _msg: TestMsg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<GetCount> for ReceiverA {
    type Error = Error;
    type Response = CountResult;

    async fn handle(&self, _: GetCount, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(CountResult(self.counter.load(Ordering::SeqCst)))
    }
}

#[async_trait]
impl AsyncHandler<TestMsg> for ReceiverB {
    type Error = Error;
    type Response = ();

    async fn handle(&self, _msg: TestMsg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<GetCount> for ReceiverB {
    type Error = Error;
    type Response = CountResult;

    async fn handle(&self, _: GetCount, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(CountResult(self.counter.load(Ordering::SeqCst)))
    }
}

/// Test that Broadcast sends to all receivers
#[tokio::test]
async fn test_send_options_broadcast() {
    let counter_a = Arc::new(AtomicU32::new(0));
    let counter_b = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ReceiverA {
            counter: counter_a.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .register(ReceiverB {
            counter: counter_b.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .build();

    // Send with Broadcast (default)
    for _ in 0..10 {
        b.send(TestMsg(1)).await.unwrap();
    }

    b.flush_all().await;

    // Both receivers should have received all messages
    assert_eq!(counter_a.load(Ordering::SeqCst), 10);
    assert_eq!(counter_b.load(Ordering::SeqCst), 10);

    b.close().await;
    poller.await;
}

/// Test that send_ext with Broadcast works the same as send
#[tokio::test]
async fn test_send_options_broadcast_explicit() {
    let counter_a = Arc::new(AtomicU32::new(0));
    let counter_b = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ReceiverA {
            counter: counter_a.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .register(ReceiverB {
            counter: counter_b.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .build();

    // Send with explicit Broadcast option
    for _ in 0..10 {
        b.send_ext(TestMsg(1), SendOptions::Broadcast)
            .await
            .unwrap();
    }

    b.flush_all().await;

    // Both receivers should have received all messages
    assert_eq!(counter_a.load(Ordering::SeqCst), 10);
    assert_eq!(counter_b.load(Ordering::SeqCst), 10);

    b.close().await;
    poller.await;
}

/// Test that a single receiver receives messages correctly
#[tokio::test]
async fn test_send_options_single_receiver() {
    let counter = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ReceiverA {
            counter: counter.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .build();

    for _ in 0..5 {
        b.send(TestMsg(1)).await.unwrap();
    }

    b.flush_all().await;

    assert_eq!(counter.load(Ordering::SeqCst), 5);

    b.close().await;
    poller.await;
}

/// Test send_one sends to only one receiver
#[tokio::test]
async fn test_send_one() {
    let counter_a = Arc::new(AtomicU32::new(0));
    let counter_b = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ReceiverA {
            counter: counter_a.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .register(ReceiverB {
            counter: counter_b.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .build();

    // Send using send_one - should only go to first receiver
    for _ in 0..10 {
        b.send_one(TestMsg(1)).await.unwrap();
    }

    b.flush_all().await;

    // Only one receiver should have received all messages
    let total = counter_a.load(Ordering::SeqCst) + counter_b.load(Ordering::SeqCst);
    assert_eq!(total, 10);

    // One should have all 10, the other should have 0
    let a_count = counter_a.load(Ordering::SeqCst);
    let b_count = counter_b.load(Ordering::SeqCst);
    assert!(
        (a_count == 10 && b_count == 0) || (a_count == 0 && b_count == 10),
        "Expected one receiver to get all messages, got a={}, b={}",
        a_count,
        b_count
    );

    b.close().await;
    poller.await;
}

/// Test try_send_one sends to only one receiver
#[tokio::test]
async fn test_try_send_one() {
    let counter_a = Arc::new(AtomicU32::new(0));
    let counter_b = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ReceiverA {
            counter: counter_a.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .register(ReceiverB {
            counter: counter_b.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .build();

    // Send using try_send_one
    for _ in 0..5 {
        b.try_send_one(TestMsg(1)).unwrap();
    }

    b.flush_all().await;

    // Only one receiver should have received all messages
    let total = counter_a.load(Ordering::SeqCst) + counter_b.load(Ordering::SeqCst);
    assert_eq!(total, 5);

    b.close().await;
    poller.await;
}

/// Test force_send works correctly
#[tokio::test]
async fn test_force_send() {
    let counter_a = Arc::new(AtomicU32::new(0));
    let counter_b = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ReceiverA {
            counter: counter_a.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .register(ReceiverB {
            counter: counter_b.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .build();

    // Send using force_send - should broadcast to all
    for _ in 0..10 {
        b.force_send(TestMsg(1)).unwrap();
    }

    b.flush_all().await;

    // Both receivers should have received all messages
    assert_eq!(counter_a.load(Ordering::SeqCst), 10);
    assert_eq!(counter_b.load(Ordering::SeqCst), 10);

    b.close().await;
    poller.await;
}

/// Test send_blocking works correctly
#[tokio::test]
async fn test_send_blocking() {
    let counter = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ReceiverA {
            counter: counter.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .build();

    // Use a thread to call send_blocking since it blocks the current thread
    let bus_clone = b.clone();
    std::thread::spawn(move || {
        for _ in 0..5 {
            bus_clone.send_blocking(TestMsg(1)).unwrap();
        }
    })
    .join()
    .unwrap();

    b.flush_all().await;

    assert_eq!(counter.load(Ordering::SeqCst), 5);

    b.close().await;
    poller.await;
}

/// Test that closed bus returns error
#[tokio::test]
async fn test_send_to_closed_bus() {
    let counter = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ReceiverA {
            counter: counter.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .build();

    b.close().await;

    // Sending to closed bus should fail
    let result = b.send(TestMsg(1)).await;
    assert!(result.is_err());

    poller.await;
}

/// Test try_send to closed bus returns error
#[tokio::test]
async fn test_try_send_to_closed_bus() {
    let counter = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ReceiverA {
            counter: counter.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .build();

    b.close().await;

    // try_send to closed bus should fail
    let result = b.try_send(TestMsg(1));
    assert!(result.is_err());

    poller.await;
}

/// Test force_send to closed bus returns error
#[tokio::test]
async fn test_force_send_to_closed_bus() {
    let counter = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ReceiverA {
            counter: counter.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .build();

    b.close().await;

    // force_send to closed bus should fail
    let result = b.force_send(TestMsg(1));
    assert!(result.is_err());

    poller.await;
}

/// Test is_closing returns correct state
#[tokio::test]
async fn test_is_closing() {
    let counter = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ReceiverA {
            counter: counter.clone(),
        })
        .subscribe_async::<TestMsg>(8, Default::default())
        .done()
        .build();

    assert!(!b.is_closing());

    b.close().await;

    assert!(b.is_closing());

    poller.await;
}
