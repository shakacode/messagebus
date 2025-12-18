use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error, AsyncHandler, Bus, Handler, Message, Module,
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
struct SharedMsg(pub u32);

#[derive(Debug, Clone, Message)]
struct GetCountA;

#[derive(Debug, Clone, Message)]
struct GetCountB;

#[derive(Debug, Clone, Message)]
struct GetCountC;

#[derive(Debug, Clone, Message)]
struct CountResult(pub u32);

// Three different receivers that all handle the same message type
struct ReceiverA {
    counter: Arc<AtomicU32>,
}

struct ReceiverB {
    counter: Arc<AtomicU32>,
}

struct ReceiverC {
    counter: Arc<AtomicU32>,
}

#[async_trait]
impl AsyncHandler<SharedMsg> for ReceiverA {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: SharedMsg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.counter.fetch_add(msg.0, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<GetCountA> for ReceiverA {
    type Error = Error;
    type Response = CountResult;

    async fn handle(&self, _: GetCountA, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(CountResult(self.counter.load(Ordering::SeqCst)))
    }
}

#[async_trait]
impl AsyncHandler<SharedMsg> for ReceiverB {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: SharedMsg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.counter.fetch_add(msg.0, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<GetCountB> for ReceiverB {
    type Error = Error;
    type Response = CountResult;

    async fn handle(&self, _: GetCountB, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(CountResult(self.counter.load(Ordering::SeqCst)))
    }
}

#[async_trait]
impl AsyncHandler<SharedMsg> for ReceiverC {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: SharedMsg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.counter.fetch_add(msg.0, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<GetCountC> for ReceiverC {
    type Error = Error;
    type Response = CountResult;

    async fn handle(&self, _: GetCountC, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(CountResult(self.counter.load(Ordering::SeqCst)))
    }
}

/// Test that multiple receivers all receive broadcast messages
#[tokio::test]
async fn test_multiple_receivers_broadcast() {
    let counter_a = Arc::new(AtomicU32::new(0));
    let counter_b = Arc::new(AtomicU32::new(0));
    let counter_c = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ReceiverA {
            counter: counter_a.clone(),
        })
        .subscribe_async::<SharedMsg>(8, Default::default())
        .subscribe_async::<GetCountA>(8, Default::default())
        .done()
        .register(ReceiverB {
            counter: counter_b.clone(),
        })
        .subscribe_async::<SharedMsg>(8, Default::default())
        .subscribe_async::<GetCountB>(8, Default::default())
        .done()
        .register(ReceiverC {
            counter: counter_c.clone(),
        })
        .subscribe_async::<SharedMsg>(8, Default::default())
        .subscribe_async::<GetCountC>(8, Default::default())
        .done()
        .build();

    // Send messages - each should go to all 3 receivers
    for i in 1..=10u32 {
        b.send(SharedMsg(i)).await.unwrap();
    }

    b.flush_all().await;

    // Each receiver should have received the sum 1+2+...+10 = 55
    let result_a = b
        .request_we::<_, CountResult, Error>(GetCountA, Default::default())
        .await
        .unwrap();
    let result_b = b
        .request_we::<_, CountResult, Error>(GetCountB, Default::default())
        .await
        .unwrap();
    let result_c = b
        .request_we::<_, CountResult, Error>(GetCountC, Default::default())
        .await
        .unwrap();

    assert_eq!(result_a.0, 55, "Receiver A should have sum 55");
    assert_eq!(result_b.0, 55, "Receiver B should have sum 55");
    assert_eq!(result_c.0, 55, "Receiver C should have sum 55");

    b.close().await;
    poller.await;
}

/// Test two receivers with same message type
#[tokio::test]
async fn test_two_receivers_same_message() {
    let counter_a = Arc::new(AtomicU32::new(0));
    let counter_b = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ReceiverA {
            counter: counter_a.clone(),
        })
        .subscribe_async::<SharedMsg>(8, Default::default())
        .subscribe_async::<GetCountA>(8, Default::default())
        .done()
        .register(ReceiverB {
            counter: counter_b.clone(),
        })
        .subscribe_async::<SharedMsg>(8, Default::default())
        .subscribe_async::<GetCountB>(8, Default::default())
        .done()
        .build();

    // Send 5 messages with value 10 each
    for _ in 0..5 {
        b.send(SharedMsg(10)).await.unwrap();
    }

    b.flush_all().await;

    // Each receiver should have 50 (5 * 10)
    let result_a = b
        .request_we::<_, CountResult, Error>(GetCountA, Default::default())
        .await
        .unwrap();
    let result_b = b
        .request_we::<_, CountResult, Error>(GetCountB, Default::default())
        .await
        .unwrap();

    assert_eq!(result_a.0, 50);
    assert_eq!(result_b.0, 50);

    b.close().await;
    poller.await;
}

/// Test using Module pattern with multiple receivers
#[tokio::test]
async fn test_multiple_receivers_with_module() {
    let counter_a = Arc::new(AtomicU32::new(0));
    let counter_b = Arc::new(AtomicU32::new(0));

    fn module_a(counter: Arc<AtomicU32>) -> Module {
        Module::new()
            .register(ReceiverA { counter })
            .subscribe_async::<SharedMsg>(8, Default::default())
            .subscribe_async::<GetCountA>(8, Default::default())
            .done()
    }

    fn module_b(counter: Arc<AtomicU32>) -> Module {
        Module::new()
            .register(ReceiverB { counter })
            .subscribe_async::<SharedMsg>(8, Default::default())
            .subscribe_async::<GetCountB>(8, Default::default())
            .done()
    }

    let (b, poller) = Bus::build()
        .add_module(module_a(counter_a.clone()))
        .add_module(module_b(counter_b.clone()))
        .build();

    // Send messages
    for i in 1..=20u32 {
        b.send(SharedMsg(i)).await.unwrap();
    }

    b.flush_all().await;

    // Sum of 1..=20 = 210
    let result_a = b
        .request_we::<_, CountResult, Error>(GetCountA, Default::default())
        .await
        .unwrap();
    let result_b = b
        .request_we::<_, CountResult, Error>(GetCountB, Default::default())
        .await
        .unwrap();

    assert_eq!(result_a.0, 210);
    assert_eq!(result_b.0, 210);

    b.close().await;
    poller.await;
}

// Test different handler types receiving same message
#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MixedMsg(pub u32);

struct SyncReceiver {
    counter: Arc<AtomicU32>,
}

struct AsyncReceiver {
    counter: Arc<AtomicU32>,
}

impl Handler<MixedMsg> for SyncReceiver {
    type Error = Error;
    type Response = ();

    fn handle(&self, msg: MixedMsg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.counter.fetch_add(msg.0, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MixedMsg> for AsyncReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: MixedMsg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.counter.fetch_add(msg.0, Ordering::SeqCst);
        Ok(())
    }
}

/// Test sync and async handlers receiving same message type
#[tokio::test]
async fn test_mixed_handler_types_same_message() {
    let sync_counter = Arc::new(AtomicU32::new(0));
    let async_counter = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(SyncReceiver {
            counter: sync_counter.clone(),
        })
        .subscribe_sync::<MixedMsg>(8, Default::default())
        .done()
        .register(AsyncReceiver {
            counter: async_counter.clone(),
        })
        .subscribe_async::<MixedMsg>(8, Default::default())
        .done()
        .build();

    // Send messages
    for i in 1..=10u32 {
        b.send(MixedMsg(i)).await.unwrap();
    }

    b.flush_all().await;

    // Both should have sum = 55
    assert_eq!(sync_counter.load(Ordering::SeqCst), 55);
    assert_eq!(async_counter.load(Ordering::SeqCst), 55);

    b.close().await;
    poller.await;
}

/// Test high volume of messages to multiple receivers
#[tokio::test]
async fn test_multiple_receivers_high_volume() {
    let counter_a = Arc::new(AtomicU32::new(0));
    let counter_b = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ReceiverA {
            counter: counter_a.clone(),
        })
        .subscribe_async::<SharedMsg>(128, Default::default())
        .done()
        .register(ReceiverB {
            counter: counter_b.clone(),
        })
        .subscribe_async::<SharedMsg>(128, Default::default())
        .done()
        .build();

    // Send many messages
    let count = 1000u32;
    for _ in 0..count {
        b.send(SharedMsg(1)).await.unwrap();
    }

    b.flush_all().await;

    // Each receiver should have received count messages
    assert_eq!(counter_a.load(Ordering::SeqCst), count);
    assert_eq!(counter_b.load(Ordering::SeqCst), count);

    b.close().await;
    poller.await;
}

/// Test receivers can forward messages to each other
#[derive(Debug, Clone, Message)]
struct StartChain(pub u32);

#[derive(Debug, Clone, Message)]
struct ChainStep(pub u32);

#[derive(Debug, Clone, Message)]
struct GetChainResult;

#[derive(Debug, Clone, Message)]
struct ChainResult(pub u32);

struct ChainStarter {
    started: Arc<AtomicU32>,
}

struct ChainProcessor {
    processed: Arc<AtomicU32>,
}

#[async_trait]
impl AsyncHandler<StartChain> for ChainStarter {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: StartChain, bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.started.fetch_add(1, Ordering::SeqCst);
        // Forward to chain processor
        bus.send(ChainStep(msg.0 * 2)).await?;
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<ChainStep> for ChainProcessor {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: ChainStep, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.processed.fetch_add(msg.0, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<GetChainResult> for ChainProcessor {
    type Error = Error;
    type Response = ChainResult;

    async fn handle(&self, _: GetChainResult, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(ChainResult(self.processed.load(Ordering::SeqCst)))
    }
}

/// Test message forwarding between receivers
#[tokio::test]
async fn test_message_forwarding_between_receivers() {
    let started = Arc::new(AtomicU32::new(0));
    let processed = Arc::new(AtomicU32::new(0));

    let (b, poller) = Bus::build()
        .register(ChainStarter {
            started: started.clone(),
        })
        .subscribe_async::<StartChain>(8, Default::default())
        .done()
        .register(ChainProcessor {
            processed: processed.clone(),
        })
        .subscribe_async::<ChainStep>(8, Default::default())
        .subscribe_async::<GetChainResult>(8, Default::default())
        .done()
        .build();

    // Start chain with values 1, 2, 3, 4, 5
    for i in 1..=5u32 {
        b.send(StartChain(i)).await.unwrap();
    }

    b.flush_all().await;

    // Starter should have started 5 chains
    assert_eq!(started.load(Ordering::SeqCst), 5);

    // Processor should have processed doubled values: 2+4+6+8+10 = 30
    let result = b
        .request_we::<_, ChainResult, Error>(GetChainResult, Default::default())
        .await
        .unwrap();

    assert_eq!(result.0, 30);

    b.close().await;
    poller.await;
}

/// Test single message going to many receivers
#[tokio::test]
async fn test_fanout_to_many_receivers() {
    let counters: Vec<Arc<AtomicU32>> = (0..5).map(|_| Arc::new(AtomicU32::new(0))).collect();

    // Create receivers dynamically
    struct DynamicReceiver {
        counter: Arc<AtomicU32>,
    }

    #[async_trait]
    impl AsyncHandler<SharedMsg> for DynamicReceiver {
        type Error = Error;
        type Response = ();

        async fn handle(&self, msg: SharedMsg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
            self.counter.fetch_add(msg.0, Ordering::SeqCst);
            Ok(())
        }
    }

    let mut builder = Bus::build();
    for counter in &counters {
        builder = builder
            .register(DynamicReceiver {
                counter: counter.clone(),
            })
            .subscribe_async::<SharedMsg>(8, Default::default())
            .done();
    }

    let (b, poller) = builder.build();

    // Send single message
    b.send(SharedMsg(42)).await.unwrap();

    b.flush_all().await;

    // All receivers should have received the message
    for (i, counter) in counters.iter().enumerate() {
        assert_eq!(
            counter.load(Ordering::SeqCst),
            42,
            "Receiver {} didn't receive message",
            i
        );
    }

    b.close().await;
    poller.await;
}
