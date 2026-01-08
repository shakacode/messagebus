use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error, AsyncSynchronizedHandler, Bus, Message, SynchronizedHandler,
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
struct MsgIncrement(pub u32);

#[derive(Debug, Clone, Message)]
struct MsgDecrement(pub u32);

#[derive(Debug, Clone, Message)]
struct MsgGetState;

#[derive(Debug, Clone, Message)]
struct StateResult(pub i64);

#[derive(Debug, Clone, Message)]
struct MsgAsyncIncrement(pub u32);

#[derive(Debug, Clone, Message)]
struct MsgAsyncDecrement(pub u32);

#[derive(Debug, Clone, Message)]
struct MsgAsyncGetState;

#[derive(Debug, Clone, Message)]
struct AsyncStateResult(pub i64);

/// A receiver that maintains mutable state
struct StatefulReceiver {
    state: i64,
    operations: Vec<String>,
}

impl StatefulReceiver {
    fn new() -> Self {
        Self {
            state: 0,
            operations: Vec::new(),
        }
    }
}

impl SynchronizedHandler<MsgIncrement> for StatefulReceiver {
    type Error = Error;
    type Response = ();

    fn handle(&mut self, msg: MsgIncrement, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.state += msg.0 as i64;
        self.operations.push(format!("inc({})", msg.0));
        Ok(())
    }
}

impl SynchronizedHandler<MsgDecrement> for StatefulReceiver {
    type Error = Error;
    type Response = ();

    fn handle(&mut self, msg: MsgDecrement, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.state -= msg.0 as i64;
        self.operations.push(format!("dec({})", msg.0));
        Ok(())
    }
}

impl SynchronizedHandler<MsgGetState> for StatefulReceiver {
    type Error = Error;
    type Response = StateResult;

    fn handle(&mut self, _: MsgGetState, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(StateResult(self.state))
    }
}

/// Test SynchronizedHandler with mutable state
#[tokio::test]
async fn test_synchronized_handler_basic() {
    let (b, poller) = Bus::build()
        .register_unsync(StatefulReceiver::new())
        .subscribe_sync::<MsgIncrement>(8, Default::default())
        .subscribe_sync::<MsgDecrement>(8, Default::default())
        .subscribe_sync::<MsgGetState>(8, Default::default())
        .done()
        .build();

    // Increment by 10
    b.send(MsgIncrement(10)).await.unwrap();

    // Decrement by 3
    b.send(MsgDecrement(3)).await.unwrap();

    // Increment by 5
    b.send(MsgIncrement(5)).await.unwrap();

    b.flush_all().await;

    // Get the state
    let result = b
        .request_we::<_, StateResult, Error>(MsgGetState, Default::default())
        .await
        .unwrap();

    assert_eq!(result.0, 12); // 10 - 3 + 5 = 12

    b.close().await;
    poller.await;
}

/// Test that SynchronizedHandler processes messages sequentially
#[tokio::test]
async fn test_synchronized_handler_sequential() {
    let (b, poller) = Bus::build()
        .register_unsync(StatefulReceiver::new())
        .subscribe_sync::<MsgIncrement>(8, Default::default())
        .subscribe_sync::<MsgGetState>(8, Default::default())
        .done()
        .build();

    // Send multiple increments
    for i in 1..=5 {
        b.send(MsgIncrement(i)).await.unwrap();
    }

    b.flush_all().await;

    // Get the state - should be 1+2+3+4+5 = 15
    let result = b
        .request_we::<_, StateResult, Error>(MsgGetState, Default::default())
        .await
        .unwrap();

    assert_eq!(result.0, 15);

    b.close().await;
    poller.await;
}

/// Async stateful receiver
struct AsyncStatefulReceiver {
    state: i64,
    operations: Vec<String>,
}

impl AsyncStatefulReceiver {
    fn new() -> Self {
        Self {
            state: 0,
            operations: Vec::new(),
        }
    }
}

#[async_trait]
impl AsyncSynchronizedHandler<MsgAsyncIncrement> for AsyncStatefulReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(
        &mut self,
        msg: MsgAsyncIncrement,
        _bus: &Bus,
    ) -> Result<Self::Response, Self::Error> {
        // Simulate async work
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        self.state += msg.0 as i64;
        self.operations.push(format!("async_inc({})", msg.0));
        Ok(())
    }
}

#[async_trait]
impl AsyncSynchronizedHandler<MsgAsyncDecrement> for AsyncStatefulReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(
        &mut self,
        msg: MsgAsyncDecrement,
        _bus: &Bus,
    ) -> Result<Self::Response, Self::Error> {
        // Simulate async work
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        self.state -= msg.0 as i64;
        self.operations.push(format!("async_dec({})", msg.0));
        Ok(())
    }
}

#[async_trait]
impl AsyncSynchronizedHandler<MsgAsyncGetState> for AsyncStatefulReceiver {
    type Error = Error;
    type Response = AsyncStateResult;

    async fn handle(
        &mut self,
        _: MsgAsyncGetState,
        _bus: &Bus,
    ) -> Result<Self::Response, Self::Error> {
        Ok(AsyncStateResult(self.state))
    }
}

/// Test AsyncSynchronizedHandler with mutable state
#[tokio::test]
async fn test_async_synchronized_handler_basic() {
    let (b, poller) = Bus::build()
        .register_unsync(AsyncStatefulReceiver::new())
        .subscribe_async::<MsgAsyncIncrement>(8, Default::default())
        .subscribe_async::<MsgAsyncDecrement>(8, Default::default())
        .subscribe_async::<MsgAsyncGetState>(8, Default::default())
        .done()
        .build();

    // Increment by 20
    b.send(MsgAsyncIncrement(20)).await.unwrap();

    // Decrement by 7
    b.send(MsgAsyncDecrement(7)).await.unwrap();

    // Increment by 3
    b.send(MsgAsyncIncrement(3)).await.unwrap();

    b.flush_all().await;

    // Get the state
    let result = b
        .request_we::<_, AsyncStateResult, Error>(MsgAsyncGetState, Default::default())
        .await
        .unwrap();

    assert_eq!(result.0, 16); // 20 - 7 + 3 = 16

    b.close().await;
    poller.await;
}

/// Test AsyncSynchronizedHandler with many messages
#[tokio::test]
async fn test_async_synchronized_handler_many_messages() {
    let (b, poller) = Bus::build()
        .register_unsync(AsyncStatefulReceiver::new())
        .subscribe_async::<MsgAsyncIncrement>(16, Default::default())
        .subscribe_async::<MsgAsyncGetState>(8, Default::default())
        .done()
        .build();

    // Send many increments
    for i in 1..=100 {
        b.send(MsgAsyncIncrement(i)).await.unwrap();
    }

    b.flush_all().await;

    // Get the state - should be sum of 1 to 100 = 5050
    let result = b
        .request_we::<_, AsyncStateResult, Error>(MsgAsyncGetState, Default::default())
        .await
        .unwrap();

    assert_eq!(result.0, 5050);

    b.close().await;
    poller.await;
}

/// Combined receiver that handles both sync and async messages
struct CombinedStatefulReceiver {
    sync_count: i64,
    async_count: i64,
}

impl CombinedStatefulReceiver {
    fn new() -> Self {
        Self {
            sync_count: 0,
            async_count: 0,
        }
    }
}

impl SynchronizedHandler<MsgIncrement> for CombinedStatefulReceiver {
    type Error = Error;
    type Response = ();

    fn handle(&mut self, msg: MsgIncrement, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.sync_count += msg.0 as i64;
        Ok(())
    }
}

#[async_trait]
impl AsyncSynchronizedHandler<MsgAsyncIncrement> for CombinedStatefulReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(
        &mut self,
        msg: MsgAsyncIncrement,
        _bus: &Bus,
    ) -> Result<Self::Response, Self::Error> {
        self.async_count += msg.0 as i64;
        Ok(())
    }
}

impl SynchronizedHandler<MsgGetState> for CombinedStatefulReceiver {
    type Error = Error;
    type Response = StateResult;

    fn handle(&mut self, _: MsgGetState, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(StateResult(self.sync_count + self.async_count))
    }
}

/// Test combined sync and async handlers on same receiver
#[tokio::test]
async fn test_combined_sync_async_handlers() {
    let (b, poller) = Bus::build()
        .register_unsync(CombinedStatefulReceiver::new())
        .subscribe_sync::<MsgIncrement>(8, Default::default())
        .subscribe_async::<MsgAsyncIncrement>(8, Default::default())
        .subscribe_sync::<MsgGetState>(8, Default::default())
        .done()
        .build();

    // Send sync increments
    b.send(MsgIncrement(5)).await.unwrap();
    b.send(MsgIncrement(10)).await.unwrap();

    // Send async increments
    b.send(MsgAsyncIncrement(3)).await.unwrap();
    b.send(MsgAsyncIncrement(7)).await.unwrap();

    b.flush_all().await;

    // Get the combined state
    let result = b
        .request_we::<_, StateResult, Error>(MsgGetState, Default::default())
        .await
        .unwrap();

    assert_eq!(result.0, 25); // (5+10) + (3+7) = 25

    b.close().await;
    poller.await;
}

/// Test sync handler with error
#[derive(Debug, Clone, Message)]
struct MsgMayFail(pub bool);

struct FailableReceiver {
    success_count: u32,
}

impl FailableReceiver {
    fn new() -> Self {
        Self { success_count: 0 }
    }
}

#[derive(Debug, Error, MbError)]
enum FailableError {
    #[error("Error({0})")]
    Error(anyhow::Error),

    #[error("Intentional failure")]
    IntentionalFailure,
}

impl<M: Message> From<error::Error<M>> for FailableError {
    fn from(err: error::Error<M>) -> Self {
        Self::Error(err.into())
    }
}

impl SynchronizedHandler<MsgMayFail> for FailableReceiver {
    type Error = FailableError;
    type Response = ();

    fn handle(&mut self, msg: MsgMayFail, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        if msg.0 {
            Err(FailableError::IntentionalFailure)
        } else {
            self.success_count += 1;
            Ok(())
        }
    }
}

/// Test that errors don't break the receiver
#[tokio::test]
async fn test_synchronized_handler_with_errors() {
    let (b, poller) = Bus::build()
        .register_unsync(FailableReceiver::new())
        .subscribe_sync::<MsgMayFail>(8, Default::default())
        .done()
        .build();

    // Send mix of failing and succeeding messages
    b.send(MsgMayFail(false)).await.unwrap(); // success
    b.send(MsgMayFail(true)).await.unwrap(); // fail
    b.send(MsgMayFail(false)).await.unwrap(); // success
    b.send(MsgMayFail(false)).await.unwrap(); // success

    b.flush_all().await;

    b.close().await;
    poller.await;
}
