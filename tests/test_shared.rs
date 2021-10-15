use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error, AsyncHandler, Bus, Message,
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

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize, Message)]
#[message(clone, shared)]
struct Msg;

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize, Message)]
#[message(clone, shared)]
struct SharedMsg<T>(T);

struct TmpReceiverContext {
    sync1: AtomicBool,
    sync2: AtomicBool,
}

struct TmpReceiver {
    ctx: Arc<TmpReceiverContext>,
}

#[async_trait]
impl AsyncHandler<SharedMsg<f32>> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(
        &self,
        _msg: SharedMsg<f32>,
        _bus: &Bus,
    ) -> Result<Self::Response, Self::Error> {
        self.ctx.sync1.store(true, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<Msg> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, _msg: Msg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.ctx.sync2.store(false, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn test_shared() {
    let ctx = Arc::new(TmpReceiverContext {
        sync1: AtomicBool::new(false),
        sync2: AtomicBool::new(false),
    });

    let (b, poller) = Bus::build()
        .register(TmpReceiver { ctx: ctx.clone() })
        .subscribe_async::<Msg>(8, Default::default())
        .subscribe_async::<SharedMsg<f32>>(8, Default::default())
        .done()
        .build();

    b.send_one(Msg).await.unwrap();
    b.send_one(SharedMsg(0.0f32)).await.unwrap();

    b.flush().await;
    b.close().await;
    poller.await;

    assert!(ctx.sync1.load(Ordering::Relaxed));
    assert!(!ctx.sync2.load(Ordering::Relaxed));
}
