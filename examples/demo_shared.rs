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

#[derive(Debug, Clone, Message)]
struct Msg;

#[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize, Message)]
#[message(clone, shared)]
struct SharedMsg<T: core::fmt::Debug + Clone + serde::Serialize + Send + Sync + Unpin + 'static>(
    #[serde(bound(deserialize = "T: serde::Deserialize<'de>"))] T,
);

struct TmpReceiver;

#[async_trait]
impl AsyncHandler<Msg> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: Msg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("---> {:?}: {}", msg, msg.as_shared_ref().is_some());
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<SharedMsg<f32>> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: SharedMsg<f32>, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("---> {:?}: {}", msg, msg.as_shared_ref().is_some());
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register(TmpReceiver)
        .subscribe_async::<Msg>(8, Default::default())
        .subscribe_async::<SharedMsg<f32>>(8, Default::default())
        .done()
        .build();

    b.send_one(Msg).await.unwrap();
    b.send_one(SharedMsg(0.0f32)).await.unwrap();

    println!("flushing");
    b.flush().await;

    println!("closing");
    b.close().await;

    println!("closed");
    poller.await;

    println!("[done]");
}
