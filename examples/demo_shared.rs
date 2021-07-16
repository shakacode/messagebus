use async_trait::async_trait;
use messagebus::{error, AsyncHandler, Bus, Message};
use thiserror::Error;

#[derive(Debug, Error)]
enum Error {
    #[error("Error({0})")]
    Error(anyhow::Error),
}

impl<M: Message> From<error::Error<M>> for Error {
    fn from(err: error::Error<M>) -> Self {
        Self::Error(err.into())
    }
}

#[derive(Debug, Clone)]
struct Msg;

struct TmpReceiver;

#[async_trait]
impl AsyncHandler<Msg> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: Msg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("---> f32 {:?}", msg);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register(TmpReceiver)
        .subscribe_local_async::<Msg>(8, Default::default())
        .done()
        .build();

    b.send_local_one(Msg).await.unwrap();

    println!("flushing");
    b.flush().await;

    println!("closing");
    b.close().await;

    println!("closed");
    poller.await;

    println!("[done]");
}
