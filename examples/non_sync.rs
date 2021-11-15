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
struct MsgF32(pub f32);

#[derive(Debug, Clone, Message)]
struct MsgI16(pub i16);

struct TmpReceiver;

impl SynchronizedHandler<MsgF32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    fn handle(&mut self, msg: MsgF32, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        // std::thread::sleep(std::time::Duration::from_millis(100));
        println!("---> f32 {:?}", msg);

        println!("done");
        Ok(())
    }
}

#[async_trait]
impl AsyncSynchronizedHandler<MsgI16> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&mut self, msg: MsgI16, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        std::thread::sleep(std::time::Duration::from_millis(100));
        println!("---> i16 {:?}", msg);

        println!("done");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register_unsync(TmpReceiver)
        .subscribe_sync::<MsgF32>(8, Default::default())
        .subscribe_async::<MsgI16>(8, Default::default())
        .done()
        .build();

    b.send(MsgF32(12.0f32)).await.unwrap();
    b.send(MsgI16(1i16)).await.unwrap();
    b.send(MsgF32(12.0f32)).await.unwrap();
    b.send(MsgI16(1i16)).await.unwrap();
    b.send(MsgF32(12.0f32)).await.unwrap();
    b.send(MsgI16(1i16)).await.unwrap();
    b.send(MsgF32(12.0f32)).await.unwrap();
    b.send(MsgI16(1i16)).await.unwrap();
    b.send(MsgF32(12.0f32)).await.unwrap();
    b.send(MsgI16(1i16)).await.unwrap();
    b.send(MsgF32(12.0f32)).await.unwrap();
    b.send(MsgI16(1i16)).await.unwrap();
    b.send(MsgF32(12.0f32)).await.unwrap();
    b.send(MsgI16(1i16)).await.unwrap();
    b.send(MsgF32(12.0f32)).await.unwrap();
    b.send(MsgI16(1i16)).await.unwrap();

    b.send(MsgF32(12.0f32)).await.unwrap();
    b.send(MsgI16(1i16)).await.unwrap();

    println!("flush");

    b.flush_all().await;

    println!("closing");

    b.close().await;

    println!("closed");

    poller.await;

    println!("[done]");
}
