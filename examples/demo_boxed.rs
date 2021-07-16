use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error, AsyncHandler, Bus, Handler, Message,
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
struct MsgF32(f32);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgU16(u16);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgU32(u32);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgI32(i32);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgI16(i16);

struct TmpReceiver;
struct TmpReceiver2;

#[async_trait]
impl AsyncHandler<MsgF32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: MsgF32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        bus.send(MsgU16(1u16)).await?;

        println!("TmpReceiver ---> {:?}", msg);

        Ok(())
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver f32: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgU16> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: MsgU16, bus: &Bus) -> Result<Self::Response, Self::Error> {
        bus.send(MsgU32(2u32)).await?;
        println!("TmpReceiver ---> {:?}", msg);

        Ok(())
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver u16: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgU32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: MsgU32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        bus.send(MsgI32(3i32)).await?;
        println!("TmpReceiver ---> {:?}", msg);

        Ok(())
    }
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver u32: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgI32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: MsgI32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        bus.send(MsgI16(4i16)).await?;
        println!("TmpReceiver ---> {:?}", msg);

        Ok(())
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver i32: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgI16> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: MsgI16, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("TmpReceiver ---> {:?}", msg);

        Ok(())
    }
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver i16: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgI32> for TmpReceiver2 {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: MsgI32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("TmpReceiver2: ---> {:?}", msg);

        bus.send(MsgI16(5i16)).await?;

        Ok(())
    }
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver2: i32: sync");

        Ok(())
    }
}

impl Handler<MsgI16> for TmpReceiver2 {
    type Error = Error;
    type Response = ();

    fn handle(&self, msg: MsgI16, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("TmpReceiver2: ---> {:?}", msg);

        Ok(())
    }

    fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver2: i16: sync");

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (b, poller) = Bus::build()
        .register(TmpReceiver)
        .subscribe_async::<MsgF32>(8, Default::default())
        .subscribe_async::<MsgU16>(8, Default::default())
        .subscribe_async::<MsgU32>(8, Default::default())
        .subscribe_async::<MsgI32>(8, Default::default())
        .subscribe_async::<MsgI16>(8, Default::default())
        .done()
        .register(TmpReceiver2)
        .subscribe_async::<MsgI32>(8, Default::default())
        .subscribe_sync::<MsgI16>(8, Default::default())
        .done()
        .build();

    b.send(MsgF32(0f32)).await.unwrap();
    println!("flush");

    b.flush().await;

    println!("sending boxed variant");

    b.send_boxed(Box::new(MsgF32(0f32)), Default::default())
        .await
        .unwrap();

    println!("flush");
    b.flush().await;

    println!("close");
    b.close().await;

    println!("closed");

    poller.await;
    println!("[done]");
}
