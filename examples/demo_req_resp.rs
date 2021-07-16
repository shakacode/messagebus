use core::f32;

use async_trait::async_trait;
use messagebus::{
    derive::Message,
    error::{self, StdSyncSendError},
    AsyncHandler, Bus, Message,
};
use thiserror::Error;

#[derive(Debug, Error, messagebus::derive::Error)]
enum Error {
    #[error("Error({0})")]
    Error(anyhow::Error),
}

impl<M: Message, E: StdSyncSendError> From<error::Error<M, E>> for Error {
    fn from(err: error::Error<M, E>) -> Self {
        Self::Error(err.into())
    }
}

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgF64(pub f64);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgF32(pub f32);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgI32(pub i32);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgU32(pub u32);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgU16(pub u16);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgI16(pub i16);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgU8(pub u8);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgI8(pub i8);

struct TmpReceiver1;
struct TmpReceiver2;

#[async_trait]
impl AsyncHandler<MsgI32> for TmpReceiver1 {
    type Error = Error;
    type Response = MsgF32;

    async fn handle(&self, msg: MsgI32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        let resp1 = bus
            .request::<_, MsgF32>(MsgI16(10i16), Default::default())
            .await?;
        let resp2 = bus
            .request::<_, MsgF32>(MsgU16(20u16), Default::default())
            .await?;

        Ok(MsgF32(msg.0 as f32 + resp1.0 + resp2.0))
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver1 i32: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgU32> for TmpReceiver1 {
    type Error = Error;
    type Response = MsgF32;

    async fn handle(&self, msg: MsgU32, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(MsgF32(msg.0 as _))
    }
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver1 u32: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgI16> for TmpReceiver1 {
    type Error = Error;
    type Response = MsgF32;

    async fn handle(&self, msg: MsgI16, bus: &Bus) -> Result<Self::Response, Self::Error> {
        let resp1 = bus
            .request::<_, MsgF32>(MsgI8(1i8), Default::default())
            .await?;
        let resp2 = bus
            .request::<_, MsgF32>(MsgU8(2u8), Default::default())
            .await?;

        Ok(MsgF32(msg.0 as f32 + resp1.0 + resp2.0))
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver i16: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgU16> for TmpReceiver1 {
    type Error = Error;
    type Response = MsgF32;

    async fn handle(&self, msg: MsgU16, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(MsgF32(msg.0 as _))
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver i16: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgI8> for TmpReceiver1 {
    type Error = Error;
    type Response = MsgF32;

    async fn handle(&self, msg: MsgI8, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(MsgF32(msg.0 as _))
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver1 i8: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgU8> for TmpReceiver1 {
    type Error = Error;
    type Response = MsgF32;

    async fn handle(&self, msg: MsgU8, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(MsgF32(msg.0 as _))
    }
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver1 u8: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgF64> for TmpReceiver2 {
    type Error = Error;
    type Response = MsgF64;

    async fn handle(&self, msg: MsgF64, bus: &Bus) -> Result<Self::Response, Self::Error> {
        let resp1 = bus
            .request::<_, MsgF32>(MsgI32(100i32), Default::default())
            .await?
            .0 as f64;
        let resp2 = bus
            .request::<_, MsgF32>(MsgU32(200u32), Default::default())
            .await?
            .0 as f64;
        let resp3 = bus
            .request::<_, MsgF32>(MsgF32(300f32), Default::default())
            .await?
            .0 as f64;

        Ok(MsgF64(msg.0 + resp1 + resp2 + resp3))
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver1 f64: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgF32> for TmpReceiver2 {
    type Error = Error;
    type Response = MsgF32;

    async fn handle(&self, msg: MsgF32, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(msg)
    }
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver2: f32: sync");

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register(TmpReceiver1)
        .subscribe_async::<MsgI32>(8, Default::default())
        .subscribe_async::<MsgU32>(8, Default::default())
        .subscribe_async::<MsgI16>(8, Default::default())
        .subscribe_async::<MsgU16>(8, Default::default())
        .subscribe_async::<MsgI8>(8, Default::default())
        .subscribe_async::<MsgU8>(8, Default::default())
        .done()
        .register(TmpReceiver2)
        .subscribe_async::<MsgF32>(8, Default::default())
        .subscribe_async::<MsgF64>(8, Default::default())
        .done()
        .build();

    println!(
        "plain {:?}",
        b.request_we::<_, MsgF64, Error>(MsgF64(1000f64), Default::default())
            .await
    );

    println!(
        "boxed {:?}",
        b.request_boxed(Box::new(MsgF64(1000.)), Default::default())
            .await
    );

    println!("flush");
    b.flush().await;

    println!("close");
    b.close().await;

    println!("closed");

    poller.await;
    println!("[done]");
}
