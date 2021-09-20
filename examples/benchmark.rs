use std::time::Instant;

use async_trait::async_trait;
use messagebus::{
    derive::Message, error, receivers::BufferUnorderedConfig, AsyncHandler, Bus, Message,
};
use thiserror::Error;

#[derive(Debug, Error, messagebus::derive::Error)]
enum Error {
    #[error("Error({0})")]
    Error(anyhow::Error),
}

impl<M: Message> From<error::Error<M>> for Error {
    fn from(err: error::Error<M>) -> Self {
        Self::Error(err.into())
    }
}

struct TmpReceiver;
struct TmpReceiver2;

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

#[async_trait]
impl AsyncHandler<MsgF32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, _msg: MsgF32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        bus.send(MsgU16(1)).await?;
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgU16> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, _msg: MsgU16, bus: &Bus) -> Result<Self::Response, Self::Error> {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        bus.send(MsgU32(2)).await?;
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgU32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, _msg: MsgU32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        bus.send(MsgI32(3)).await?;
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgI32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, _msg: MsgI32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        bus.send(MsgI16(4)).await?;
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgI16> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, _msg: MsgI16, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<MsgI32> for TmpReceiver2 {
    type Error = Error;
    type Response = ();

    async fn handle(&self, _msg: MsgI32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        bus.send(MsgI16(5)).await?;

        Ok(())
    }
}

async fn iter(bus: &Bus) {
    for _ in 0..10_000 {
        bus.send(MsgF32(0.)).await.unwrap();
    }

    bus.flush().await;
}

#[tokio::main]
async fn main() {
    let cfg = BufferUnorderedConfig {
        buffer_size: 8,
        max_parallel: 8,
    };

    let (b, poller) = Bus::build()
        .register(TmpReceiver)
        .subscribe_async::<MsgF32>(cfg.buffer_size as _, cfg)
        .subscribe_async::<MsgU16>(cfg.buffer_size as _, cfg)
        .subscribe_async::<MsgU32>(cfg.buffer_size as _, cfg)
        .subscribe_async::<MsgI32>(cfg.buffer_size as _, cfg)
        .subscribe_async::<MsgI16>(cfg.buffer_size as _, cfg)
        .done()
        .register(TmpReceiver2)
        .subscribe_async::<MsgI32>(cfg.buffer_size as _, cfg)
        .done()
        .build();

    iter(&b).await;

    let count = 5;

    let mut time_sum = 0;
    for _ in 0..count {
        let inst = Instant::now();
        iter(&b).await;
        let diff = inst.elapsed();

        time_sum += diff.as_micros();
    }

    println!("Avg time: {:.4}", time_sum as f64 / (count as f64 * 1000.0));

    println!("flush");
    b.flush().await;

    println!("close");
    b.close().await;

    println!("closed");

    poller.await;
    println!("[done]");
}
