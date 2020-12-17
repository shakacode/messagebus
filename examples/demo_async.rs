use async_trait::async_trait;
use messagebus::{receivers, AsyncHandler, Bus, Handler, Result as MbusResult};

struct TmpReceiver;
struct TmpReceiver2;

#[async_trait]
impl AsyncHandler<f32> for TmpReceiver {
    async fn handle(&self, msg: f32, bus: &Bus) -> MbusResult {
        bus.send(1u16).await?;

        println!("---> f32 {}", msg);

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<u16> for TmpReceiver {
    async fn handle(&self, msg: u16, bus: &Bus) -> MbusResult {
        bus.send(2u32).await?;
        println!("---> u16 {}", msg);

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<u32> for TmpReceiver {
    async fn handle(&self, msg: u32, bus: &Bus) -> MbusResult {
        bus.send(3i32).await?;
        println!("---> u32 {}", msg);

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<i32> for TmpReceiver {
    async fn handle(&self, msg: i32, bus: &Bus) -> MbusResult {
        bus.send(4i16).await?;
        println!("---> i32 {}", msg);

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<i16> for TmpReceiver {
    async fn handle(&self, msg: i16, _bus: &Bus) -> MbusResult {
        println!("---> i16 {}", msg);

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<i32> for TmpReceiver2 {
    async fn handle(&self, msg: i32, bus: &Bus) -> MbusResult {
        bus.send(5i16).await?;
        println!("---> 2 i32 {}", msg);

        Ok(())
    }
}

impl Handler<i16> for TmpReceiver2 {
    fn handle(&self, msg: i16, _bus: &Bus) -> MbusResult {
        println!("---> 2 i16 {}", msg);

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register(TmpReceiver)
        .subscribe::<f32, receivers::BufferUnorderedAsync<_>>(Default::default())
        .subscribe::<u16, receivers::BufferUnorderedAsync<_>>(Default::default())
        .subscribe::<u32, receivers::BufferUnorderedAsync<_>>(Default::default())
        .subscribe::<i32, receivers::BufferUnorderedAsync<_>>(Default::default())
        .subscribe::<i16, receivers::BufferUnorderedAsync<_>>(Default::default())
        .done()
        .register(TmpReceiver2)
        .subscribe::<i32, receivers::BufferUnorderedAsync<_>>(Default::default())
        .subscribe::<i16, receivers::BufferUnorderedSync<_>>(Default::default())
        .done()
        .build();

    b.send(0f32).await.unwrap();
    poller.await
}
