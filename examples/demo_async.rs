use async_trait::async_trait;
use messagebus::{receivers, AsyncHandler, Bus, Handler};

struct TmpReceiver;
struct TmpReceiver2;

#[async_trait]
impl AsyncHandler<f32> for TmpReceiver {
    type Error = anyhow::Error;
    type Response = ();

    async fn handle(&self, msg: f32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        bus.send(1u16).await?;

        println!("TmpReceiver ---> f32 {}", msg);

        Ok(())
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver f32: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<u16> for TmpReceiver {
    type Error = anyhow::Error;
    type Response = ();

    async fn handle(&self, msg: u16, bus: &Bus) -> Result<Self::Response, Self::Error> {
        bus.send(2u32).await?;
        println!("TmpReceiver ---> u16 {}", msg);

        Ok(())
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver u16: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<u32> for TmpReceiver {
    type Error = anyhow::Error;
    type Response = ();

    async fn handle(&self, msg: u32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        bus.send(3i32).await?;
        println!("TmpReceiver ---> u32 {}", msg);

        Ok(())
    }
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver u32: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<i32> for TmpReceiver {
    type Error = anyhow::Error;
    type Response = ();

    async fn handle(&self, msg: i32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        bus.send(4i16).await?;
        println!("TmpReceiver ---> i32 {}", msg);

        Ok(())
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver i32: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<i16> for TmpReceiver {
    type Error = anyhow::Error;
    type Response = ();

    async fn handle(&self, msg: i16, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("TmpReceiver ---> i16 {}", msg);

        Ok(())
    }
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver i16: sync");

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<i32> for TmpReceiver2 {
    type Error = anyhow::Error;
    type Response = ();

    async fn handle(&self, msg: i32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("!!!! TmpReceiver2: ---> 2 i32 {}", msg);

        bus.send(5i16).await?;

        Ok(())
    }
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver2: i32: sync");

        Ok(())
    }
}

impl Handler<i16> for TmpReceiver2 {
    type Error = anyhow::Error;
    type Response = ();

    fn handle(&self, msg: i16, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("TmpReceiver2: ---> 2 i16 {}", msg);

        Ok(())
    }

    fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver2: i16: sync");

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register(TmpReceiver)
        .subscribe::<f32, receivers::BufferUnorderedAsync<_>, _, _>(8, Default::default())
        .subscribe::<u16, receivers::BufferUnorderedAsync<_>, _, _>(8, Default::default())
        .subscribe::<u32, receivers::BufferUnorderedAsync<_>, _, _>(8, Default::default())
        .subscribe::<i32, receivers::BufferUnorderedAsync<_>, _, _>(8, Default::default())
        .subscribe::<i16, receivers::BufferUnorderedAsync<_>, _, _>(8, Default::default())
        .done()
        .register(TmpReceiver2)
            .subscribe::<i32, receivers::BufferUnorderedAsync<_>, _, _>(8, Default::default())
            .subscribe::<i16, receivers::BufferUnorderedSync<_>, _, _>(8, Default::default())
        .done()
    .build();

    b.send(0f32).await.unwrap();

    println!("flush");
    b.flush().await;

    println!("close");
    b.close().await;

    poller.await;
    println!("[done]");
}
