use async_trait::async_trait;
use messagebus::{receivers, Bus, SynchronizedHandler, AsyncSynchronizedHandler};
use receivers::SynchronizedConfig;

struct TmpReceiver;

impl SynchronizedHandler<f32> for TmpReceiver {
    type Error = anyhow::Error;
    type Response = ();

    fn handle(&mut self, msg: f32, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        
        // std::thread::sleep(std::time::Duration::from_millis(100));
        println!("---> f32 {}", msg);

        println!("done");
        Ok(())
    }
}

#[async_trait]
impl AsyncSynchronizedHandler<i16> for TmpReceiver {
    type Error = anyhow::Error;
    type Response = ();
    
    async fn handle(&mut self, msg: i16, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        
        std::thread::sleep(std::time::Duration::from_millis(100));
        println!("---> i16 {}", msg);

        println!("done");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register_unsync(TmpReceiver)
        .subscribe::<f32, receivers::SynchronizedSync<_>, _, _>(8, Default::default())
        .subscribe::<i16, receivers::SynchronizedAsync<_>, _, _>(8, Default::default())
        .done()
        .build();

    b.send(12.0f32).await.unwrap();
    b.send(1i16).await.unwrap();
    b.send(12.0f32).await.unwrap();
    b.send(1i16).await.unwrap();
    b.send(12.0f32).await.unwrap();
    b.send(1i16).await.unwrap();
    b.send(12.0f32).await.unwrap();
    b.send(1i16).await.unwrap();
    b.send(12.0f32).await.unwrap();
    b.send(1i16).await.unwrap();
    b.send(12.0f32).await.unwrap();
    b.send(1i16).await.unwrap();
    b.send(12.0f32).await.unwrap();
    b.send(1i16).await.unwrap();
    b.send(12.0f32).await.unwrap();
    b.send(1i16).await.unwrap();

    b.send(12.0f32).await.unwrap();
    b.send(1i16).await.unwrap();

    println!("closing");

    b.close().await;

    poller.await;

    println!("[done]");
}
