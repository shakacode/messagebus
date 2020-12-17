use messagebus::{receivers, Bus, Result as MbusResult, SynchronizedHandler};
use receivers::SynchronizedConfig;

struct TmpReceiver;

impl SynchronizedHandler<f32> for TmpReceiver {
    fn handle(&mut self, msg: f32, _bus: &Bus) -> MbusResult {
        println!("---> f32 {}", msg);

        std::thread::sleep(std::time::Duration::from_secs(1));

        println!("done");
        Ok(())
    }
}

impl SynchronizedHandler<i16> for TmpReceiver {
    fn handle(&mut self, msg: i16, _bus: &Bus) -> MbusResult {
        println!("---> i16 {}", msg);

        std::thread::sleep(std::time::Duration::from_secs(1));

        println!("done");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register_unsync(TmpReceiver)
        .subscribe::<f32, receivers::SynchronizedSync<_>>(SynchronizedConfig { buffer_size: 1 })
        .subscribe::<i16, receivers::SynchronizedSync<_>>(Default::default())
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

    println!("finish");

    poller.await;
}
