use messagebus::{receivers, Bus, Handler, Result as MbusResult};

struct TmpReceiver;

impl Handler<f32> for TmpReceiver {
    fn handle(&self, msg: f32, _bus: &Bus) -> MbusResult {
        println!("---> f32 {}", msg);

        std::thread::sleep(std::time::Duration::from_secs(1));

        println!("done");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register(TmpReceiver)
        .subscribe::<f32, receivers::BufferUnorderedSync<_>>(receivers::BufferUnorderedConfig {
            buffer_size: 1,
            max_parallel: 1,
        })
        .done()
        .build();

    println!("sending 1");
    b.send(32f32).await.unwrap();

    println!("sending 2");
    b.send(32f32).await.unwrap();

    println!("sending 3");
    b.send(32f32).await.unwrap();

    println!("sending 4");
    b.send(32f32).await.unwrap();

    println!("sending 5");
    b.send(32f32).await.unwrap();

    println!("sending 6");
    b.send(32f32).await.unwrap();

    println!("sending 7");
    b.send(32f32).await.unwrap();

    println!("sending 8");
    b.send(32f32).await.unwrap();

    println!("sending 9");
    b.send(32f32).await.unwrap();

    println!("sending 10");
    b.send(32f32).await.unwrap();

    println!("sending 11");
    b.send(32f32).await.unwrap();

    println!("finish");

    poller.await;
}
