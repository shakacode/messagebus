use async_trait::async_trait;
use messagebus::{error::Error, receivers, AsyncBatchHandler, BatchHandler, Bus};

struct TmpReceiver;

#[async_trait]
impl AsyncBatchHandler<i32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: Vec<i32>, _bus: &Bus) -> Result<Vec<Self::Response>, Self::Error> {
        println!("---> [i32; {}] {:?}", msg.len(), msg);

        Ok(vec![])
    }
}

impl BatchHandler<i16> for TmpReceiver {
    type Error = Error;
    type Response = ();

    fn handle(&self, msg: Vec<i16>, _bus: &Bus) -> Result<Vec<Self::Response>, Self::Error> {
        println!("---> [i16; {}] {:?}", msg.len(), msg);
        Ok(vec![])
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register(TmpReceiver)
        .subscribe::<i32, receivers::BufferUnorderedBatchedAsync<_>, _, _>(16, Default::default())
        .subscribe::<i16, receivers::BufferUnorderedBatchedSync<_>, _, _>(16, Default::default())
        .done()
        .build();

    for i in 1..100 {
        b.send(i).await.unwrap();
    }

    b.send(1i16).await.unwrap();
    b.send(2i16).await.unwrap();
    b.send(3i16).await.unwrap();
    b.send(4i16).await.unwrap();
    b.send(5i16).await.unwrap();
    b.send(6i16).await.unwrap();
    b.send(7i16).await.unwrap();

    println!("flush");
    b.flush().await;

    println!("close");
    b.close().await;

    println!("closed");

    poller.await;
    println!("[done]");
}
