use std::sync::Arc;

use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error, AsyncBatchSynchronizedHandler, BatchSynchronizedHandler, Bus, Message,
};
use thiserror::Error;

#[derive(Debug, Error, Clone, MbError)]
enum Error {
    #[error("Error({0})")]
    Error(Arc<anyhow::Error>),
}

impl<M: Message> From<error::Error<M>> for Error {
    fn from(err: error::Error<M>) -> Self {
        Self::Error(Arc::new(err.into()))
    }
}

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgI32(i32);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgI16(i16);

struct TmpReceiver;

#[async_trait]
impl AsyncBatchSynchronizedHandler<MsgI32> for TmpReceiver {
    type Error = Error;
    type Response = ();
    type InBatch = Vec<MsgI32>;
    type OutBatch = Vec<()>;

    async fn handle(
        &mut self,
        msg: Vec<MsgI32>,
        _bus: &Bus,
    ) -> Result<Vec<Self::Response>, Self::Error> {
        println!("---> [i32; {}] {:?}", msg.len(), msg);

        Ok(vec![])
    }
}

impl BatchSynchronizedHandler<MsgI16> for TmpReceiver {
    type Error = Error;
    type Response = ();
    type InBatch = Vec<MsgI16>;
    type OutBatch = Vec<()>;

    fn handle(&mut self, msg: Vec<MsgI16>, _bus: &Bus) -> Result<Vec<Self::Response>, Self::Error> {
        println!("---> [i16; {}] {:?}", msg.len(), msg);
        Ok(vec![])
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register_unsync(TmpReceiver)
        .subscribe_batch_async::<MsgI32>(16, Default::default())
        .subscribe_batch_sync::<MsgI16>(16, Default::default())
        .done()
        .build();

    for i in 1..100i32 {
        b.send(MsgI32(i)).await.unwrap();
    }

    b.send(MsgI16(1i16)).await.unwrap();
    b.send(MsgI16(2i16)).await.unwrap();
    b.send(MsgI16(3i16)).await.unwrap();
    b.send(MsgI16(4i16)).await.unwrap();
    b.send(MsgI16(5i16)).await.unwrap();
    b.send(MsgI16(6i16)).await.unwrap();
    b.send(MsgI16(7i16)).await.unwrap();

    println!("flush");
    b.flush().await;

    println!("close");
    b.close().await;

    println!("closed");

    poller.await;
    println!("[done]");
}
