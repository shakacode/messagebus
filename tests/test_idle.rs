use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error, receivers, AsyncHandler, Bus, Message,
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

#[allow(dead_code)]
#[derive(Debug, Clone, Message)]
struct MsgF32(pub f32);

#[allow(dead_code)]
#[derive(Debug, Clone, Message)]
struct MsgF64(pub f64);

struct TmpReceiver;

#[async_trait]
impl AsyncHandler<MsgF64> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, _msg: MsgF64, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        std::thread::sleep(std::time::Duration::from_millis(100));

        Ok(())
    }
}
#[async_trait]
impl AsyncHandler<MsgF32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, _msg: MsgF32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        bus.send(MsgF64(12.0)).await.unwrap();
        bus.flush::<MsgF64>().await;

        Ok(())
    }
}

#[tokio::test]
async fn test_backpressure() {
    let (b, poller) = Bus::build()
        .register(TmpReceiver)
        .subscribe_async::<MsgF32>(
            1,
            receivers::BufferUnorderedConfig {
                buffer_size: 1,
                max_parallel: 1,
            },
        )
        .done()
        .build();

    b.send(MsgF32(10.0)).await.unwrap();
    // b.idle_all().await;
    b.flush_all().await;
    b.close().await;
    poller.await;
}
