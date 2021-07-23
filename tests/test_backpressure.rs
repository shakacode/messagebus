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

#[derive(Debug, Clone, Message)]
struct MsgF32(pub f32);

struct TmpReceiver;

#[async_trait]
impl AsyncHandler<MsgF32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, _msg: MsgF32, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        std::thread::sleep(std::time::Duration::from_millis(100));
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

    b.send(MsgF32(32f32)).await.unwrap();
    b.send(MsgF32(32f32)).await.unwrap();

    assert!(b.try_send(MsgF32(32f32)).is_err());

    b.flush().await;
    b.close().await;
    poller.await;
}
