use std::{sync::atomic::AtomicU32, time::Duration};

use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error,
    receivers::BufferUnorderedConfig,
    AsyncHandler, Bus, Message, Module,
};
use thiserror::Error;

#[derive(Debug, Error, MbError)]
enum Error {
    #[error("Error({0})")]
    Error(anyhow::Error),

    #[error("MyError")]
    MyError,
}

impl<M: Message> From<error::Error<M>> for Error {
    fn from(err: error::Error<M>) -> Self {
        Self::Error(err.into())
    }
}

#[derive(Debug, Clone, Message)]
struct Req(pub u32);

#[allow(dead_code)]
#[derive(Debug, Clone, Message)]
struct Resp(pub u32);

#[derive(Debug, Clone, Message)]
struct GetCount;

#[derive(Debug, Clone, Message)]
struct CountResult(pub u32);

struct TmpReceiver {
    counter: AtomicU32,
}

#[async_trait]
impl AsyncHandler<Req> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: Req, bus: &Bus) -> Result<Self::Response, Self::Error> {
        tokio::time::sleep(Duration::from_millis((msg.0 % 20) as _)).await;

        if msg.0 % 128 == 0 {
            return Err(Error::MyError);
        } else {
            bus.send(Resp(msg.0)).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<Resp> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, _msg: Resp, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        self.counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<GetCount> for TmpReceiver {
    type Error = Error;
    type Response = CountResult;

    async fn handle(&self, _: GetCount, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(CountResult(
            self.counter.load(std::sync::atomic::Ordering::SeqCst),
        ))
    }
}

fn module() -> Module {
    Module::new()
        .register(TmpReceiver {
            counter: AtomicU32::new(0),
        })
        .subscribe_async::<Req>(
            1024,
            BufferUnorderedConfig {
                buffer_size: 1024,
                max_parallel: 1024,
            },
        )
        .subscribe_async::<Resp>(1024, Default::default())
        .subscribe_async::<GetCount>(8, Default::default())
        .done()
}

#[tokio::test]
async fn test_sync() {
    let (b, poller) = Bus::build().add_module(module()).build();
    let cnt = 4u32;
    for i in 0..cnt {
        for j in 0..32768 {
            b.send(Req(i * 128 + j)).await.unwrap();
        }

        println!("{} sent", i);
    }

    println!("sent");

    b.flush_all().await;

    println!("flushed");

    assert_eq!(
        b.request_we::<_, CountResult, Error>(GetCount, Default::default())
            .await
            .unwrap()
            .0,
        cnt * 32768 - cnt * 256
    );

    b.close().await;

    println!("closed");

    poller.await;
}
