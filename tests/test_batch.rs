use std::sync::Arc;

use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error,
    receivers::BufferUnorderedBatchedConfig,
    AsyncBatchHandler, Bus, Message,
};
use parking_lot::Mutex;
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

struct TmpReceiver {
    batches: Arc<Mutex<Vec<Vec<i32>>>>,
}

#[async_trait]
impl AsyncBatchHandler<MsgI32> for TmpReceiver {
    type Error = Error;
    type Response = ();
    type InBatch = Vec<MsgI32>;
    type OutBatch = Vec<()>;

    async fn handle(
        &self,
        msg: Vec<MsgI32>,
        _bus: &Bus,
    ) -> Result<Vec<Self::Response>, Self::Error> {
        self.batches
            .lock()
            .push(msg.into_iter().map(|x| x.0).collect());

        Ok(vec![])
    }
}

#[tokio::test]
async fn test_batch() {
    let batches = Arc::new(Mutex::new(Vec::new()));

    let (b, poller) = Bus::build()
        .register(TmpReceiver {
            batches: batches.clone(),
        })
        .subscribe_batch_async::<MsgI32>(
            16,
            BufferUnorderedBatchedConfig {
                batch_size: 8,
                ..Default::default()
            },
        )
        .done()
        .build();

    for i in 1..100i32 {
        b.send(MsgI32(i)).await.unwrap();
    }

    let mut re = Vec::new();
    let mut counter = 1i32;
    for _ in 1..100i32 {
        let mut v = Vec::new();
        for _ in 0..8 {
            if counter >= 100 {
                break;
            }

            v.push(counter);

            counter += 1;
        }

        re.push(v);

        if counter >= 100 {
            break;
        }
    }

    println!("flush");
    b.flush_all().await;

    let mut lock = batches.lock();
    lock.sort_by(|a, b| a[0].cmp(&b[0]));

    assert_eq!(lock.as_slice(), re.as_slice());

    println!("close");
    b.close().await;

    println!("closed");

    poller.await;
    println!("[done]");
}
