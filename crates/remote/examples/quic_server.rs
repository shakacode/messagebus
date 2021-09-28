use messagebus::{error, Message, derive::{Message, Error as MbError}, AsyncHandler, Bus};
use messagebus_remote::relays::{QuicServerRelay};
use serde_derive::{Serialize, Deserialize};
use async_trait::async_trait;
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


#[derive(Serialize, Deserialize, Debug, Clone, Message)]
#[namespace("example")]
#[message(shared, clone)]
pub struct Req {
    data: i32,
    text: String
}

#[derive(Serialize, Deserialize, Debug, Clone, Message)]
#[namespace("example")]
#[message(shared, clone)]
pub struct Resp {
    data: i32,
    text: String
}


struct TmpReceiver;

#[async_trait]
impl AsyncHandler<Req> for TmpReceiver {
    type Error = Error;
    type Response = Resp;

    async fn handle(&self, msg: Req, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("TmpReceiver::handle {:?}", msg);
        Ok(Resp {
            data: msg.data + 12,
            text: format!("<< {} >>", msg.text),
        })
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver::sync");
        Ok(())
    }
}


#[tokio::main]
async fn main() {
    let relay = QuicServerRelay::new(
        "./examples/cert.key",
        "./examples/cert.der", 
        "0.0.0.0:8083".parse().unwrap(), 
        (vec![],
        vec![
            ("example::Req".into(), "example::Resp".into(), "GenericError".into())
        ])
    ).unwrap();

    let (b, poller) = Bus::build()
        .register_shared_message::<Req>()
        .register_shared_message::<Resp>()
        .register_relay(relay)
        .register(TmpReceiver)
            .subscribe_async::<Req>(8, Default::default())
        .done()
        .build();
    
    b.ready().await;

    println!("ready");

    poller.await;
}
