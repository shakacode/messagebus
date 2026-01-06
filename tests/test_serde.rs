use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error, AsyncHandler, Bus, Message, TypeTag,
};
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(shared, clone)]
#[type_tag("Msg")]
struct Msg {
    test1: u32,
    test2: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[message(shared, clone)]
#[type_tag("MsgResponse")]
struct MsgResponse {
    test1: u32,
    test2: String,
}

struct TmpReceiver;

#[async_trait]
impl AsyncHandler<Msg> for TmpReceiver {
    type Error = Error;
    type Response = MsgResponse;

    async fn handle(&self, msg: Msg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        Ok(MsgResponse {
            test1: msg.test1 * 2,
            test2: msg.test2 + ", World!",
        })
    }
}

#[tokio::test]
async fn test() {
    let (b, poller) = Bus::build()
        .register(TmpReceiver)
        .subscribe_async::<Msg>(8, Default::default())
        .done()
        .build();

    let type_tag: TypeTag = "Msg".into();
    let message = br#"{"test1":12,"test2":"Hello"}"#;

    let json = &mut serde_json::Deserializer::from_slice(message);
    let mut de = <dyn erased_serde::Deserializer>::erase(json);

    let val = b
        .request_deserialize(type_tag, &mut de, Default::default())
        .await
        .unwrap();

    let mut buff: Vec<u8> = Vec::new();
    let json = &mut serde_json::Serializer::new(&mut buff);
    let mut ser = <dyn erased_serde::Serializer>::erase(json);
    val.as_shared_ref()
        .unwrap()
        .erased_serialize(&mut ser)
        .unwrap();

    assert_eq!(val.type_tag(), TypeTag::from("MsgResponse"));
    assert_eq!(buff.as_slice(), br#"{"test1":24,"test2":"Hello, World!"}"#);

    b.flush_all().await;
    b.close().await;
    poller.await;
}
