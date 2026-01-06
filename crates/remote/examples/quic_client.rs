use messagebus::derive::Message;
use messagebus::error::GenericError;
use messagebus::{Bus, TypeTagged};
use messagebus_remote::relays::QuicClientRelay;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Message)]
#[namespace("example")]
#[message(shared, clone)]
pub struct Req {
    data: i32,
    text: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Message)]
#[namespace("example")]
#[message(shared, clone)]
pub struct Resp {
    data: i32,
    text: String,
}

#[tokio::main]
async fn main() {
    let relay = QuicClientRelay::new(
        "./examples/cert.der",
        "127.0.0.1:8083".parse().unwrap(),
        "localhost".into(),
        (
            vec![(
                Req::type_tag_(),
                Some((Resp::type_tag_(), GenericError::type_tag_())),
            )],
            vec![],
        ),
    )
    .unwrap();

    let (b, poller) = Bus::build().register_relay(relay).build();

    b.ready().await;
    println!("ready");

    let resp: Resp = b
        .request(
            Req {
                data: 12,
                text: String::from("test"),
            },
            Default::default(),
        )
        .await
        .unwrap();

    println!("resp {:?}", resp);

    b.flush_all().await;
    b.close().await;
    poller.await;
}
