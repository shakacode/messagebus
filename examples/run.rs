#![feature(type_alias_impl_trait)]

use std::sync::Arc;

use futures::Future;
use messagebus::{
    bus::{Bus, MaskMatch},
    cell::MsgCell,
    derive::Message,
    error::Error,
    handler::Handler,
    receivers::wrapper::HandlerWrapper,
};

#[derive(Debug, Clone, Message)]
struct Msg(pub u32);

struct Test {
    inner: u32,
}

impl Handler<Msg> for Test {
    type Response = ();

    type InitFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;
    type HandleFuture<'a> = impl Future<Output = Result<Self::Response, Error>> + 'a;
    type FlushFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;
    type CloseFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;

    fn init(&self, _bus: &Bus) -> Self::InitFuture<'_> {
        async move { Ok(()) }
    }

    fn handle(&self, msg: &mut MsgCell<Msg>, _bus: &Bus) -> Self::HandleFuture<'_> {
        let msg = msg.get();

        async move {
            println!("msg {msg:?}");
            // let x = self.inner;
            Ok(())
        }
    }

    fn flush(&self, _bus: &Bus) -> Self::FlushFuture<'_> {
        async move { Ok(()) }
    }

    fn close(&self) -> Self::CloseFuture<'_> {
        async move { Ok(()) }
    }
}

async fn run() -> Result<(), Error> {
    let bus = Bus::new();
    bus.register(Test { inner: 12 })
        .handler(MaskMatch::all())
        .await?;

    let res: () = bus.request(Msg(13)).await?.result().await?;
    println!("request result got {:?}", res);

    bus.send(Msg(12)).await?;

    bus.close().await;
    bus.wait().await;

    Ok(())
}

#[tokio::main]
async fn main() {
    run().await.unwrap();
}
