#![feature(type_alias_impl_trait)]

use std::sync::Arc;

use futures::Future;
use messagebus::{
    bus::{Bus, MaskMatch},
    cell::MsgCell,
    derive_message_clone,
    error::Error,
    handler::Handler,
    receivers::wrapper::HandlerWrapper,
};

#[derive(Debug, Clone)]
struct Msg(pub u32);
derive_message_clone!(EXAMPLE_MSG, Msg, "example::Msg");

struct Test {
    inner: u32,
}

impl Handler<Msg> for Test {
    type Response = Msg;
    type HandleFuture<'a> = impl Future<Output = Result<Self::Response, Error>> + 'a;
    type FlushFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;
    type CloseFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;

    fn handle(&self, msg: &mut MsgCell<Msg>, _bus: &Bus) -> Self::HandleFuture<'_> {
        let msg = msg.get();

        async move {
            println!("msg {msg:?}");
            let x = self.inner;
            Ok(Msg(x + msg.0))
        }
    }

    fn flush(&mut self, _bus: &Bus) -> Self::FlushFuture<'_> {
        async move { Ok(()) }
    }

    fn close(&mut self) -> Self::CloseFuture<'_> {
        async move { Ok(()) }
    }
}

async fn run() -> Result<(), Error> {
    let bus = Bus::new();

    let wrapper = HandlerWrapper::new(Arc::new(Test { inner: 12 }));
    bus.register(wrapper, MaskMatch::all());

    let res: Msg = bus.request(Msg(13)).await?.result().await?;
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
