#![feature(type_alias_impl_trait)]

use std::sync::Arc;
use std::time::Duration;

use futures::Future;
use messagebus::{
    bus::{Bus, MaskMatch},
    cell::MsgCell,
    derive,
    error::{Error, ErrorKind},
    handler::{Handler, MessageProducer},
    receivers::{producer::ProducerWrapper, wrapper::HandlerWrapper},
};

#[derive(Debug, Clone, derive::Message)]
struct Msg(pub u64);

#[derive(Debug, Clone, derive::Message)]
struct StartMsg(u64);

struct Test {}

impl MessageProducer<StartMsg> for Test {
    type Message = Msg;
    type Context = u64;
    type NextFuture<'a> = impl Future<Output = Result<Self::Message, Error>> + 'a;
    type StartFuture<'a> = impl Future<Output = Result<Self::Context, Error>> + 'a;
    type CloseFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;

    fn start(&self, msg: &mut MsgCell<StartMsg>, _: &Bus) -> Self::StartFuture<'_> {
        let start_from = msg.get().0;
        async move {
            println!("start {}", start_from);

            tokio::time::sleep(Duration::from_millis(1000)).await;
            Ok(start_from)
        }
    }

    fn next<'a>(&'a self, ctx: &'a mut Self::Context, _: &Bus) -> Self::NextFuture<'a> {
        async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let curr = *ctx;
            *ctx += 1;
            let msg = Msg(curr);
            println!("next #{}", msg.0);
            if msg.0 == 99 || msg.0 == 199 {
                return Err(ErrorKind::ProducerFinished.into());
            }
            Ok(msg)
        }
    }

    fn close(&self, _ctx: Self::Context) -> Self::CloseFuture<'_> {
        async move { Ok(()) }
    }
}

impl Handler<Msg> for Test {
    type Response = ();
    type HandleFuture<'a> = impl Future<Output = Result<Self::Response, Error>> + 'a;
    type FlushFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;
    type CloseFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;

    fn handle(&self, msg: &mut MsgCell<Msg>, _bus: &Bus) -> Self::HandleFuture<'_> {
        let msg = msg.get();

        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            println!("handing #{}", msg.0);
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
    let test = Arc::new(Test {});
    // bus.register(ProducerWrapper::new(test.clone()), MaskMatch::all());
    // bus.register(
    //     Unordered::new(HandlerWrapper::new(test), 25),
    //     MaskMatch::all(),
    // );

    // println!("111");
    // bus.start_producer(StartMsg(0)).await?;

    // println!("222");
    // bus.start_producer(StartMsg(100)).await?;

    println!("333");
    bus.close().await;
    bus.wait().await;

    Ok(())
}

#[tokio::main]
async fn main() {
    run().await.unwrap();
}
