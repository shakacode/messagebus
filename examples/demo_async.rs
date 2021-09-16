#![feature(associated_type_defaults, generic_associated_types, type_alias_impl_trait)]

use futures::Future;
use messagebus::{derive::Message, error, AsyncHandler, Bus, Handler, Message, TypeTagged};
use thiserror::Error;

#[derive(Debug, Error, messagebus::derive::Error)]
enum Error {
    #[error("Error({0})")]
    Error(anyhow::Error),
}

impl<M: Message> From<error::Error<M>> for Error {
    fn from(err: error::Error<M>) -> Self {
        Self::Error(err.into())
    }
}

struct TmpReceiver;
struct TmpReceiver2;

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgF32(f32);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgU16(u16);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgU32(u32);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgI32(i32);

#[derive(Debug, Clone, Message)]
#[message(clone)]
struct MsgI16(i16);

impl AsyncHandler<MsgF32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    type AsyncHandleFuture<'a> = impl Future<Output = Result<Self::Response, Self::Error>> + Send + Sync + 'a;
    type AsyncSyncFuture<'a> = impl Future<Output = Result<(), Self::Error>> + Send + Sync + 'a;

    fn handle(&self, msg: MsgF32, bus: &Bus) -> Self::AsyncHandleFuture<'_> {
        let bus = bus.clone();

        async move {
            bus.send(MsgU16(1)).await?;

            println!("TmpReceiver ---> {:?} {}", msg, msg.type_tag());

            Ok(())
        }
    }

    fn sync(&self, _bus: &Bus) -> Self::AsyncSyncFuture<'_> {
        async move {
            println!("TmpReceiver f32: sync");

            Ok(())
        }
    }
}

impl AsyncHandler<MsgU16> for TmpReceiver {
    type Error = Error;
    type Response = ();

    type AsyncHandleFuture<'a> = impl Future<Output = Result<Self::Response, Self::Error>> + Send + Sync + 'a;
    type AsyncSyncFuture<'a> = impl Future<Output = Result<(), Self::Error>> + Send + Sync + 'a;

    fn handle(&self, msg: MsgU16, bus: &Bus) -> Self::AsyncHandleFuture<'_> {
        let bus = bus.clone();

        async move {
            bus.send(MsgU32(2)).await?;
            println!("TmpReceiver ---> {:?}", msg);

            Ok(())
        }
    }

    fn sync(&self, _bus: &Bus) -> Self::AsyncSyncFuture<'_> {
        async move {
            println!("TmpReceiver u16: sync");

            Ok(())
        }
    }
}

impl AsyncHandler<MsgU32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    type AsyncHandleFuture<'a> = impl Future<Output = Result<Self::Response, Self::Error>> + Send + Sync + 'a;
    type AsyncSyncFuture<'a> = impl Future<Output = Result<(), Self::Error>> + Send + Sync + 'a;

    fn handle(&self, msg: MsgU32, bus: &Bus) -> Self::AsyncHandleFuture<'_> {
        let bus = bus.clone();

        async move {
            bus.send(MsgI32(3)).await?;
            println!("TmpReceiver ---> {:?}", msg);

            Ok(())
        }
    }

    fn sync(&self, _bus: &Bus) -> Self::AsyncSyncFuture<'_> {
        async move {
            println!("TmpReceiver u32: sync");

            Ok(())
        }
    }
}

impl AsyncHandler<MsgI32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    type AsyncHandleFuture<'a> = impl Future<Output = Result<Self::Response, Self::Error>> + Send + Sync + 'a;
    type AsyncSyncFuture<'a> = impl Future<Output = Result<(), Self::Error>> + Send + Sync + 'a;

    fn handle(&self, msg: MsgI32, bus: &Bus) -> Self::AsyncHandleFuture<'_> {
        let bus = bus.clone();

        async move {
            bus.send(MsgI16(4)).await?;
            println!("TmpReceiver ---> {:?}", msg);

            Ok(())
        }
    }

    fn sync(&self, _bus: &Bus) -> Self::AsyncSyncFuture<'_> {
        async move {
            println!("TmpReceiver i32: sync");

            Ok(())
        }
    }
}

impl AsyncHandler<MsgI16> for TmpReceiver {
    type Error = Error;
    type Response = ();

    type AsyncHandleFuture<'a> = impl Future<Output = Result<Self::Response, Self::Error>> + Send + Sync + 'a;
    type AsyncSyncFuture<'a> = impl Future<Output = Result<(), Self::Error>> + Send + Sync + 'a;

    fn handle(&self, msg: MsgI16, _bus: &Bus) -> Self::AsyncHandleFuture<'_> {
        async move {
            println!("TmpReceiver ---> {:?}", msg);

            Ok(())
        }
    }

    fn sync(&self, _bus: &Bus) -> Self::AsyncSyncFuture<'_> {
        async move {
            println!("TmpReceiver i16: sync");

            Ok(())
        }
    }
}


impl AsyncHandler<MsgI32> for TmpReceiver2 {
    type Error = Error;
    type Response = ();

    type AsyncHandleFuture<'a> = impl Future<Output = Result<Self::Response, Self::Error>> + Send + Sync + 'a;
    type AsyncSyncFuture<'a> = impl Future<Output = Result<(), Self::Error>> + Send + Sync + 'a;

    fn handle(&self, msg: MsgI32, bus: &Bus) -> Self::AsyncHandleFuture<'_> {
        let bus = bus.clone();

        async move {
            println!("TmpReceiver2: ---> {:?}", msg);

            bus.send(MsgI16(5)).await?;

            Ok(())
        }
    }
    fn sync(&self, _bus: &Bus) -> Self::AsyncSyncFuture<'_> {
        async move {
            println!("TmpReceiver2: i32: sync");

            Ok(())
        }
    }
}

impl Handler<MsgI16> for TmpReceiver2 {
    type Error = Error;
    type Response = ();

    fn handle(&self, msg: MsgI16, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("TmpReceiver2: ---> {:?}", msg);

        Ok(())
    }

    fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver2: i16: sync");

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let (b, poller) = Bus::build()
        .register(TmpReceiver)
        .subscribe_async::<MsgF32>(8, Default::default())
        .subscribe_async::<MsgU16>(8, Default::default())
        .subscribe_async::<MsgU32>(8, Default::default())
        .subscribe_async::<MsgI32>(8, Default::default())
        .subscribe_async::<MsgI16>(8, Default::default())
        .done()
        .register(TmpReceiver2)
        .subscribe_async::<MsgI32>(8, Default::default())
        .subscribe_sync::<MsgI16>(8, Default::default())
        .done()
        .build();

    b.send(MsgF32(0.)).await.unwrap();

    println!("flush");
    b.flush().await;

    println!("close");
    b.close().await;

    println!("closed");

    poller.await;
    println!("[done]");
}
