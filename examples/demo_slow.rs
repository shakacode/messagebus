use messagebus::{
    derive::{Error as MbError, Message},
    error, Bus, Handler, Message, Module,
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

#[derive(Debug, Clone, Message)]
struct MsgU32(pub u32);

#[derive(Debug, Clone, Message)]
struct MsgU16(pub u16);

struct TmpReceiver;

impl Handler<MsgF32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    fn handle(&self, msg: MsgF32, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("---> f32 {:?}", msg);

        std::thread::sleep(std::time::Duration::from_secs(5));

        println!("done");

        Ok(())
    }
}

impl Handler<MsgU16> for TmpReceiver {
    type Error = Error;
    type Response = ();

    fn handle(&self, msg: MsgU16, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("---> u16 {:?}", msg);
        Ok(())
    }
}

impl Handler<MsgU32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    fn handle(&self, msg: MsgU32, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("---> u32 {:?}", msg);
        Ok(())
    }
}

fn module() -> Module {
    Module::new()
        .register(TmpReceiver)
        .subscribe_sync::<MsgF32>(8, Default::default())
        .subscribe_sync::<MsgU16>(8, Default::default())
        .subscribe_sync::<MsgU32>(8, Default::default())
        .done()
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build().add_module(module()).build();

    b.send(MsgF32(32f32)).await.unwrap();
    b.send(MsgU16(11u16)).await.unwrap();
    b.send(MsgU32(32u32)).await.unwrap();

    println!("flush");
    b.flush_all().await;

    println!("close");
    b.close().await;

    println!("closed");

    poller.await;
    println!("[done]");
}
