use messagebus::{
    derive::{Error as MbError, Message},
    error, Bus, Handler, Message, MessageBounds, Module,
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
pub struct Msg<F: MessageBounds + Clone>(pub F);

struct TmpReceiver;

impl Handler<Msg<u32>> for TmpReceiver {
    type Error = Error;
    type Response = ();

    fn handle(&self, msg: Msg<u32>, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("---> u32 {:?}", msg);
        Ok(())
    }
}

fn module() -> Module {
    Module::new()
        .register(TmpReceiver)
        .subscribe_sync::<Msg<u32>>(8, Default::default())
        .done()
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build().add_module(module()).build();

    // b.

    println!("flush");
    b.flush_all().await;

    println!("close");
    b.close().await;

    println!("closed");

    poller.await;
    println!("[done]");
}
