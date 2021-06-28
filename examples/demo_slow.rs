use messagebus::{Bus, Handler, Message, Module, error, receivers};
use thiserror::Error;

#[derive(Debug, Error)]
enum Error {
    #[error("Error({0})")]
    Error(anyhow::Error)
}

impl<M: Message> From<error::Error<M>> for Error {
    fn from(err: error::Error<M>) -> Self {
        Self::Error(err.into())
    }
}

struct TmpReceiver;

impl Handler<f32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    fn handle(&self, msg: f32, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("---> f32 {}", msg);

        std::thread::sleep(std::time::Duration::from_secs(5));

        println!("done");

        Ok(())
    }
}

impl Handler<u16> for TmpReceiver {
    type Error = Error;
    type Response = ();

    fn handle(&self, msg: u16, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("---> u16 {}", msg);
        Ok(())
    }
}

impl Handler<u32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    fn handle(&self, msg: u32, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("---> u32 {}", msg);
        Ok(())
    }
}

fn module() -> Module {
    Module::new()
    .register(TmpReceiver)
        .subscribe::<f32, receivers::BufferUnorderedSync<_, _, _>, _, _>(8, Default::default())
        .subscribe::<u16, receivers::BufferUnorderedSync<_, _, _>, _, _>(8, Default::default())
        .subscribe::<u32, receivers::BufferUnorderedSync<_, _, _>, _, _>(8, Default::default())
    .done()
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .add_module(module())
        .build();

    b.send(32f32).await.unwrap();
    b.send(11u16).await.unwrap();
    b.send(32u32).await.unwrap();

    println!("flush");
    b.flush().await;

    println!("close");
    b.close().await;

    println!("closed");

    poller.await;
    println!("[done]");
}
