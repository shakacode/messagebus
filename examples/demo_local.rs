use std::rc::Rc;
use std::cell::Cell;
use async_trait::async_trait;
use messagebus::{receivers, Bus, LocalHandler, LocalAsyncHandler, Result as MbusResult};

struct TmpReceiver {
    _inner: Rc<Cell<i32>>
}


#[async_trait(?Send)]
impl LocalAsyncHandler<f32> for TmpReceiver {
    async fn handle(&mut self, msg: f32, bus: &Bus) -> MbusResult {
        println!("---> f32 {}", msg);

        bus.send(11u16).await.unwrap();

        Ok(())
    }
}

impl LocalHandler<u16> for TmpReceiver {
    fn handle(&mut self, msg: u16, _bus: &Bus) -> MbusResult {
        println!("---> u16 {}", msg);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register_local(||TmpReceiver {_inner: Rc::new(Cell::new(12))})
        .subscribe::<f32, receivers::LocalAsync<_>>(Default::default())
        .subscribe::<u16, receivers::LocalSync<_>>(Default::default())
        .done()
        .build();

    b.send(32f32).await.unwrap();

    poller.await
}
