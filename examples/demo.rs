use messagebus::{receivers, Bus, Handler, Result as MbusResult};

struct TmpReceiver;
struct TmpReceiver2;

impl Handler<f32> for TmpReceiver {
    fn handle(&self, msg: f32, bus: &Bus) -> MbusResult {
        bus.try_send(1u16).unwrap();

        println!("---> f32 {}", msg);
        Ok(())
    }
}

impl Handler<u16> for TmpReceiver {
    fn handle(&self, msg: u16, bus: &Bus) -> MbusResult {
        bus.try_send(1u32).unwrap();
        println!("---> u16 {}", msg);
        Ok(())
    }
}

impl Handler<u32> for TmpReceiver {
    fn handle(&self, msg: u32, bus: &Bus) -> MbusResult {
        bus.try_send(2i32).unwrap();
        println!("---> u32 {}", msg);
        Ok(())
    }
}

impl Handler<i32> for TmpReceiver {
    fn handle(&self, msg: i32, bus: &Bus) -> MbusResult {
        bus.try_send(3i16).unwrap();
        println!("---> i32 {}", msg);
        Ok(())
    }
}

impl Handler<i16> for TmpReceiver {
    fn handle(&self, msg: i16, _bus: &Bus) -> MbusResult {
        println!("---> i16 {}", msg);
        Ok(())
    }
}

impl Handler<i32> for TmpReceiver2 {
    fn handle(&self, msg: i32, bus: &Bus) -> MbusResult {
        bus.try_send(3i16).unwrap();
        println!("---> 2 i32 {}", msg);
        Ok(())
    }
}

impl Handler<i16> for TmpReceiver2 {
    fn handle(&self, msg: i16, _bus: &Bus) -> MbusResult {
        println!("---> 2 i16 {}", msg);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register(TmpReceiver)
        .subscribe::<f32, receivers::BufferUnorderedSync<_>>(Default::default())
        .subscribe::<u16, receivers::BufferUnorderedSync<_>>(Default::default())
        .subscribe::<u32, receivers::BufferUnorderedSync<_>>(Default::default())
        .subscribe::<i32, receivers::BufferUnorderedSync<_>>(Default::default())
        .subscribe::<i16, receivers::BufferUnorderedSync<_>>(Default::default())
        .done()
        .register(TmpReceiver2)
        .subscribe::<i32, receivers::BufferUnorderedSync<_>>(Default::default())
        .subscribe::<i16, receivers::BufferUnorderedSync<_>>(Default::default())
        .done()
        .build();

    b.send(32f32).await.unwrap();
    poller.await
}
