<div align="center">
  <h1>Message Bus</h1>
  <p>
    <strong>Async Message Bus for Rust</strong>
  </p>
  <p>
</div>

Inspired by Actix

### Basics
1. Can deliver messages between actors using receivers (usually a queue implementations)
2. Messages distincts and delivers by TypeId
3. Messages delivers ether in a broadcast fashion to many receivers (Cloned) or addressed by recevier id, balanced (depends on queue load) or random
4. There are different kind of receivers implemented:
  - BufferUnordered Receiver (sync and async)
  - Synchronized (sync and async)
  - BatchedBufferUnordered Receiver (sync and async)
  - BatchedSynchronized (sync and async)
5. Request/response api. There is an example is [demo_req_resp.rs](./examples/demo_req_resp.rs)

Here are the list of implmented handler kinds:
```rust
pub trait Handler<M: Message>: Send + Sync {
    type Error: StdSyncSendError;
    type Response: Message;

    fn handle(&self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;
    fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncHandler<M: Message>: Send + Sync {
    type Error: StdSyncSendError;
    type Response: Message;

    async fn handle(&self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait SynchronizedHandler<M: Message>: Send {
    type Error: StdSyncSendError;
    type Response: Message;

    fn handle(&mut self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;
    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncSynchronizedHandler<M: Message>: Send {
    type Error: StdSyncSendError;
    type Response: Message;

    async fn handle(&mut self, msg: M, bus: &Bus) -> Result<Self::Response, Self::Error>;
    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait BatchHandler<M: Message>: Send + Sync {
    type Error: StdSyncSendError + Clone;
    type Response: Message;
    type InBatch: FromIterator<M> + Send;
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    fn handle(&self, msg: Self::InBatch, bus: &Bus) -> Result<Self::OutBatch, Self::Error>;
    fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncBatchHandler<M: Message>: Send + Sync {
    type Error: StdSyncSendError + Clone;
    type Response: Message;
    type InBatch: FromIterator<M> + Send;
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    async fn handle(&self, msg: Self::InBatch, bus: &Bus) -> Result<Self::OutBatch, Self::Error>;
    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait BatchSynchronizedHandler<M: Message>: Send {
    type Error: StdSyncSendError + Clone;
    type Response: Message;
    type InBatch: FromIterator<M> + Send;
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    fn handle(&mut self, msg: Self::InBatch, bus: &Bus) -> Result<Self::OutBatch, Self::Error>;
    fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait AsyncBatchSynchronizedHandler<M: Message>: Send {
    type Error: StdSyncSendError + Clone;
    type Response: Message;
    type InBatch: FromIterator<M> + Send;
    type OutBatch: IntoIterator<Item = Self::Response> + Send;

    async fn handle(&mut self, msg: Self::InBatch, bus: &Bus) -> Result<Self::OutBatch, Self::Error>;
    async fn sync(&mut self, _bus: &Bus) -> Result<(), Self::Error> {
        Ok(())
    }
}

```
6. Implemented handler kinds:
    1. No Synchronization needed (Handler implements `Send` and `Sync`)
        * Not batched operations
            - sync  (spawn_blocking)
            - async (spawn)
        * Batched
            - sync  (spawn_blocking)
            - async (spawn)
    2. Synchronization needed (Handler implements only `Send` but not implements `Sync`)
        * Not batched operations
            - sync  (spawn_blocking)
            - async (spawn)
        * Batched
            - sync  (spawn_blocking)
            - async (spawn)
    
7. Not yet implemented handler kinds:
    1. Synchronization needed and thread dedicated (Handler is `!Sync` and `!Send`)
        * Not batched operations
            - sync  (spawn_blocking)
            - async (spawn)
        * Batched
            - sync  (spawn_blocking)
            - async (spawn)

8. Example:
```rust
use messagebus::{error::Error, receivers, AsyncHandler, Bus};
use async_trait::async_trait;

struct TmpReceiver;

#[async_trait]
impl AsyncHandler<i32> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: i32, bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("---> i32 {}", msg);

        bus.send(2i64).await?;

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<i64> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: i64, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("---> i64 {}", msg);

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register(TmpReceiver)
            .subscribe::<i32, receivers::BufferUnorderedAsync<_>, _, _>(8, Default::default())
            .subscribe::<i64, receivers::BufferUnorderedAsync<_>, _, _>(8, Default::default())
            .done()
        .build();

    b.send(1i32).await.unwrap();

    println!("flush");
    b.flush().await;

    println!("close");
    b.close().await;

    println!("closed");

    poller.await;
    println!("[done]");
}
```