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
3. Messages delivers in a broadcast fashion to many receivers (Cloned)
4. There are different kind of receivers implemented:
  - BufferUnordered Receiver (in sync and async version depending by handler)
  - Synchronized (also sync and async) if receiving part needs syncronization
  - SynchronizeBuffered (also sync and async)
here are the implmented handlers definitions:
```rust

// Handler is Sync and we can spawn many of concurrent tasks
pub trait Handler<M: Message>: Send + Sync {
    fn handle(&self, msg: M, bus: &Bus) -> anyhow::Result<()>;
    fn sync(&self, _bus: &Bus) -> anyhow::Result<()> {Ok(())}
}

#[async_trait]
pub trait AsyncHandler<M: Message>: Send + Sync {
    async fn handle(&self, msg: M, bus: &Bus) -> anyhow::Result<()>;
    async fn sync(&self, _bus: &Bus) -> anyhow::Result<()> {Ok(())}
}

// Handler is not Sync and we cannot spawn many of concurrent tasks same time (uses synchronization primitives such as Mutex or RwLock)
pub trait SynchronizedHandler<M: Message>: Send {
    fn handle(&mut self, msg: M, bus: &Bus) -> anyhow::Result<()>;
    fn sync(&mut self, _bus: &Bus) -> anyhow::Result<()> {Ok(())}
}

#[async_trait]
pub trait AsyncSynchronizedHandler<M: Message>: Send {
    async fn handle(&mut self, msg: M, bus: &Bus) -> anyhow::Result<()>;
    async fn sync(&mut self, _bus: &Bus) -> anyhow::Result<()> {Ok(())}
}

// Handler is not Sync and handler will process items in batched mode 
pub trait BatchSynchronizedHandler<M: Message>: Send {
    fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> anyhow::Result<()>;
    fn sync(&mut self, _bus: &Bus) -> anyhow::Result<()> {Ok(())}
}

#[async_trait]
pub trait AsyncBatchSynchronizedHandler<M: Message>: Send {
    async fn handle(&mut self, msg: Vec<M>, bus: &Bus) -> anyhow::Result<()>;
    async fn sync(&mut self, _bus: &Bus) -> anyhow::Result<()> {Ok(())}
}

```
4. Handler Kinds:
    1. No Synchronization needed (Handler is `Send` + `Sync`)
        * Not batched operations **(implemented)**
            - sync  (spawn_blocking)
            - async (spawn)
        * Batched
            - sync  (spawn_blocking)
            - async (spawn)
    2. Synchronization needed (Handler is `Sync` + `!Send`)
        * Not batched operations **(implemented)**
            - sync  (spawn_blocking)
            - async (spawn)
        * Batched **(implemented)**
            - sync  (spawn_blocking)
            - async (spawn)
    3. Synchronization needed and thread dedicated (Handler is `!Sync` + `!Send`)
        * Not batched operations
            - sync  (spawn_blocking)
            - async (spawn)
        * Batched
            - sync  (spawn_blocking)
            - async (spawn)

5. Example:
```rust
use messagebus::{Bus, AsyncHandler, Result as MbusResult, receivers};
use async_trait::async_trait;

struct TmpReceiver;

#[async_trait]
impl AsyncHandler<i32> for TmpReceiver {
    async fn handle(&self, msg: i32, bus: &Bus) -> MbusResult {
        println!("---> i32 {}", msg);

        bus.send(2i64).await?;

        Ok(())
    }
}

#[async_trait]
impl AsyncHandler<i64> for TmpReceiver {
    async fn handle(&self, msg: i64, _bus: &Bus) -> MbusResult {
        println!("---> i64 {}", msg);

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let (b, poller) = Bus::build()
        .register(TmpReceiver)
            .subscribe::<i32, receivers::BufferUnorderedAsync<_>>(Default::default())
            .subscribe::<i64, receivers::BufferUnorderedAsync<_>>(Default::default())
            .done()
        .build();

    b.send(1i32).await.unwrap();
    poller.await
}
```