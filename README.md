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
6. Task grouping - assign group IDs to messages and wait for group completion

### Task Grouping

Task grouping allows you to track related messages and wait for all tasks in a group to complete. This is useful for job processing, batch operations, or any scenario where you need to know when a set of related tasks has finished.

#### Basic Usage

```rust
use messagebus::{Bus, GroupId, derive::Message};

// Define a message with a group_id
#[derive(Debug, Clone, Message)]
#[group_id(self.job_id)]
struct ProcessJob {
    job_id: i64,
    task_name: String,
}

async fn process_job(bus: &Bus) {
    let job_id: GroupId = 1001;

    // Send multiple messages with the same group_id
    bus.send(ProcessJob { job_id, task_name: "Task A".into() }).await.unwrap();
    bus.send(ProcessJob { job_id, task_name: "Task B".into() }).await.unwrap();

    // Wait for all tasks in the group to complete
    bus.flush_group(job_id).await;

    // Check if group is idle
    assert!(bus.is_group_idle(job_id));
}
```

#### Group ID Propagation

Group IDs propagate to child messages - when a handler sends new messages, they inherit the parent task's group ID unless the child message has its own `#[group_id]` attribute.

#### Flush Methods

There are three flush methods for groups, each with different use cases:

| Method | Use Case |
|--------|----------|
| `flush_group(id)` | Flushes partial batches and waits for completion. Simple and sufficient for most cases. |
| `flush_and_sync_group(id, force)` | Flushes, waits, then syncs receivers. Use for cascading messages or when you need sync. |
| `flush_current_group()` | For use within handlers to flush child messages without blocking. |

Use `flush_and_sync_group()` when handlers send cascading messages (handler A sends to handler B) that need the two-pass flush strategy to avoid deadlocks.

```rust
// Simple case - works for all handler types including batched:
bus.flush_group(job_id).await;

// For cascading messages or when you need sync:
bus.flush_and_sync_group(job_id, false).await;

// The `force` parameter skips waiting and proceeds directly to sync:
bus.flush_and_sync_group(job_id, true).await;  // Use when you know group is already idle
```

#### Triggering Child Message Processing from Within Handlers

When a handler sends child messages to batched receivers, those messages may sit in a buffer until the batch is full. Use `flush_current_group()` to trigger processing of buffered child messages:

```rust
#[async_trait]
impl AsyncHandler<ParentMessage> for MyHandler {
    async fn handle(&self, msg: ParentMessage, bus: &Bus) -> Result<(), Error> {
        // Send child messages (they inherit the parent's group_id)
        for item in msg.items {
            bus.send(ChildMessage { data: item }).await?;
        }

        // Trigger flush of child messages (non-blocking)
        bus.flush_current_group().await;

        Ok(())
    }
}
```

**Important:** `flush_current_group()` triggers non-blocking flushes via `flush_nowait()` - it does **not** wait for child messages to complete. This is intentional to prevent deadlocks (the calling handler is itself counted in `processing_count`). The actual waiting for all group messages to complete happens when the caller uses `flush_group()` or `flush_and_sync_group()` after sending the initial messages.

#### Batch Handler Behavior

Batched handlers have special group handling:
- Messages are batched **per group_id** - messages from different groups are never mixed in the same batch
- Each message's group counter is properly decremented after batch processing
- Batch handlers don't propagate group context to child messages (since batches may contain messages from different groups)

#### Group Cleanup

Groups remain in memory until explicitly removed. For long-running applications, clean up completed groups:

```rust
use messagebus::GroupRemovalResult;

// Safe removal - only removes if idle
match bus.remove_group(job_id) {
    GroupRemovalResult::Removed => println!("Group cleaned up"),
    GroupRemovalResult::NotIdle => println!("Group still has in-flight tasks"),
    GroupRemovalResult::NotFound => println!("Group doesn't exist"),
}

// Force removal (use with caution - can cause tracking issues)
bus.force_remove_group(job_id);

// Monitor tracked groups
let count = bus.tracked_group_count();
```

#### Utility Methods

```rust
// Check if a group has no in-flight tasks
bus.is_group_idle(group_id);

// Get current in-flight task count for a group
bus.group_processing_count(group_id);

// Get the current task's group ID from within a handler
Bus::current_group_id();
```

There is an example at [demo_groups.rs](./examples/demo_groups.rs)

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