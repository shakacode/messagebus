mod buffer_unordered;
// mod buffer_unordered_batched;
// mod mpsc_futures;
// mod synchronize_batched;
// mod synchronized;


// mod mpsc;
// mod mpsc {
//     pub use super::mpsc_futures::*;
// }

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Stats {
    pub has_queue: bool,
    pub queue_capacity: u64,
    pub queue_size: u64,

    pub has_parallel: bool,
    pub parallel_capacity: u64,
    pub parallel_size: u64,

    pub has_batch: bool,
    pub batch_capacity: u64,
    pub batch_size: u64,
}

#[non_exhaustive]
#[derive(Debug)]
pub enum Action<M> {
    Request(u64, M),
    Flush,
    Sync,
    Close,
    Stats,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum Event<M, E> {
    Response(u64, Result<M, E>),
    SyncResponse(E),
    Stats(Stats),
    Synchronized,
    Flushed,
    Exited,
}

pub struct Permit {
    pub(crate) fuse: bool,
    pub(crate) inner: Arc<dyn PermitDrop>
}

pub trait PermitDrop {
    fn permit_drop(&self);
}

impl Drop for Permit {
    fn drop(&mut self) {
        if !self.fuse {
            self.inner.permit_drop();
        }
    }
}

pub use buffer_unordered::{
    BufferUnorderedAsync, BufferUnorderedAsyncSubscriber, BufferUnorderedConfig,
    BufferUnorderedSync, BufferUnorderedSyncSubscriber,
};

// pub use buffer_unordered_batched::{
//     BufferUnorderedBatchedAsync, BufferUnorderedBatchedAsyncSubscriber, BufferUnorderedBatchedConfig,
//     BufferUnorderedBatchedSync, BufferUnorderedBatchedSyncSubscriber,
// };

// pub use synchronized::{
//     SynchronizedAsync, SynchronizedAsyncSubscriber, SynchronizedConfig, SynchronizedSync,
//     SynchronizedSyncSubscriber,
// };

// pub use synchronize_batched::{
//     SynchronizeBatchedAsync, SynchronizeBatchedAsyncSubscriber, SynchronizeBatchedConfig,
//     SynchronizeBatchedSync, SynchronizeBatchedSyncSubscriber,
// };
