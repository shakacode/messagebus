mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use sync::{SynchronizeBatchedSync, SynchronizeBatchedSyncSubscriber};

pub use r#async::{SynchronizeBatchedAsync, SynchronizeBatchedAsyncSubscriber};

#[derive(Debug)]
pub struct SynchronizeBatchedStats {
    pub buffer: AtomicU64,
    pub buffer_total: AtomicU64,
    pub batch: AtomicU64,
    pub batch_size: AtomicU64,
}

#[derive(Copy, Clone, Debug)]
pub struct SynchronizeBatchedConfig {
    pub buffer_size: usize,
    pub batch_size: usize,
}

impl Default for SynchronizeBatchedConfig {
    fn default() -> Self {
        Self {
            buffer_size: 4,
            batch_size: 16,
        }
    }
}
