mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use sync::{LocalBatchedSync, LocalBatchedSyncSubscriber};

pub use r#async::{LocalBatchedAsync, LocalBatchedAsyncSubscriber};

#[derive(Debug)]
pub struct LocalBatchedStats {
    pub buffer: AtomicU64,
    pub buffer_total: AtomicU64,
    pub batch: AtomicU64,
    pub batch_size: AtomicU64,
}

#[derive(Copy, Clone, Debug)]
pub struct LocalBatchedConfig {
    pub buffer_size: usize,
    pub batch_size: usize,
    pub when_ready: bool,
}

impl Default for LocalBatchedConfig {
    fn default() -> Self {
        Self {
            buffer_size: 4,
            batch_size: 16,
            when_ready: false,
        }
    }
}
