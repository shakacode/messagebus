mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use r#async::{BufferUnorderedBatchedAsync, BufferUnorderedBatchedAsyncSubscriber};
pub use sync::{BufferUnorderedBatchedSync, BufferUnorderedBatchedSyncSubscriber};

#[derive(Debug)]
pub struct BufferUnorderedBatchedStats {
    pub buffer: AtomicU64,
    pub buffer_total: AtomicU64,
    pub parallel: AtomicU64,
    pub parallel_total: AtomicU64,
    pub batch: AtomicU64,
    pub batch_size: AtomicU64,
}

#[derive(Copy, Clone, Debug)]
pub struct BufferUnorderedBatchedConfig {
    pub buffer_size: usize,
    pub max_parallel: usize,
    pub batch_size: usize,
    pub when_ready: bool,
}

impl Default for BufferUnorderedBatchedConfig {
    fn default() -> Self {
        Self {
            buffer_size: 8,
            max_parallel: 2,
            batch_size: 8,
            when_ready: false,
        }
    }
}
