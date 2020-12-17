mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use r#async::{BufferUnorderedAsync, BufferUnorderedAsyncSubscriber};
pub use sync::{BufferUnorderedSync, BufferUnorderedSyncSubscriber};

#[derive(Debug)]
pub struct BufferUnorderedStats {
    pub buffer: AtomicU64,
    pub buffer_total: AtomicU64,
    pub parallel: AtomicU64,
    pub parallel_total: AtomicU64,
}

#[derive(Copy, Clone, Debug)]
pub struct BufferUnorderedConfig {
    pub buffer_size: usize,
    pub max_parallel: usize,
}

impl Default for BufferUnorderedConfig {
    fn default() -> Self {
        Self {
            buffer_size: 8,
            max_parallel: 8,
        }
    }
}
