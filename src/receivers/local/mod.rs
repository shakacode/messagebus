mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use sync::{LocalSync, LocalSyncSubscriber};

pub use r#async::{LocalAsync, LocalAsyncSubscriber};

#[derive(Debug)]
pub struct LocalStats {
    pub buffer: AtomicU64,
    pub buffer_total: AtomicU64,
}

#[derive(Copy, Clone, Debug)]
pub struct LocalConfig {
    pub buffer_size: usize,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self { buffer_size: 1 }
    }
}
