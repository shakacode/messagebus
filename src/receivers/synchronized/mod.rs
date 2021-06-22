mod r#async;
mod sync;

use std::sync::atomic::AtomicU64;

pub use r#async::SynchronizedAsync;
pub use sync::SynchronizedSync;

#[derive(Debug)]
pub struct SynchronizedStats {
    pub buffer: AtomicU64,
    pub buffer_total: AtomicU64,
}

#[derive(Copy, Clone, Debug)]
pub struct SynchronizedConfig {
    pub buffer_size: usize,
}

impl Default for SynchronizedConfig {
    fn default() -> Self {
        Self { buffer_size: 1 }
    }
}
