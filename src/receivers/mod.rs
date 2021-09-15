mod buffer_unordered;
mod buffer_unordered_batched;
mod synchronize_batched;
mod synchronized;

pub use buffer_unordered::{BufferUnorderedAsync, BufferUnorderedConfig, BufferUnorderedSync};
pub use buffer_unordered_batched::{
    BufferUnorderedBatchedAsync, BufferUnorderedBatchedConfig, BufferUnorderedBatchedSync,
};
pub use synchronized::{SynchronizedAsync, SynchronizedConfig, SynchronizedSync};

pub use synchronize_batched::{
    SynchronizedBatchedAsync, SynchronizedBatchedConfig, SynchronizedBatchedSync,
};

use crate::receiver::Action;

#[derive(Debug)]
pub(crate) enum Request<M> {
    Action(Action),
    Request(u64, M, bool),
}
