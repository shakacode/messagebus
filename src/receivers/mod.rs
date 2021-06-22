mod buffer_unordered;
mod synchronized;
// mod buffer_unordered_batched;
// mod mpsc_futures;
// mod synchronize_batched;

// mod mpsc;
// mod mpsc {
//     pub use super::mpsc_futures::*;
// }

pub use buffer_unordered::{BufferUnorderedAsync, BufferUnorderedConfig, BufferUnorderedSync};
pub use synchronized::{SynchronizedAsync, SynchronizedConfig, SynchronizedSync};

use crate::receiver::Action;

pub(crate) enum Request<M> {
    Action(Action),
    Request(u64, M),
}

// pub use buffer_unordered_batched::{
//     BufferUnorderedBatchedAsync, BufferUnorderedBatchedAsyncSubscriber, BufferUnorderedBatchedConfig,
//     BufferUnorderedBatchedSync, BufferUnorderedBatchedSyncSubscriber,
// };

// pub use synchronize_batched::{
//     SynchronizeBatchedAsync, SynchronizeBatchedAsyncSubscriber, SynchronizeBatchedConfig,
//     SynchronizeBatchedSync, SynchronizeBatchedSyncSubscriber,
// };
