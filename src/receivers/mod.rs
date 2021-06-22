mod buffer_unordered;
// mod buffer_unordered_batched;
// mod mpsc_futures;
// mod synchronize_batched;
// mod synchronized;


// mod mpsc;
// mod mpsc {
//     pub use super::mpsc_futures::*;
// }

pub use buffer_unordered::{
    BufferUnorderedAsync, BufferUnorderedConfig,
    BufferUnorderedSync,
};

use crate::receiver::Action;

pub(crate) enum Request<M> {
    Action(Action),
    Request(u64, M)
}


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
