mod buffer_unordered;
mod synchronized;
// mod buffer_unordered_batched;
// mod mpsc_futures;
// mod synchronize_batched;

// mod mpsc;
// mod mpsc {
//     pub use super::mpsc_futures::*;
// }

use std::pin::Pin;

pub use buffer_unordered::{BufferUnorderedAsync, BufferUnorderedConfig, BufferUnorderedSync};
use futures::Future;
pub use synchronized::{SynchronizedAsync, SynchronizedConfig, SynchronizedSync};

use crate::receiver::Action;

#[inline(always)]
pub(crate) unsafe fn fix_type1<'a, F, R, E>(
    x: &'a mut F,
) -> Pin<&'a mut (impl Future<Output = (u64, Result<R, E>)> + Send)>
where
    F: Future<Output = (u64, Result<R, E>)> + Send,
{
    Pin::new_unchecked(x)
}

#[inline(always)]
pub(crate) unsafe fn fix_type2<'a, F, E>(
    x: &'a mut F,
) -> Pin<&'a mut (impl Future<Output = Result<(), E>> + Send)>
where
    F: Future<Output = Result<(), E>> + Send,
{
    Pin::new_unchecked(x)
}

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
