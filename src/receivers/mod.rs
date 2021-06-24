mod buffer_unordered;
mod buffer_unordered_batched;
mod synchronize_batched;
mod synchronized;

use futures::Future;
use std::pin::Pin;

pub use buffer_unordered::{BufferUnorderedAsync, BufferUnorderedConfig, BufferUnorderedSync};
pub use buffer_unordered_batched::{
    BufferUnorderedBatchedAsync, BufferUnorderedBatchedConfig, BufferUnorderedBatchedSync,
};
pub use synchronized::{SynchronizedAsync, SynchronizedConfig, SynchronizedSync};

pub use synchronize_batched::{
    SynchronizedBatchedAsync, SynchronizedBatchedConfig, SynchronizedBatchedSync,
};

use crate::receiver::Action;

#[inline(always)]
pub(crate) unsafe fn fix_type<'a, F, T>(
    x: &'a mut F,
) -> Pin<&'a mut (impl Future<Output = T> + Send)>
where
    F: Future<Output = T> + Send,
{
    Pin::new_unchecked(x)
}

pub(crate) enum Request<M> {
    Action(Action),
    Request(u64, M),
}
