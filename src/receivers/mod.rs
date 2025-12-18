//! Receiver implementations and configurations.
//!
//! This module provides different receiver types that determine how messages
//! are processed. Each receiver type has an associated configuration struct.
//!
//! # Receiver Types
//!
//! | Type | Thread-Safe | Batched | Description |
//! |------|-------------|---------|-------------|
//! | [`BufferUnorderedSync`] / [`BufferUnorderedAsync`] | Yes | No | Concurrent processing with thread-safe handlers |
//! | [`BufferUnorderedBatchedSync`] / [`BufferUnorderedBatchedAsync`] | Yes | Yes | Batched concurrent processing |
//! | [`SynchronizedSync`] / [`SynchronizedAsync`] | No | No | Sequential processing with mutable state |
//! | [`SynchronizedBatchedSync`] / [`SynchronizedBatchedAsync`] | No | Yes | Batched sequential processing |
//!
//! # Configuration Types
//!
//! - [`BufferUnorderedConfig`] - For concurrent handlers (Handler, AsyncHandler)
//! - [`BufferUnorderedBatchedConfig`] - For concurrent batch handlers
//! - [`SynchronizedConfig`] - For synchronized handlers with mutable state
//! - [`SynchronizedBatchedConfig`] - For synchronized batch handlers

mod buffer_unordered;
mod buffer_unordered_batched;
// mod producer;
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

// pub use producer::{AsyncProducer, AsyncProducerConfig};

use crate::receiver::Action;

#[macro_export]
macro_rules! process_batch_result {
    ($resp: expr, $mids: expr, $stx: expr) => {
        let mids = $mids;

        match $resp {
            Ok(re) => {
                let mut mids = mids.into_iter();
                let mut re = re.into_iter();

                while let Some((mid, _req)) = mids.next() {
                    if let Some(r) = re.next() {
                        $stx.send(Event::Response(mid, Ok(r))).unwrap();
                    } else {
                        $stx.send(Event::Response(mid, Err(Error::NoResponse)))
                            .unwrap();
                    }
                }
            }
            Err(er) => {
                for (mid, _req) in mids {
                    $stx.send(Event::Response(mid, Err(Error::Other(er.clone()))))
                        .unwrap();
                }

                $stx.send(Event::Error(Error::Other(er))).unwrap();
            }
        }
    };
}

#[derive(Debug)]
pub(crate) enum Request<M> {
    Action(Action),
    Request(u64, M, bool),
}
