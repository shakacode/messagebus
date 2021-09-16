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
                        $stx.send(Event::Response(mid, Ok(r)))
                            .unwrap();
                    } else {
                        $stx.send(Event::Response(mid, Err(Error::NoResponse)))
                            .unwrap();
                    }
                }
            }
            Err(er) => {
                for (mid, _req) in mids {
                    $stx.send(Event::Response(
                        mid,
                        Err(Error::Other(er.clone())),
                    )).unwrap();
                }

                $stx.send(Event::Error(er)).unwrap();
            }
        }
    };
}

#[derive(Debug)]
pub(crate) enum Request<M> {
    Action(Action),
    Request(u64, M, bool),
}
