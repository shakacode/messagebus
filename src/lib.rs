mod builder;
mod envelop;
mod handler;
pub mod msgs;
mod receiver;
pub mod receivers;
mod trait_object;
mod untyped;
mod utils;

use builder::BusBuilder;
pub use envelop::Message;
pub use handler::*;
pub use receiver::SendError;
use receiver::{Receiver, ReceiverStats};
use utils::binary_search_range_by_key;

use core::any::TypeId;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

pub use untyped::Untyped;
pub type Result = anyhow::Result<()>;

pub struct BusInner {
    receivers: Vec<(TypeId, Receiver)>,
    closed: AtomicBool,
}

impl BusInner {
    pub(crate) fn new(mut receivers: Vec<(TypeId, Receiver)>) -> Self {
        receivers.sort_unstable_by_key(|(k, _)| *k);

        Self {
            receivers,
            closed: AtomicBool::new(false),
        }
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);

        for (_, r) in &self.receivers {
            r.close();
        }
    }

    pub async fn sync(&self) {
        for (_, r) in &self.receivers {
            r.sync().await;
        }
    }

    pub fn stats(&self) -> impl Iterator<Item = ReceiverStats> + '_ {
        self.receivers.iter().map(|(_, r)| r.stats())
    }

    pub fn try_send<M: Message>(&self, msg: M) -> core::result::Result<(), SendError<M>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg));
        }

        let tid = TypeId::of::<M>();
        let range = binary_search_range_by_key(&self.receivers, &tid, |(k, _)| *k);

        for i in (range.start + 1)..range.end {
            self.receivers[i].1.try_broadcast(msg.clone())?;
        }

        if let Some((_, r)) = self.receivers.get(range.start) {
            r.try_broadcast(msg)?;
        } else {
            println!("Unhandled message {:?}", core::any::type_name::<M>());
        }

        Ok(())
    }

    #[inline]
    pub fn send_blocking<M: Message>(&self, msg: M) -> core::result::Result<(), SendError<M>> {
        futures::executor::block_on(self.send(msg))
    }

    pub async fn send<M: Message>(&self, msg: M) -> core::result::Result<(), SendError<M>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg));
        }

        let tid = TypeId::of::<M>();
        let range = binary_search_range_by_key(&self.receivers, &tid, |(k, _)| *k);

        for i in (range.start + 1)..range.end {
            self.receivers[i].1.broadcast(msg.clone()).await?;
        }

        if let Some((_, r)) = self.receivers.get(range.start) {
            r.broadcast(msg).await?;
        } else {
            println!("Unhandled message {:?}", core::any::type_name::<M>());
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct Bus {
    inner: Arc<BusInner>,
}

impl core::ops::Deref for Bus {
    type Target = BusInner;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl Bus {
    #[inline]
    pub fn build() -> BusBuilder {
        BusBuilder::new()
    }
}
