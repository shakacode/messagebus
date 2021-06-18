mod builder;
mod envelop;
mod handler;
pub mod msgs;
mod receiver;
pub mod receivers;
mod trait_object;

#[macro_use]
extern crate log;

use builder::BusBuilder;
pub use envelop::Message;
pub use handler::*;
pub use receiver::SendError;
use receiver::{Receiver, ReceiverStats};
use smallvec::SmallVec;

use core::any::{Any, TypeId};
use std::{collections::HashMap, sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}}};

use crate::receivers::Permit;

pub type Untyped = Arc<dyn Any + Send + Sync>;

// pub trait ErrorTrait: std::error::Error + Send + Sync + 'static {}
pub trait Error: Into<anyhow::Error> + Send + Sync + 'static {}
impl <T: Into<anyhow::Error> + Send + Sync + 'static> Error for T {}

static ID_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct BusInner {
    receivers: HashMap<TypeId, SmallVec<[Receiver; 4]>>,
    closed: AtomicBool,
}

impl BusInner {
    pub(crate) fn new(input: Vec<(TypeId, Receiver)>) -> Self {
        let mut receivers = HashMap::new();

        for (key, value) in input {
            receivers.entry(key)
                .or_insert_with(SmallVec::new)
                .push(value);
        }

        Self {
            receivers,
            closed: AtomicBool::new(false),
        }
    }

    pub async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);

        for (_, rs) in &self.receivers {
            for r in rs {
                r.close().await;
            }
        }
    }

    pub async fn flush(&self) {
        let fuse_count = 32i32;
        let mut breaked = false;
        let mut iters = 0usize;
        for _ in 0..fuse_count {
            iters += 1;
            let mut flushed = false;
            for (_, rs) in &self.receivers {
                
                for r in rs {
                    if r.need_flush() {
                        flushed = true; 
                        r.flush().await;
                    }
                }
            }

            if !flushed {
                breaked = true;
                break;
            }
        }

        if !breaked {
            warn!("!!! WARNING: unable to reach equilibrium in {} iterations !!!", fuse_count);
        } else {
            info!("flushed in {} iterations !!!", iters);
        }
    } 

    pub async fn flash_and_sync(&self) {
        self.flush().await;

        for (_, rs) in &self.receivers {
            for r in rs {
                r.sync().await;
            }
        }
    }

    // pub fn stats(&self) -> impl Iterator<Item = ReceiverStats> + '_ {
    //     self.receivers.iter()
    //         .map(|(_, i)|i.iter())
    //         .flatten()
    //         .map(|r| r.stats())
    // }

    pub fn try_send<M: Message>(&self, msg: M) -> core::result::Result<(), SendError<M>> {
        if self.closed.load(Ordering::SeqCst) {
            warn!("Bus closed. Skipping send!");
            return Ok(());
        }

        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tid = TypeId::of::<M>();

        if let Some(rs) = self.receivers.get(&tid) {
            let mut permits = SmallVec::<[Permit; 32]>::new();

            for r in rs {
                if let Some(prmt) = r.try_reserve() {
                    permits.push(prmt);
                } else {
                    return Err(SendError::Full(msg));
                };
            }

            let mut iter = permits.into_iter().zip(rs.iter());
            let mut counter = 1;
            let total = rs.len();

            while counter < total {
                let (p, r) = iter.next().unwrap();
                let _ = r.send(mid, p, msg.clone());

                counter += 1;
            }

            if let Some((p, r)) = iter.next() {
                let _ = r.send(mid, p, msg);
                return Ok(());
            }
        }

        warn!("Unhandled message {:?}: no receivers", core::any::type_name::<M>());

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

        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tid = TypeId::of::<M>();

        if let Some(rs) = self.receivers.get(&tid) {
            if let Some((last, head)) = rs.split_last() {
                for r in head {
                    let _ = r.send(mid, r.reserve().await, msg.clone());
                }

                let _ = last.send(mid, last.reserve().await, msg.clone());

                return Ok(());
            }
        }

        warn!("Unhandled message {:?}: no receivers", core::any::type_name::<M>());

        Ok(())
    }

    pub async fn force_send<M: Message>(&self, msg: M) -> core::result::Result<(), SendError<M>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(msg));
        }

        let mid = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tid = TypeId::of::<M>();

        if let Some(rs) = self.receivers.get(&tid) {
            if let Some((last, head)) = rs.split_last() {
                for r in head {
                    let _ = r.force_send(mid, msg.clone());
                }

                let _ = last.force_send(mid, msg.clone());

                return Ok(());
            }
        }

        warn!("Unhandled message {:?}: no receivers", core::any::type_name::<M>());

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
