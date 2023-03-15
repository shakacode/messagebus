use core::fmt;
use std::{
    any::Any,
    future::poll_fn,
    sync::{Arc, Weak},
};

use dashmap::DashMap;
use segvec::SegVec;

use crate::{
    cell::{MessageCell, MsgCell},
    error::Error,
    message::Message,
    polling_pool::PollingPool,
    receiver::{AbstractReceiver, IntoAbstractReceiver, Receiver},
};

pub use crate::handler::*;

pub struct TaskHandlerVTable {
    pub drop: fn(Arc<dyn Any + Send + Sync>, usize),
}

pub struct TaskHandler {
    data: Arc<dyn Any + Send + Sync>,
    gen: u64,
    index: usize,
    vtable: &'static TaskHandlerVTable,
}

impl fmt::Debug for TaskHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Task #{} in {:?}", self.index, self.data)
    }
}

unsafe impl Send for TaskHandler {}
unsafe impl Sync for TaskHandler {}

impl TaskHandler {
    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }

    #[inline]
    pub fn data(&self) -> &Arc<dyn Any + Send + Sync> {
        &self.data
    }

    #[inline]
    pub fn new(
        vtable: &'static TaskHandlerVTable,
        data: Arc<dyn Any + Send + Sync>,
        index: usize,
    ) -> Self {
        Self {
            data,
            gen: 0,
            index,
            vtable,
        }
    }

    pub(crate) fn finish(&mut self) {
        // TODO
    }

    pub(crate) fn is_finished(&self) -> bool {
        // TODO
        false
    }
}

impl Drop for TaskHandler {
    fn drop(&mut self) {
        // TODO optimize redundant clone
        (self.vtable.drop)(self.data.clone(), self.index);
    }
}

pub const MASK_ALL: u64 = u64::MAX;
pub const MASK_NONE: u64 = 0;

#[derive(Debug, Clone)]
pub struct SendOptions {
    pub mask: u64,
}
impl SendOptions {
    pub fn from_mask(mask: u64) -> SendOptions {
        Self { mask }
    }
}

impl Default for SendOptions {
    fn default() -> Self {
        SendOptions { mask: MASK_ALL }
    }
}

pub struct BusContext {
    parent: Option<Weak<BusContext>>,
    id: usize,
    name: String,
}

#[derive(Clone)]
pub struct Bus {
    inner: Arc<BusInner>,
    context: Arc<BusContext>,
}

impl Bus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(BusInner::new()),
            context: Arc::new(BusContext {
                parent: None,
                id: 0,
                name: String::from("root"),
            }),
        }
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    #[inline]
    pub fn send_try<M: Message>(&self, msg: M) -> Result<(), Error> {
        let mut msg = MsgCell::new(msg);

        self.inner
            .try_send(&mut msg, SendOptions::default(), self)?;

        Ok(())
    }

    #[inline]
    pub async fn send<M: Message>(&self, msg: M) -> Result<(), Error> {
        let mut msg = MsgCell::new(msg);

        self.inner
            .send(&mut msg, SendOptions::default(), self)
            .await?;

        Ok(())
    }

    #[inline]
    pub fn register<M: Message, R: Message, H: Receiver<M, R> + Send + Sync + 'static>(
        &self,
        r: H,
        mask: MaskMatch,
    ) {
        self.inner.register(r, mask)
    }

    #[inline]
    pub async fn start_producer<M: Message>(&self, msg: M) -> Result<(), Error> {
        let mut msg = MsgCell::new(msg);

        self.inner
            .producer_start(&mut msg, SendOptions::default(), self)
            .await?;

        Ok(())
    }

    #[inline]
    pub async fn request<M: Message, R: Message>(&self, msg: M) -> Result<R, Error> {
        self.inner.request(msg, self).await
    }

    #[inline]
    pub async fn send_with_mask<M: Message>(&self, mask: u64, msg: M) -> Result<(), Error> {
        let mut msg = MsgCell::new(msg);

        self.inner
            .send(&mut msg, SendOptions::from_mask(mask), self)
            .await?;

        Ok(())
    }

    #[inline]
    pub async fn wait(&self) {
        self.inner.wait(self).await
    }

    #[inline]
    pub async fn close(&self) {
        self.inner.close().await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MaskMatch {
    pos: u64,
    neg: u64,
}

impl MaskMatch {
    #[inline]
    pub fn all() -> Self {
        Self {
            pos: u64::MAX,
            neg: 0,
        }
    }

    #[inline]
    pub fn only(mask: u64) -> Self {
        Self {
            pos: mask,
            neg: !mask,
        }
    }

    #[inline]
    pub fn new(pos: u64, neg: u64) -> Self {
        Self { pos, neg }
    }

    #[inline]
    pub fn pos(pos: u64) -> Self {
        Self { pos, neg: 0 }
    }

    #[inline]
    fn test(&self, mask: u64) -> bool {
        self.pos & mask > 0 && self.neg & mask == 0
    }
}

impl Default for MaskMatch {
    fn default() -> Self {
        Self {
            pos: MASK_ALL,
            neg: MASK_NONE,
        }
    }
}

#[derive(Clone)]
struct BusReceiver {
    inner: Arc<dyn AbstractReceiver>,
    mask: MaskMatch,
}

struct BusReceivers {
    inner: SegVec<BusReceiver>,
}

impl BusReceivers {
    pub fn new() -> Self {
        Self {
            inner: SegVec::with_capacity(8),
        }
    }

    #[inline]
    pub fn add(&mut self, mask: MaskMatch, inner: Arc<dyn AbstractReceiver>) {
        self.inner.push(BusReceiver { inner, mask })
    }
}

impl From<Arc<dyn AbstractReceiver>> for BusReceivers {
    fn from(inner: Arc<dyn AbstractReceiver>) -> Self {
        let mut vec = SegVec::with_capacity(8);
        vec.push(BusReceiver {
            inner,
            mask: Default::default(),
        });

        BusReceivers { inner: vec }
    }
}

pub struct BusInner {
    receivers: DashMap<(u64, u64), BusReceivers>,
    processing: Arc<PollingPool>,
}

impl BusInner {
    pub(crate) fn new() -> Self {
        Self {
            receivers: DashMap::new(),
            processing: Arc::new(PollingPool::new()),
        }
    }

    #[inline]
    pub(crate) fn is_closed(&self) -> bool {
        self.processing.is_closed()
    }

    pub(crate) fn register<M: Message, R: Message, H: Receiver<M, R> + Send + Sync + 'static>(
        &self,
        r: H,
        mask: MaskMatch,
    ) {
        let mtt = M::TYPE_TAG();
        let rtt = R::TYPE_TAG();
        let receiver = r.into_abstract_arc();

        self.receivers
            .entry((mtt.hash, rtt.hash))
            .or_insert_with(BusReceivers::new)
            .add(mask, receiver.clone());

        self.receivers
            .entry((mtt.hash, 0))
            .or_insert_with(BusReceivers::new)
            .add(mask, receiver.clone());
    }

    pub(crate) fn try_send(
        &self,
        msg: &mut dyn MessageCell,
        options: SendOptions,
        bus: &Bus,
    ) -> Result<(), Error> {
        let tt = msg.type_tag();

        let receivers = self
            .receivers
            .get(&(tt.hash, 0))
            .ok_or_else(|| Error::NoSuchReceiver(tt, None))?;

        for receiver in receivers.inner.iter() {
            if !receiver.mask.test(options.mask) {
                continue;
            }

            match receiver.inner.try_send_dyn(msg, bus) {
                Ok(task) => {
                    let receiver = receiver.clone();
                    self.processing.push(task, receiver.inner, false);
                }

                Err(err) => {
                    println!("send failed {}", err);
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn send(
        &self,
        msg: &mut dyn MessageCell,
        options: SendOptions,
        bus: &Bus,
    ) -> Result<(), Error> {
        let tt = msg.type_tag();

        let receivers = self
            .receivers
            .get(&(tt.hash, 0))
            .ok_or_else(|| Error::NoSuchReceiver(tt, None))?;

        for receiver in receivers.inner.iter() {
            if !receiver.mask.test(options.mask) {
                continue;
            }

            match receiver.inner.send_dyn(msg, bus.clone()).await {
                Ok(task) => {
                    let receiver = receiver.clone();
                    self.processing.push(task, receiver.inner, false);
                }

                Err(err) => {
                    println!("send failed {}", err);
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn producer_start(
        &self,
        msg: &mut dyn MessageCell,
        options: SendOptions,
        bus: &Bus,
    ) -> Result<(), Error> {
        let tt = msg.type_tag();

        let receivers = self
            .receivers
            .get(&(tt.hash, 0))
            .ok_or_else(|| Error::NoSuchReceiver(tt, None))?;

        for receiver in receivers.inner.iter() {
            if !receiver.mask.test(options.mask) {
                continue;
            }

            match receiver.inner.send_dyn(msg, bus.clone()).await {
                Ok(task) => {
                    let receiver = receiver.clone();
                    self.processing.push(task, receiver.inner, true);
                }

                Err(err) => {
                    println!("send failed {}", err);
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn request<M: Message, R: Message>(
        &self,
        msg: M,
        bus: &Bus,
    ) -> Result<R, Error> {
        let mtt = M::TYPE_TAG();
        let rtt = R::TYPE_TAG();

        let receivers = self
            .receivers
            .get(&(mtt.hash, rtt.hash))
            .ok_or_else(|| Error::NoSuchReceiver(mtt.clone(), Some(rtt.clone())))?;

        if let Some(receiver) = receivers.inner.iter().next() {
            receiver.inner.request(msg, bus.clone()).await
        } else {
            Err(Error::NoSuchReceiver(mtt, Some(rtt)))
        }
    }

    pub(crate) async fn wait(&self, bus: &Bus) {
        let pool = self.processing.clone();

        poll_fn(move |cx| pool.poll(cx, bus)).await
    }

    pub(crate) async fn close(&self) {
        self.processing.close();
    }
}
