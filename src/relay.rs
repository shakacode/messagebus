use crate::{
    error::Error,
    receiver::{
        Action, AnyReceiver, AnyWrapperRef, BusPollerCallback, PermitDrop, ReceiverTrait,
        SendUntypedReceiver, TypeTagAccept,
    },
    stats::Stats,
    Bus, Event, Message, Permit, ReciveUntypedReceiver, TypeTag, TypeTagAcceptItem,
};
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use dashmap::DashMap;
use futures::{pin_mut, StreamExt};
use std::sync::Arc;
use tokio::sync::{oneshot, Notify};

pub trait Relay: TypeTagAccept + SendUntypedReceiver + ReciveUntypedReceiver + 'static {}
impl<T: TypeTagAccept + SendUntypedReceiver + ReciveUntypedReceiver + 'static> Relay for T {}

struct SlabCfg;
impl sharded_slab::Config for SlabCfg {
    const RESERVED_BITS: usize = 1;
}

type Slab<T> = sharded_slab::Slab<T, SlabCfg>;

pub(crate) struct RelayContext {
    receivers: DashMap<TypeTag, Arc<RelayReceiverContext>>,
    need_flush: AtomicBool,
    ready_flag: AtomicBool,
    idling_flag: AtomicBool,
    init_sent: AtomicBool,
    flushed: Notify,
    synchronized: Notify,
    closed: Notify,
    ready: Notify,
    idle: Notify,
}

pub struct RelayReceiverContext {
    limit: u64,
    processing: AtomicU64,
    response: Arc<Notify>,
}

impl RelayReceiverContext {
    fn new(limit: u64) -> Self {
        Self {
            limit,
            processing: Default::default(),
            response: Arc::new(Notify::new()),
        }
    }
}

impl PermitDrop for RelayReceiverContext {
    fn permit_drop(&self) {
        self.processing.fetch_sub(1, Ordering::SeqCst);
    }
}

pub(crate) struct RelayWrapper<S>
where
    S: 'static,
{
    id: u64,
    inner: S,
    context: Arc<RelayContext>,
    waiters: Slab<oneshot::Sender<Result<Box<dyn Message>, Error>>>,
}
impl<S> RelayWrapper<S> {
    pub fn new(id: u64, inner: S) -> Self {
        Self {
            id,
            inner,
            context: Arc::new(RelayContext {
                receivers: DashMap::new(),
                need_flush: AtomicBool::new(false),
                ready_flag: AtomicBool::new(false),
                idling_flag: AtomicBool::new(true),
                init_sent: AtomicBool::new(false),
                flushed: Notify::new(),
                synchronized: Notify::new(),
                closed: Notify::new(),
                ready: Notify::new(),
                idle: Notify::new(),
            }),
            waiters: sharded_slab::Slab::new_with_config::<SlabCfg>(),
        }
    }
}

impl<S> TypeTagAccept for RelayWrapper<S>
where
    S: Relay + Send + Sync + 'static,
{
    fn iter_types(&self) -> Box<dyn Iterator<Item = TypeTagAcceptItem> + '_> {
        self.inner.iter_types()
    }

    fn accept_msg(&self, msg: &TypeTag) -> bool {
        self.inner.accept_msg(msg)
    }

    fn accept_req(&self, req: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
        self.inner.accept_req(req, resp, err)
    }
}

impl<S> ReceiverTrait for RelayWrapper<S>
where
    S: Relay + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    fn typed(&self) -> Option<AnyReceiver<'_>> {
        None
    }

    fn wrapper(&self) -> Option<AnyWrapperRef<'_>> {
        None
    }
    fn id(&self) -> u64 {
        self.id
    }

    fn send_boxed(
        &self,
        mid: u64,
        boxed_msg: Box<dyn Message>,
        req: bool,
        bus: &Bus,
    ) -> Result<(), Error<Box<dyn Message>>> {
        self.inner.send_msg(mid, boxed_msg, req, bus)
    }

    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<Box<dyn Message>, Error>>,
    ) -> Result<u64, Error> {
        Ok(self
            .waiters
            .insert(listener)
            .ok_or(Error::AddListenerError)? as _)
    }

    fn stats(&self) -> Stats {
        unimplemented!()
    }

    fn send_action(&self, bus: &Bus, action: Action) -> Result<(), Error<Action>> {
        SendUntypedReceiver::send(&self.inner, action, bus)
    }

    fn close_notify(&self) -> &Notify {
        &self.context.closed
    }

    fn sync_notify(&self) -> &Notify {
        &self.context.synchronized
    }

    fn flush_notify(&self) -> &Notify {
        &self.context.flushed
    }

    fn ready_notify(&self) -> &Notify {
        &self.context.ready
    }

    fn idle_notify(&self) -> &Notify {
        &self.context.idle
    }

    fn is_init_sent(&self) -> bool {
        self.context.init_sent.load(Ordering::SeqCst)
    }

    fn is_ready(&self) -> bool {
        self.context.ready_flag.load(Ordering::SeqCst)
    }

    fn is_idling(&self) -> bool {
        self.context.idling_flag.load(Ordering::SeqCst)
    }

    fn need_flush(&self) -> bool {
        self.context.need_flush.load(Ordering::SeqCst)
    }

    fn set_need_flush(&self) {
        self.context.need_flush.store(true, Ordering::SeqCst);
    }

    fn try_reserve(&self, tt: &TypeTag) -> Option<Permit> {
        let context = self
            .context
            .receivers
            .entry(tt.clone())
            .or_insert_with(|| Arc::new(RelayReceiverContext::new(16)))
            .clone();

        loop {
            let count = context.processing.load(Ordering::Relaxed);

            if count < context.limit {
                let res = context.processing.compare_exchange(
                    count,
                    count + 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                if res.is_ok() {
                    break Some(Permit {
                        fuse: false,
                        inner: context.clone(),
                    });
                }

                // continue
            } else {
                break None;
            }
        }
    }

    fn reserve_notify(&self, tt: &TypeTag) -> Arc<Notify> {
        self.context
            .receivers
            .entry(tt.clone())
            .or_insert_with(|| Arc::new(RelayReceiverContext::new(16)))
            .response
            .clone()
    }

    fn increment_processing(&self, tt: &TypeTag) {
        self.context
            .receivers
            .get(tt)
            .map(|r| r.processing.fetch_add(1, Ordering::SeqCst));
    }

    fn start_polling(self: Arc<Self>) -> BusPollerCallback {
        Box::new(move |bus| {
            Box::pin(async move {
                let this = self.clone();
                let events = this.inner.event_stream(bus);
                pin_mut!(events);

                loop {
                    let event = if let Some(event) = events.next().await {
                        event
                    } else {
                        self.context.closed.notify_waiters();
                        break;
                    };

                    match event {
                        Event::Error(err) => error!("Batch Error: {}", err),
                        Event::Pause => self.context.ready_flag.store(false, Ordering::SeqCst),
                        Event::Ready => {
                            self.context.ready_flag.store(true, Ordering::SeqCst);
                            self.context.ready.notify_waiters();
                        }
                        Event::InitFailed(err) => {
                            error!("Relay init failed: {}", err);

                            self.context.ready_flag.store(false, Ordering::SeqCst);
                            self.context.ready.notify_waiters();
                        }
                        Event::Exited => {
                            self.context.closed.notify_waiters();
                            break;
                        }
                        Event::Flushed => {
                            self.context.need_flush.store(false, Ordering::SeqCst);
                            self.context.flushed.notify_one()
                        }
                        Event::Synchronized(_res) => self.context.synchronized.notify_waiters(),
                        Event::Response(mid, resp) => {
                            let tt = if let Ok(bm) = &resp {
                                Some(bm.type_tag())
                            } else {
                                None
                            };

                            if let Some(chan) = self.waiters.take(mid as _) {
                                if let Err(err) = chan.send(resp) {
                                    error!("Response error for mid({}): {:?}", mid, err);
                                }
                            } else {
                                warn!("No waiters for mid({})", mid);
                            };

                            if let Some(tt) = tt {
                                if let Some(ctx) = self.context.receivers.get(&tt) {
                                    ctx.processing.fetch_sub(1, Ordering::SeqCst);
                                    ctx.response.notify_one();
                                }
                            }
                        }

                        Event::BatchComplete(tt, n) => {
                            if let Some(ctx) = self.context.receivers.get(&tt) {
                                ctx.processing.fetch_sub(n, Ordering::SeqCst);

                                for _ in 0..n {
                                    ctx.response.notify_one();
                                }
                            }
                        }
                        _ => unimplemented!(),
                    }
                }
            })
        })
    }
}
