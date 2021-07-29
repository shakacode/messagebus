use std::{collections::HashMap, pin::Pin, sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}}};
use futures::Future;
use tokio::sync::{Notify, oneshot};
use crate::{Bus, Message, Permit, TypeTag, TypeTagged, error::Error, receiver::{Action, AnyReceiver, AnyWrapperArc, AnyWrapperRef, PermitDrop, ReceiverTrait, SendUntypedReceiver, Stats}};



struct SlabCfg;
impl sharded_slab::Config for SlabCfg {
    const RESERVED_BITS: usize = 1;
}

type Slab<T> = sharded_slab::Slab<T, SlabCfg>;

#[macro_export]
macro_rules! type_map {
    (inbound : $($tt:tt)*) => {{
        let mut type_map = TypeMap::new();
        type_map!(@inbound type_map, $($tt)*);
        type_map
    }};

    (outbound : $($tt:tt)*) => {
        let mut type_map = TypeMap::new();
        type_map!(@outbound type_map, $($tt)*);
        type_map
    };

    (@inbound $tm: ident, $msg:ty => $resp: ty, $($tt:tt)*) => {
        $tm.add_inbound(<$msg as TypeTagged>::type_tag_(), <$resp as TypeTagged>::type_tag_(), None);
        type_map!(@inbound $tm, $($tt)*)
    };

    (@inbound $tm: ident, $msg:ty => ($resp:ty) throws $err: ty, $($tt:tt)*) => {
        $tm.add_inbound(<$msg as TypeTagged>::type_tag_(), <$resp as TypeTagged>::type_tag_(), Some(<$err as TypeTagged>::type_tag_()));
        type_map!(@inbound $tm, $($tt)*)
    };

    (@inbound $tm: ident, $msg:ty => $resp: ty) => {
        $tm.add_inbound(<$msg as TypeTagged>::type_tag_(), <$resp as TypeTagged>::type_tag_(), None);
    };

    (@inbound $tm: ident,) => {};

    (@inbound $tm: ident, outbound : $($tt:tt)*) => {
        type_map!(@outbound $tm, $($tt)*)
    };

    (@outbound $tm: ident, $msg:ty => ($resp:ty) throws $err: ty, $($tt:tt)*) => {
        $tm.add_outbound(<$msg as TypeTagged>::type_tag_(), <$resp as TypeTagged>::type_tag_(), Some(<$err as TypeTagged>::type_tag_()));
        type_map!(@outbound $tm, $($tt)*)
    };

    (@outbound $tm: ident, $msg:ty => $resp: ty, $($tt:tt)*) => {
        $tm.add_outbound(<$msg as TypeTagged>::type_tag_(), <$resp as TypeTagged>::type_tag_(), None);
        type_map!(@outbound $tm, $($tt)*)
    };

    (@outbound $tm: ident, $msg:ty => $resp: ty) => {
        $tm.add_outbound(<$msg as TypeTagged>::type_tag_(), <$resp as TypeTagged>::type_tag_(), None);
    };

    (@outbound $tm: ident,) => {};

    (@outbound $tm: ident, inbound : $($tt:tt)*) => {
        type_map!(@inbound $tm, $($tt)*)
    };
}

#[derive(Debug, Clone)]
pub struct TypeMap {
    inbound: HashMap<TypeTag, Vec<(TypeTag, Option<TypeTag>)>>,
    outbound: HashMap<TypeTag, Vec<(TypeTag, Option<TypeTag>)>>,
}

impl TypeMap {
    pub fn new() -> Self {
        Self {
            inbound: Default::default(),
            outbound: Default::default(),
        }
    }

    pub fn add_inbound(&mut self, msg: TypeTag, resp: TypeTag, err: Option<TypeTag>) -> &mut Self {
        let vec = self.inbound.entry(msg)
            .or_insert_with(Vec::new);

        vec.push((resp, err));
        self
    }

    pub fn add_outbound(&mut self, msg: TypeTag, resp: TypeTag, err: Option<TypeTag>) -> &mut Self {
        let vec = self.outbound.entry(msg)
            .or_insert_with(Vec::new);

        vec.push((resp, err));
        self
    }

    pub fn accept_inbound(&self, msg: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
        if let Some(vec) = self.inbound.get(msg) {
            if let Some(rr) = resp {
                vec.iter().find(|(r, e)| {
                    let ee = if let Some(e) = e {
                        if let Some(te) = err {
                            te.as_ref() == e.as_ref()
                        } else {
                            true
                        }
                    } else {
                        err.is_none()
                    }; 

                    r.as_ref() == rr.as_ref() && ee
                }).is_some()
            } else {
                true
            }
        } else {
            false
        }
    }

    pub fn accept_outbound(&self, msg: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
        if let Some(vec) = self.outbound.get(msg) {
            if let Some(rr) = resp {
                vec.iter().find(|(r, e)| {
                    let ee = if let Some(e) = e {
                        if let Some(te) = err {
                            te.as_ref() == e.as_ref()
                        } else {
                            true
                        }
                    } else {
                        err.is_none()
                    }; 

                    r.as_ref() == rr.as_ref() && ee
                }).is_some()
            } else {
                true
            }
        } else {
            false
        }
    }
}

pub(crate) struct RelayContext {
    limit: u64,
    processing: AtomicU64,
    need_flush: AtomicBool,
    flushed: Notify,
    synchronized: Notify,
    closed: Notify,
    response: Notify,
    type_map: TypeMap,
}

impl PermitDrop for RelayContext {
    fn permit_drop(&self) {
        self.processing.fetch_sub(1, Ordering::SeqCst);
    }
}


pub(crate) struct RelayWrapper<S>
where
    S: 'static,
{
    inner: S,
    context: Arc<RelayContext>,
    waiters: Slab<oneshot::Sender<Result<Box<dyn Message>, Error>>>,
}
impl<S> RelayWrapper<S> {
    pub fn new(inner: S, type_map: TypeMap, limit: u64) -> Self {
        Self {
            inner,
            context: Arc::new(RelayContext {
                limit,
                processing: AtomicU64::new(0),
                need_flush: AtomicBool::new(false),
                flushed: Notify::new(),
                synchronized: Notify::new(),
                closed: Notify::new(),
                response: Notify::new(),
                type_map,
            }),
            waiters: sharded_slab::Slab::new_with_config::<SlabCfg>(),
        }
    }
}
impl<S> ReceiverTrait for RelayWrapper<S>
where
    S: SendUntypedReceiver + 'static,
{
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    fn typed(&self) -> Option<AnyReceiver<'_>> { None }
    fn wrapper(&self) -> Option<AnyWrapperRef<'_>> { None }
    fn wrapper_arc(self: Arc<Self>) -> Option<AnyWrapperArc> { None }
    fn send_boxed(
        &self,
        mid: u64,
        boxed_msg: Box<dyn Message>,
    ) -> Result<(), Error<Box<dyn Message>>> {
        Ok(self.inner.send_msg(mid, boxed_msg)?)
    }

    fn need_flush(&self) -> bool {
        self.context.need_flush.load(Ordering::SeqCst)
    }

    fn stats(&self) -> Result<Stats, Error<Action>> {
        unimplemented!()
    }

    fn close(&self) -> Result<(), Error<Action>> {
        Ok(SendUntypedReceiver::send(&self.inner, Action::Close)?)
    }

    fn close_notify(&self) -> &Notify {
        &self.context.closed
    }
    
    fn sync(&self) -> Result<(), Error<Action>> {
        Ok(SendUntypedReceiver::send(&self.inner, Action::Sync)?)
    }

    fn sync_notify(&self) -> &Notify {
        &self.context.synchronized
    }

    fn flush(&self) -> Result<(), Error<Action>> {
        Ok(SendUntypedReceiver::send(&self.inner, Action::Flush)?)
    }

    fn flush_notify(&self) -> &Notify {
        &self.context.flushed
    }

    fn add_response_listener(
        &self,
        listener: oneshot::Sender<Result<Box<dyn Message>, Error>>,
    ) -> Result<u64, Error> {
        Ok(self
            .waiters
            .insert(listener)
            .ok_or_else(|| Error::AddListenerError)? as _)
    }

    fn try_reserve(&self) -> Option<Permit> {
        loop {
            let count = self.context.processing.load(Ordering::Relaxed);

            if count < self.context.limit {
                let res = self.context.processing.compare_exchange(
                    count,
                    count + 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                if res.is_ok() {
                    break Some(Permit {
                        fuse: false,
                        inner: self.context.clone(),
                    });
                }

                // continue
            } else {
                break None;
            }
        }
    }

    fn reserve_notify(&self) -> &Notify {
        &self.context.response
    }

    fn accept(&self, msg: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
        self.context.type_map.accept_inbound(msg, resp, err)
    }

    fn start_polling(self: Arc<Self>) -> Box<dyn FnOnce(Bus) -> Pin<Box<dyn Future<Output = ()> + Send>>> {
        Box::new(move |_| Box::pin(async move {
            
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate as messagebus;

    use crate::derive::{ Message, Error };

    #[derive(Debug, Message)]
    struct Test1;

    #[derive(Debug, Message)]
    struct Test2;

    #[derive(Debug, Message)]
    struct Test3;

    #[derive(Debug, Message)]
    struct Test4;
    #[derive(Debug, Error)]
    struct Error1;
    #[derive(Debug, Error)]
    struct Error2;
    #[test]
    fn test_type_map() {
        let tm = type_map! {
            inbound:
            Test1 => (Test2) throws Error1,
            Test2 => (Test3) throws Error1,

            outbound:
            Test3 => (Test4) throws Error2,
        };

        assert!(tm.accept_inbound(&Test1::type_tag_(), None, None));
        assert!(tm.accept_inbound(&Test2::type_tag_(), None, None));
        assert!(!tm.accept_inbound(&Test3::type_tag_(), None, None));
        assert!(tm.accept_outbound(&Test3::type_tag_(), None, None));

        assert!(tm.accept_inbound(&Test1::type_tag_(), Some(&Test2::type_tag_()), None));
        assert!(!tm.accept_inbound(&Test1::type_tag_(), Some(&Test3::type_tag_()), None));

        assert!(!tm.accept_inbound(&Test2::type_tag_(), Some(&Test2::type_tag_()), None));
        assert!(tm.accept_inbound(&Test2::type_tag_(), Some(&Test3::type_tag_()), None));

        assert!(tm.accept_inbound(&Test1::type_tag_(), Some(&Test2::type_tag_()), Some(&Error1::type_tag_())));
        assert!(!tm.accept_inbound(&Test1::type_tag_(), Some(&Test2::type_tag_()), Some(&Error2::type_tag_())));

        assert!(tm.accept_outbound(&Test3::type_tag_(), Some(&Test4::type_tag_()), Some(&Error2::type_tag_())));
    }
}