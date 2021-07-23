use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Notify, oneshot};
use crate::{Message, Permit, TypeTag, error::Error, receiver::{Action, AnyReceiver, AnyWrapperArc, AnyWrapperRef, ReceiverTrait, SendUntypedReceiver, Stats}};

struct SlabCfg;
impl sharded_slab::Config for SlabCfg {
    const RESERVED_BITS: usize = 1;
}

type Slab<T> = sharded_slab::Slab<T, SlabCfg>;


pub enum TypeTagKind {
    Message,
    Response,
    Error,
}

pub struct TypeDescriptor {
    kind: TypeTagKind,
    
}

pub struct TypeMap {
    hash_map: HashMap<TypeTag, TypeDescriptor>
}

pub(crate) struct RelayContext {
    
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
    pub fn new(inner: S, limit: u64) -> Self {
        Self {
            inner,
            context: Arc::new(RelayContext {

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
        false
    }

    fn stats(&self) -> Result<Stats, Error<Action>> {
        unimplemented!()
    }

    fn close(&self) -> Result<(), Error<Action>> {
        Ok(SendUntypedReceiver::send(&self.inner, Action::Close)?)
    }

    fn close_notify(&self) -> &Notify {
        unimplemented!()
    }
    
    fn sync(&self) -> Result<(), Error<Action>> {
        Ok(SendUntypedReceiver::send(&self.inner, Action::Sync)?)
    }

    fn sync_notify(&self) -> &Notify {
        unimplemented!()
    }

    fn flush(&self) -> Result<(), Error<Action>> {
        Ok(SendUntypedReceiver::send(&self.inner, Action::Flush)?)
    }

    fn flush_notify(&self) -> &Notify {
        unimplemented!()
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
        None
    }

    fn reserve_notify(&self) -> &Notify {
        unimplemented!()
    }

    fn accept_message_type(&self, tt: &TypeTag) -> bool {
        false
    }

    fn accept_response_type(&self, tt: &TypeTag) -> bool {
        false
    }

    fn accept_error_type(&self, tt: &TypeTag) -> bool {
        false
    }
}