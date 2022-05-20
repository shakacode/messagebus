use core::any::Any;
use std::sync::Arc;

use crossbeam::atomic::AtomicCell;
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
use tokio::sync::Notify;

use crate::{error::Error, message::Message, type_tag::TypeTag};

struct SharedCellInner<I: ?Sized> {
    type_tag: TypeTag,
    notify: Notify,
    inner: I,
}

pub struct SharedCell {
    inner: Arc<SharedCellInner<dyn Any + Send + Sync>>,
}

impl SharedCell {
    pub fn set<M: Message>(self, val: M) -> Result<(), Error> {
        self.inner
            .inner
            .downcast_ref::<AtomicCell<MsgCell<M>>>()
            .ok_or_else(|| {
                Error::MessageDynamicCastFail(self.inner.type_tag.clone(), M::TYPE_TAG())
            })?
            .store(MsgCell::new(val));

        self.inner.notify.notify_waiters();

        Ok(())
    }
}

pub struct MsgCell<M: Message>(Mutex<Option<M>>);
impl<M: Message> MsgCell<M> {
    pub fn new(msg: M) -> Self {
        MsgCell(Mutex::new(Some(msg)))
    }

    pub fn empty() -> Self {
        MsgCell(Mutex::new(None))
    }

    pub fn take(&mut self) -> Option<M> {
        self.0.lock().take()
    }

    pub fn put(&mut self, val: M) {
        self.0.lock().replace(val);
    }

    pub fn as_dyn(&mut self) -> &mut dyn MessageCell {
        self
    }

    pub fn clone(&self) -> MsgCell<M> {
        MsgCell(Mutex::new(
            self.0.lock().as_ref().and_then(|x| x.try_clone()),
        ))
    }
}

pub trait MessageCell: Send + 'static {
    fn is_empty(&self) -> bool;
    fn type_tag(&self) -> TypeTag;
    fn take_dyn(&mut self) -> Option<Box<dyn Message>>;
    fn peek_dyn(&self) -> Option<MappedMutexGuard<'_, dyn Message>>;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<M: Message> MessageCell for MsgCell<M> {
    fn is_empty(&self) -> bool {
        self.0.lock().is_none()
    }

    fn take_dyn(&mut self) -> Option<Box<dyn Message>> {
        self.0
            .lock()
            .take()
            .map(|x| Box::new(x) as Box<dyn Message>)
    }

    fn peek_dyn(&self) -> Option<MappedMutexGuard<'_, dyn Message>> {
        let lock = self.0.lock();

        if lock.is_some() {
            return Some(MutexGuard::map(lock, |x| {
                x.as_mut().unwrap() as &mut dyn Message
            }));
        } else {
            None
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn type_tag(&self) -> TypeTag {
        M::TYPE_TAG()
    }
}

impl dyn MessageCell {
    pub fn put<T: Message>(&mut self, val: T) -> Result<(), Error> {
        let tt = self.type_tag();
        match self.as_any_mut().downcast_mut::<MsgCell<T>>() {
            Some(inner) => inner.put(val),
            None => return Err(Error::MessageDynamicCastFail(tt, T::TYPE_TAG())),
        }

        Ok(())
    }

    pub fn into_typed<T: Message>(&mut self) -> Result<&mut MsgCell<T>, Error> {
        let tt = self.type_tag();
        match self.as_any_mut().downcast_mut::<MsgCell<T>>() {
            Some(inner) => Ok(inner),
            None => Err(Error::MessageDynamicCastFail(tt, T::TYPE_TAG())),
        }
    }

    pub fn take_cell<T: Message>(&mut self) -> Result<MsgCell<T>, Error> {
        match self.as_any_mut().downcast_mut::<MsgCell<T>>() {
            Some(cell) => Ok(MsgCell(Mutex::new(cell.take()))),
            None => Err(Error::MessageDynamicCastFail(
                self.type_tag(),
                T::TYPE_TAG(),
            )),
        }
    }

    pub fn take<T: Message>(&mut self) -> Result<T, Error> {
        match self.as_any_mut().downcast_mut::<MsgCell<T>>() {
            Some(cell) => cell.take().ok_or_else(|| Error::SendError),
            None => Err(Error::MessageDynamicCastFail(
                self.type_tag(),
                T::TYPE_TAG(),
            )),
        }
    }

    pub fn cloned<T: Message>(&self) -> Result<MsgCell<T>, Error> {
        match self.as_any().downcast_ref::<MsgCell<T>>() {
            Some(cell) => Ok(cell.clone()),
            None => Err(Error::MessageDynamicCastFail(
                self.type_tag(),
                T::TYPE_TAG(),
            )),
        }
    }
}
