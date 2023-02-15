use core::any::Any;

use crate::{error::Error, message::Message, type_tag::TypeTag};

pub trait MessageCell: Send + 'static {
    fn is_empty(&self) -> bool;
    fn type_tag(&self) -> TypeTag;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn deserialize_from(&mut self, de: &mut dyn erased_serde::Deserializer) -> Result<(), Error>;
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

    pub fn put_result<T: Message>(&mut self, val: Result<T, Error>) -> Result<(), Error> {
        let tt = self.type_tag();
        match self.as_any_mut().downcast_mut::<ResultCell<T>>() {
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

    pub fn into_typed_result<R: Message>(&mut self) -> Result<&mut ResultCell<R>, Error> {
        let tt = self.type_tag();
        match self.as_any_mut().downcast_mut::<ResultCell<R>>() {
            Some(inner) => Ok(inner),
            None => Err(Error::MessageDynamicCastFail(tt, R::TYPE_TAG())),
        }
    }

    pub fn take_cell<T: Message>(&mut self) -> Result<MsgCell<T>, Error> {
        match self.as_any_mut().downcast_mut::<MsgCell<T>>() {
            Some(cell) => Ok(MsgCell(cell.take().ok())),
            None => Err(Error::MessageDynamicCastFail(
                self.type_tag(),
                T::TYPE_TAG(),
            )),
        }
    }

    pub fn take<T: Message>(&mut self) -> Result<T, Error> {
        match self.as_any_mut().downcast_mut::<MsgCell<T>>() {
            Some(cell) => cell.take(),
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

#[derive(Debug, Clone)]
pub struct ResultCell<R: Message>(Option<Result<R, Error>>);

impl<R: Message> ResultCell<R> {
    #[inline]
    pub fn new(result: Result<R, Error>) -> Self {
        ResultCell(Some(result))
    }

    #[inline]
    pub fn empty() -> Self {
        ResultCell(None)
    }

    #[inline]
    pub fn take(&mut self) -> Option<Result<R, Error>> {
        self.0.take()
    }

    #[inline]
    pub fn put(&mut self, val: Result<R, Error>) {
        self.0.replace(val);
    }

    #[inline]
    pub fn as_dyn(&mut self) -> &mut dyn MessageCell {
        self
    }

    #[inline]
    pub fn unwrap(&mut self) -> Result<R, Error> {
        match self.0.take() {
            Some(inner) => inner,
            None => Err(Error::HandlerNoResultAttached),
        }
    }
}

impl<R: Message> MessageCell for ResultCell<R> {
    fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn type_tag(&self) -> TypeTag {
        R::TYPE_TAG()
    }

    fn deserialize_from(&mut self, _de: &mut dyn erased_serde::Deserializer) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MsgCell<M>(Option<M>);

impl<M: Message> MsgCell<M> {
    pub fn new(msg: M) -> Self {
        MsgCell(Some(msg))
    }

    pub fn empty() -> Self {
        MsgCell(None)
    }

    #[inline]
    pub fn peek(&self) -> &M {
        self.0.as_ref().unwrap()
    }

    #[inline]
    pub fn take(&mut self) -> Result<M, Error> {
        self.0.take().ok_or(Error::EmptyMessageCellError)
    }

    #[inline]
    pub fn take_cell(&mut self) -> MsgCell<M> {
        MsgCell(self.0.take())
    }

    #[inline]
    pub fn put(&mut self, val: M) {
        self.0.replace(val);
    }

    #[inline]
    pub fn as_dyn(&mut self) -> &mut dyn MessageCell {
        self
    }

    #[inline]
    pub fn clone(&self) -> MsgCell<M> {
        MsgCell(self.0.as_ref().and_then(|x| x.try_clone()))
    }
}

impl<M: Message> MessageCell for MsgCell<M> {
    fn is_empty(&self) -> bool {
        self.0.is_none()
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

    fn deserialize_from(&mut self, _de: &mut dyn erased_serde::Deserializer) -> Result<(), Error> {
        Ok(())
    }
}
