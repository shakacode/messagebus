use core::any::Any;

use crate::{
    error::{Error, ErrorKind},
    message::Message,
    type_tag::TypeTag,
};

pub trait MessageCell: Send + 'static {
    fn is_empty(&self) -> bool;
    fn type_tag(&self) -> TypeTag;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn finalize(&mut self);
    fn deserialize_from(&mut self, de: &mut dyn erased_serde::Deserializer) -> Result<(), Error>;
}

impl dyn MessageCell {
    pub fn put<T: Message>(&mut self, val: T) -> Result<(), Error> {
        let tt = self.type_tag();
        match self.as_any_mut().downcast_mut::<MsgCell<T>>() {
            Some(inner) => inner.put(val),
            None => return Err(ErrorKind::MessageDynamicCastFail(tt, T::TYPE_TAG()).into()),
        }

        Ok(())
    }

    pub fn put_result<T: Message>(&mut self, val: Result<T, Error>) -> Result<(), Error> {
        let tt = self.type_tag();
        match self.as_any_mut().downcast_mut::<ResultCell<T>>() {
            Some(inner) => inner.put(val),
            None => return Err(ErrorKind::MessageDynamicCastFail(tt, T::TYPE_TAG()).into()),
        }

        Ok(())
    }

    pub fn into_typed<T: Message>(&mut self) -> Result<&mut MsgCell<T>, Error> {
        let tt = self.type_tag();
        match self.as_any_mut().downcast_mut::<MsgCell<T>>() {
            Some(inner) => Ok(inner),
            None => Err(ErrorKind::MessageDynamicCastFail(tt, T::TYPE_TAG()).into()),
        }
    }

    pub fn into_typed_result<R: Message>(&mut self) -> Result<&mut ResultCell<R>, Error> {
        let tt = self.type_tag();
        match self.as_any_mut().downcast_mut::<ResultCell<R>>() {
            Some(inner) => Ok(inner),
            None => Err(ErrorKind::MessageDynamicCastFail(tt, R::TYPE_TAG()).into()),
        }
    }

    pub fn take_cell<T: Message>(&mut self) -> Result<MsgCell<T>, Error> {
        match self.as_any_mut().downcast_mut::<MsgCell<T>>() {
            Some(cell) => Ok(MsgCell(cell.0.take())),
            None => Err(ErrorKind::MessageDynamicCastFail(self.type_tag(), T::TYPE_TAG()).into()),
        }
    }

    pub fn take<T: Message>(&mut self) -> Result<T, Error> {
        match self.as_any_mut().downcast_mut::<MsgCell<T>>() {
            Some(cell) => cell.take().ok_or(ErrorKind::EmptyMessageCellError.into()),
            None => Err(ErrorKind::MessageDynamicCastFail(self.type_tag(), T::TYPE_TAG()).into()),
        }
    }

    // pub fn cloned<T: Message>(&self) -> Result<MsgCell<T>, Error> {
    //     match self.as_any().downcast_ref::<MsgCell<T>>() {
    //         Some(cell) => Ok(cell.clone()),
    //         None => Err(Error::MessageDynamicCastFail(
    //             self.type_tag(),
    //             T::TYPE_TAG(),
    //         )),
    //     }
    // }
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
            None => Err(ErrorKind::HandlerNoResultAttached.into()),
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

    fn finalize(&mut self) {}

    fn deserialize_from(&mut self, _de: &mut dyn erased_serde::Deserializer) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Debug)]
enum MsgCellInner<M> {
    Empty,
    Clonable(M),
    Message(M),
}

impl<M: Message> MsgCellInner<M> {
    pub fn as_ref(&self) -> Option<&M> {
        match self {
            MsgCellInner::Empty => None,
            MsgCellInner::Clonable(x) => Some(x),
            MsgCellInner::Message(x) => Some(x),
        }
    }

    #[inline]
    fn get(&mut self) -> M {
        match std::mem::replace(self, MsgCellInner::Empty) {
            MsgCellInner::Empty => panic!("!!!"),
            MsgCellInner::Clonable(m) => {
                if let Some(x) = m.try_clone() {
                    *self = MsgCellInner::Clonable(m);
                    x
                } else {
                    m
                }
            }
            MsgCellInner::Message(m) => m,
        }
    }

    #[inline]
    fn take(&mut self) -> MsgCellInner<M> {
        std::mem::replace(self, MsgCellInner::Empty)
    }

    #[inline]
    fn take_option(self) -> Option<M> {
        match self {
            MsgCellInner::Empty => None,
            MsgCellInner::Clonable(m) => Some(m),
            MsgCellInner::Message(m) => Some(m),
        }
    }

    #[inline]
    fn finalize(&mut self) {
        *self = match std::mem::replace(self, MsgCellInner::Empty) {
            MsgCellInner::Empty => MsgCellInner::Empty,
            MsgCellInner::Clonable(m) => MsgCellInner::Message(m),
            MsgCellInner::Message(m) => MsgCellInner::Message(m),
        }
    }

    #[inline]
    fn put(&mut self, val: M) {
        *self = MsgCellInner::new(val);
    }

    #[inline]
    fn unwrap_or<T>(self, err: T) -> Result<M, T> {
        match self {
            MsgCellInner::Empty => Err(err),
            MsgCellInner::Clonable(m) => Ok(m),
            MsgCellInner::Message(m) => Ok(m),
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        matches!(self, MsgCellInner::Empty)
    }

    #[inline]
    fn new(msg: M) -> MsgCellInner<M> {
        if msg.is_cloneable() {
            MsgCellInner::Clonable(msg)
        } else {
            MsgCellInner::Message(msg)
        }
    }
}

#[derive(Debug)]
pub struct MsgCell<M>(MsgCellInner<M>);

impl<M: Message> MsgCell<M> {
    pub fn new(msg: M) -> Self {
        MsgCell(MsgCellInner::new(msg))
    }

    pub fn empty() -> Self {
        MsgCell(MsgCellInner::Empty)
    }

    #[inline]
    pub fn peek(&self) -> &M {
        self.0.as_ref().unwrap()
    }

    #[inline]
    pub fn get(&mut self) -> M {
        self.0.get()
    }

    #[inline]
    pub fn take(&mut self) -> Option<M> {
        self.0.take().take_option()
    }

    #[inline]
    pub fn take_cell(&mut self) -> MsgCell<M> {
        MsgCell(self.0.take())
    }

    #[inline]
    pub fn put(&mut self, val: M) {
        self.0.put(val);
    }

    #[inline]
    pub fn make_value(&mut self) {
        self.0.finalize()
    }

    #[inline]
    pub fn as_dyn(&mut self) -> &mut dyn MessageCell {
        self
    }

    // #[inline]
    // pub fn clone(&self) -> MsgCell<M> {
    //     MsgCell(MsgCellInner::)
    // }
}

impl<M: Message> MessageCell for MsgCell<M> {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
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

    fn finalize(&mut self) {
        self.0.finalize()
    }

    fn deserialize_from(&mut self, _de: &mut dyn erased_serde::Deserializer) -> Result<(), Error> {
        Ok(())
    }
}
