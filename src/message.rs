use core::fmt;
use std::{alloc::Layout, any::Any, sync::Arc};

use crate::type_tag::TypeTag;

pub trait ErrorMessage: Message {}

pub trait Message: fmt::Debug + Unpin + Send + Sync + 'static {
    #[allow(non_snake_case)]
    fn TYPE_TAG() -> TypeTag
    where
        Self: Sized;

    fn type_tag(&self) -> TypeTag;
    fn type_layout(&self) -> Layout;

    fn as_any_ref(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any>;
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any>;

    fn as_shared_ref(&self) -> Option<&dyn SharedMessage>;
    fn as_shared_mut(&mut self) -> Option<&mut dyn SharedMessage>;
    fn as_shared_boxed(self: Box<Self>) -> Result<Box<dyn SharedMessage>, Box<dyn Message>>;
    fn as_shared_arc(self: Arc<Self>) -> Option<Arc<dyn SharedMessage>>;

    fn try_clone_into(&self, into: &mut dyn Message) -> bool;
    fn try_clone_boxed(&self) -> Option<Box<dyn Message>>;

    fn try_clone(&self) -> Option<Self>
    where
        Self: Sized;
}

pub trait SharedMessage: Message + erased_serde::Serialize {
    fn upcast_arc(self: Arc<Self>) -> Arc<dyn Message>;
    fn upcast_box(self: Box<Self>) -> Box<dyn Message>;
    fn upcast_ref(&self) -> &dyn Message;
    fn upcast_mut(&mut self) -> &mut dyn Message;
}

impl<T: Message + erased_serde::Serialize> SharedMessage for T {
    fn upcast_arc(self: Arc<Self>) -> Arc<dyn Message> {
        self
    }
    fn upcast_box(self: Box<Self>) -> Box<dyn Message> {
        self
    }
    fn upcast_ref(&self) -> &dyn Message {
        self
    }
    fn upcast_mut(&mut self) -> &mut dyn Message {
        self
    }
}

pub trait IntoBoxedMessage {
    fn into_boxed(self) -> Box<dyn Message>;
}

impl<T: Message> IntoBoxedMessage for T {
    fn into_boxed(self) -> Box<dyn Message> {
        Box::new(self)
    }
}

pub trait IntoSharedMessage {
    fn into_shared(self) -> Box<dyn SharedMessage>;
}

impl<T: Message + serde::Serialize> IntoSharedMessage for T {
    fn into_shared(self) -> Box<dyn SharedMessage> {
        Box::new(self)
    }
}
