use core::any::{Any, type_name};
use core::fmt;
use serde::{de::DeserializeOwned, Serialize};

pub trait Message:
    fmt::Debug + Unpin + Send + Sync + 'static
{
    fn type_name(&self) -> &str;
}

impl<T: fmt::Debug + Unpin + Send + Sync + 'static> Message for T {
    fn type_name(&self) -> &str {
        type_name::<T>()
    }
}

pub trait TransferableMessage: Message + Serialize + DeserializeOwned
{
    fn into_boxed(self) -> BoxedMessage; 
}
impl<T: Message + Serialize + DeserializeOwned> TransferableMessage for T {
    fn into_boxed(self) -> BoxedMessage {
        BoxedMessage(Box::new(self) as _)
    }
}

pub trait SafeMessage:
    Any + fmt::Debug + erased_serde::Serialize + Unpin + Send + Sync + 'static
{
    fn type_name(&self) -> &str;
}
impl<T: Any + fmt::Debug + erased_serde::Serialize + Unpin + Send + Sync> SafeMessage for T {
    fn type_name(&self) -> &str {
        type_name::<T>()
    }
}

#[derive(Debug)]
pub struct BoxedMessage(Box<dyn SafeMessage>);

impl<M: TransferableMessage> From<M> for BoxedMessage {
    fn from(m: M) -> Self {
        BoxedMessage(Box::new(m))
    }
}
