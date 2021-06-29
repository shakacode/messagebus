use core::any::Any;
use core::fmt;
// use erased_serde::{Deserializer, Serialize};

pub trait Message: Any + fmt::Debug + Unpin + Send + Sync + 'static {}
impl<T: Any + fmt::Debug + Unpin + Send + Sync> Message for T {}
