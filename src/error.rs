use core::fmt;
use std::{any::type_name, sync::Arc};

use anyhow::anyhow;

use crate::{Message, SendError};

#[derive(Clone)]
pub struct Error {
    inner: Arc<anyhow::Error>,
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl std::error::Error for Error {}

impl From<anyhow::Error> for Error {
    fn from(t: anyhow::Error) -> Self {
        Self { inner: Arc::new(t) }
    }
}

impl<T: Message> From<SendError<T>> for Error {
    fn from(t: SendError<T>) -> Self {
        Self {
            inner: Arc::new(anyhow!(
                "Message <{}> Sending Error: {:?}",
                type_name::<T>(),
                t
            )),
        }
    }
}
