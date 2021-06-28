use core::panic;

use thiserror::Error;
use tokio::sync::oneshot;

use crate::Message;

pub trait StdSyncSendError: std::error::Error + Send + Sync + Unpin + 'static {}
impl<T: std::error::Error + Send + Sync + Unpin + 'static> StdSyncSendError for T {}


#[derive(Debug, Error)]
pub enum VoidError {}

#[derive(Debug, Error)]
pub enum SendError<M: Message> {
    #[error("Closed")]
    Closed(M),

    #[error("Full")]
    Full(M),
}

#[derive(Debug, Error)]
pub enum Error<M: Message = (), E: StdSyncSendError = VoidError> {
    #[error("Message Send Error: {0}")]
    SendError(#[from] SendError<M>),

    #[error("NoResponse")]
    NoResponse,

    #[error("NoReceivers")]
    NoReceivers,

    #[error("Other({0})")]
    Other(E),

    #[error("Other({0})")]
    OtherBoxed(Box<dyn StdSyncSendError>),
}

impl<M: Message, E: StdSyncSendError> Error<M, E> {
    pub fn into_dyn(self) -> Error<M> {
        match self {
            Error::SendError(inner) => Error::SendError(inner),
            Error::NoResponse => Error::NoReceivers,
            Error::NoReceivers => Error::NoReceivers,
            Error::Other(inner) => Error::OtherBoxed(Box::new(inner) as _),
            Error::OtherBoxed(inner) => Error::OtherBoxed(inner),
        }
    }    
    
    pub fn map<U: From<Box<dyn StdSyncSendError>> + StdSyncSendError>(self) -> Error<M, U> {
        match self {
            Error::SendError(inner) => Error::SendError(inner),
            Error::NoResponse => Error::NoReceivers,
            Error::NoReceivers => Error::NoReceivers,
            Error::Other(_) => panic!("expected boxed error!"),
            Error::OtherBoxed(inner) => Error::Other(inner.into()),
        }
    }
}

impl <E: StdSyncSendError> Error<(), E> {
    pub fn specify<M: Message>(self) -> Error<M, E> {
        match self {
            Error::SendError(_) => panic!("cannot specify type on typed error"),
            Error::NoResponse => Error::NoReceivers,
            Error::NoReceivers => Error::NoReceivers,
            Error::Other(inner) => Error::Other(inner),
            Error::OtherBoxed(inner) => Error::OtherBoxed(inner),
        }
    }
}

impl<M: Message, E: StdSyncSendError> From<oneshot::error::RecvError> for Error<M, E> {
    fn from(_: oneshot::error::RecvError) -> Self {
        Error::NoResponse
    }
}

// impl<M: Message, E: StdSyncSendError, U: StdSyncSendError> From<oneshot::error::RecvError> for Error<M, E> {
//     fn from(_: oneshot::error::RecvError) -> Self {
//         Error::NoResponse
//     }
// }