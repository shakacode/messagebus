use core::fmt;
use std::{any::Any, sync::Arc};
use thiserror::Error;

use crate::{message::ErrorMessage, type_tag::TypeTag};

pub trait AbstractError: fmt::Debug + Any {}

#[derive(Debug, Clone)]
pub enum HandlerErrorKind {
    InitFailed,
    HandleFailed,
    FlushFailed,
}

#[derive(Debug, Clone, Error)]
pub enum ErrorKind {}

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("Internal Error: {0}")]
    InternalError(&'static str),

    #[error("No Such Receiver Error: MessageTypeQuery ({0:?}) -> {1:?}")]
    NoSuchReceiver(TypeTag, Option<TypeTag>),

    #[error("No Such Receiver Error: MessageTypeQuery")]
    AlreadyInitialized,

    #[error("Handler Error")]
    HandlerError(HandlerErrorKind, Arc<dyn ErrorMessage>),

    #[error("Dynamic Cast Failed Real({0}) Expected({1})")]
    MessageDynamicCastFail(TypeTag, TypeTag),

    #[error("Try Send Fail Error")]
    TrySendError,

    #[error("Send Fail")]
    SendError,
}

impl Error {
    pub(crate) fn no_such_receiver(tid: TypeTag, rid: Option<TypeTag>) -> Self {
        Self::NoSuchReceiver(tid, rid)
    }

    pub(crate) fn already_initialized() -> Error {
        Self::AlreadyInitialized
    }

    pub(crate) fn handler_init_error<E: ErrorMessage>(err: E) -> Self {
        Self::HandlerError(
            HandlerErrorKind::InitFailed,
            Arc::new(err) as Arc<dyn ErrorMessage>,
        )
    }
}
