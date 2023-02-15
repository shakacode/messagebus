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

    #[error("Dynamic Cast Failed: got ({0}), but expected({1})")]
    MessageDynamicCastFail(TypeTag, TypeTag),

    #[error("Try Send Fail Error")]
    TrySendError,

    #[error("MessageCell is empty, it expected to have message!")]
    EmptyMessageCellError,

    #[error("Send Fail")]
    SendError,

    #[error("Handler did not attached any result (it supposed to be send?)")]
    HandlerNoResultAttached,

    #[error("Wrong permit object")]
    WrongPermitObject,

    #[error("Trying to poll wrong task ({0})")]
    ErrorPollWrongTask(String),
}
