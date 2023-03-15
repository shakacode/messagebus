use std::sync::Arc;
use thiserror::Error;

use crate::{type_tag::TypeTag, Message};

pub trait AbstractError: std::error::Error + Message {}
impl<T: std::error::Error + Message> AbstractError for T {}

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("Internal Error: {0}")]
    InternalError(&'static str),

    #[error("No Such Receiver Error: MessageTypeQuery ({0:?}) -> {1:?}")]
    NoSuchReceiver(TypeTag, Option<TypeTag>),

    #[error("No Such Receiver Error: MessageTypeQuery")]
    AlreadyInitialized,

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

    #[error("Marker indicats that producer finished producing.")]
    ProducerFinished,

    #[error("Handler Error: {0}")]
    HandlerError(Arc<dyn AbstractError>),
}

impl<T: AbstractError> From<T> for Error {
    fn from(value: T) -> Self {
        Self::HandlerError(Arc::new(value))
    }
}
