use crate::{type_tag::TypeTag, Message};
use ctxerr::ctxerr;
use std::sync::Arc;

pub trait AbstractError: std::error::Error + Message {}
impl<T: std::error::Error + Message> AbstractError for T {}

#[derive(Clone)]
#[ctxerr]
pub enum ErrorKind {
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

    #[error("Bus is closed!")]
    BusClosed,

    #[error("Handler Error: {0}")]
    HandlerError(Arc<dyn AbstractError>),
}

impl Clone for Error {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind.clone(),
            backtrace: None,
            location: None,
        }
    }
}

pub trait IntoError {
    fn into_error(self) -> Error;
}

impl<T: AbstractError> IntoError for T {
    fn into_error(self) -> Error {
        ErrorKind::HandlerError(Arc::new(self)).into()
    }
}
