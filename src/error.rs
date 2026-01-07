//! Error types for the message bus.
//!
//! This module provides error types used throughout the messagebus library:
//!
//! - [`enum@Error`] - The main error type for bus operations
//! - [`SendError`] - Error when sending messages
//! - [`GenericError`] - A type-erased error wrapper
//!
//! # Creating Custom Error Types
//!
//! Use the derive macro to create handler error types:
//!
//! ```rust,no_run
//! use messagebus::derive::Error as MbError;
//! use thiserror::Error;
//!
//! #[derive(Debug, Error, MbError)]
//! enum MyHandlerError {
//!     #[error("Not found: {0}")]
//!     NotFound(String),
//!
//!     #[error("Invalid input")]
//!     InvalidInput,
//! }
//!
//! fn main() {}
//! ```

use core::fmt;
use std::any::type_name;

use thiserror::Error;
use tokio::sync::oneshot;

use crate::{
    envelop::{IntoBoxedMessage, TypeTag, TypeTagged},
    Message,
};

/// Trait for dynamic error types that can provide a description.
pub trait DynError: TypeTagged {
    /// Returns a human-readable description of the error.
    fn description(&self) -> String;
}

/// Trait bound for error types that can be used in handlers.
///
/// This trait is automatically implemented for any type that implements
/// `Error + TypeTagged + Send + Sync + Unpin + 'static`.
///
/// Use `#[derive(Error)]` from `messagebus::derive` to implement `TypeTagged`
/// on your error types.
pub trait StdSyncSendError: std::error::Error + TypeTagged + Send + Sync + Unpin + 'static {}
impl<T: std::error::Error + TypeTagged + Send + Sync + Unpin + 'static> StdSyncSendError for T {}

/// A type-erased error wrapper.
///
/// Used when the concrete error type is not known at compile time,
/// such as when errors need to be stored or passed through dynamic contexts.
#[derive(Debug)]
pub struct GenericError {
    /// The type tag of the original error.
    pub type_tag: TypeTag,
    /// A formatted description of the error.
    pub description: String,
}

impl GenericError {
    /// Creates a GenericError from any TypeTagged displayable value.
    pub fn from_any<T: TypeTagged + fmt::Display>(err: T) -> Self {
        GenericError {
            type_tag: err.type_tag(),
            description: format!("{}[{}]", err.type_tag(), err),
        }
    }

    /// Creates a GenericError from a type tag and displayable error.
    pub fn from_err(tt: TypeTag, err: impl fmt::Display) -> Self {
        GenericError {
            description: format!("{}[{}]", tt, err),
            type_tag: tt,
        }
    }
}

impl fmt::Display for GenericError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GenericError({}): {}", self.type_tag, self.description)
    }
}

impl std::error::Error for GenericError {}

impl TypeTagged for GenericError {
    fn type_tag_() -> TypeTag {
        type_name::<GenericError>().into()
    }

    fn type_tag(&self) -> TypeTag {
        type_name::<GenericError>().into()
    }

    fn type_name(&self) -> TypeTag {
        type_name::<GenericError>().into()
    }
    fn type_layout(&self) -> std::alloc::Layout {
        std::alloc::Layout::for_value(self)
    }
}

/// Error returned when sending a message fails.
///
/// Contains the original message so it can be recovered or retried.
#[derive(Debug, Error)]
pub enum SendError<M: fmt::Debug> {
    /// The bus or receiver has been closed.
    #[error("Closed")]
    Closed(M),

    /// The receiver's queue is full (for non-blocking sends).
    #[error("Full")]
    Full(M),
}

impl<M: fmt::Debug> SendError<M> {
    /// Maps the message type to a different type.
    pub fn map_msg<UM: fmt::Debug + 'static, F: FnOnce(M) -> UM>(self, f: F) -> SendError<UM> {
        match self {
            SendError::Closed(inner) => SendError::Closed(f(inner)),
            SendError::Full(inner) => SendError::Full(f(inner)),
        }
    }
}

impl<M: Message> SendError<M> {
    /// Converts to a SendError with a boxed message.
    pub fn into_boxed(self) -> SendError<Box<dyn Message>> {
        match self {
            SendError::Closed(m) => SendError::Closed(m.into_boxed()),
            SendError::Full(m) => SendError::Full(m.into_boxed()),
        }
    }
}

/// The main error type for bus operations.
///
/// This error can occur when sending messages, making requests, or during
/// handler execution.
///
/// # Type Parameters
///
/// - `M` - The message type (defaults to `()` for errors without a message)
/// - `E` - The handler error type (defaults to [`GenericError`])
#[derive(Debug, Error)]
pub enum Error<M: fmt::Debug + 'static = (), E: StdSyncSendError = GenericError> {
    /// Error sending a message.
    #[error("Message Send Error: {0}")]
    SendError(#[from] SendError<M>),

    /// The receiver dropped the message; try another receiver.
    #[error("Message receiver dropped try again another receiver")]
    TryAgain(M),

    /// No response was received for a request.
    #[error("NoResponse")]
    NoResponse,

    /// No receivers are registered for the message type.
    #[error("NoReceivers")]
    NoReceivers,

    /// Failed to add a response listener.
    #[error("AddListenerError")]
    AddListenerError,

    /// Failed to cast a message to the expected type.
    #[error("MessageCastError")]
    MessageCastError,

    /// The bus or receiver is not ready.
    #[error("Not Ready")]
    NotReady,

    /// A handler-specific error.
    #[error("Other({0})")]
    Other(E),

    /// Serialization error (for remote messages).
    #[error("Serialization({0})")]
    Serialization(#[from] erased_serde::Error),

    /// A boxed handler error (type-erased).
    #[error("Other({0})")]
    OtherBoxed(Box<dyn StdSyncSendError>),

    /// The message was of the wrong type.
    #[error("WrongMessageType()")]
    WrongMessageType(M),

    /// The type tag is not registered with the bus.
    #[error("TypeTagNotRegistered({0})")]
    TypeTagNotRegistered(TypeTag),

    /// An unknown error occurred.
    #[error("Unknown Error: {0}")]
    Unknown(String),
}

impl<M: fmt::Debug + 'static, E: StdSyncSendError> Error<M, E> {
    pub fn send_closed(m: M) -> Self {
        Error::SendError(SendError::Closed(m))
    }

    pub fn send_full(m: M) -> Self {
        Error::SendError(SendError::Full(m))
    }

    pub fn map_msg<UM: fmt::Debug + 'static, F: FnOnce(M) -> UM>(self, f: F) -> Error<UM, E> {
        match self {
            Error::SendError(inner) => Error::SendError(inner.map_msg(f)),
            Error::TryAgain(inner) => Error::TryAgain(f(inner)),
            Error::NoResponse => Error::NoResponse,
            Error::NoReceivers => Error::NoReceivers,
            Error::Serialization(s) => Error::Serialization(s),
            Error::Other(inner) => Error::Other(inner),
            Error::OtherBoxed(inner) => Error::OtherBoxed(inner),
            Error::WrongMessageType(inner) => Error::WrongMessageType(f(inner)),
            Error::AddListenerError => Error::AddListenerError,
            Error::MessageCastError => Error::MessageCastError,
            Error::TypeTagNotRegistered(tt) => Error::TypeTagNotRegistered(tt),
            Error::NotReady => Error::NotReady,
            Error::Unknown(msg) => Error::Unknown(msg),
        }
    }

    pub fn map_err<UE: StdSyncSendError, F: FnOnce(E) -> UE>(self, f: F) -> Error<M, UE> {
        match self {
            Error::SendError(inner) => Error::SendError(inner),
            Error::TryAgain(inner) => Error::TryAgain(inner),
            Error::NoResponse => Error::NoResponse,
            Error::NoReceivers => Error::NoReceivers,
            Error::Serialization(s) => Error::Serialization(s),
            Error::Other(inner) => Error::Other(f(inner)),
            Error::OtherBoxed(inner) => Error::OtherBoxed(inner),
            Error::WrongMessageType(inner) => Error::WrongMessageType(inner),
            Error::AddListenerError => Error::AddListenerError,
            Error::MessageCastError => Error::MessageCastError,
            Error::TypeTagNotRegistered(tt) => Error::TypeTagNotRegistered(tt),
            Error::NotReady => Error::NotReady,
            Error::Unknown(msg) => Error::Unknown(msg),
        }
    }

    pub fn try_unwrap(self) -> Result<E, Self> {
        match self {
            Error::Other(inner) => Ok(inner),
            s => Err(s),
        }
    }
}

impl<M: Message, E: StdSyncSendError> Error<M, E> {
    pub fn into_dyn(self) -> Error<M> {
        match self {
            Error::SendError(inner) => Error::SendError(inner),
            Error::TryAgain(inner) => Error::TryAgain(inner),
            Error::NoResponse => Error::NoResponse,
            Error::NoReceivers => Error::NoReceivers,
            Error::Serialization(s) => Error::Serialization(s),
            Error::Other(inner) => Error::OtherBoxed(Box::new(inner) as _),
            Error::OtherBoxed(inner) => Error::OtherBoxed(inner),
            Error::WrongMessageType(inner) => Error::WrongMessageType(inner),
            Error::AddListenerError => Error::AddListenerError,
            Error::MessageCastError => Error::MessageCastError,
            Error::TypeTagNotRegistered(tt) => Error::TypeTagNotRegistered(tt),
            Error::NotReady => Error::NotReady,
            Error::Unknown(msg) => Error::Unknown(msg),
        }
    }

    pub fn map<U: From<Box<dyn StdSyncSendError>> + StdSyncSendError>(self) -> Error<M, U> {
        match self {
            Error::SendError(inner) => Error::SendError(inner),
            Error::TryAgain(inner) => Error::TryAgain(inner),
            Error::NoResponse => Error::NoResponse,
            Error::NoReceivers => Error::NoReceivers,
            Error::Serialization(s) => Error::Serialization(s),
            Error::Other(_) => panic!("expected boxed error!"),
            Error::OtherBoxed(inner) => Error::Other(inner.into()),
            Error::WrongMessageType(inner) => Error::WrongMessageType(inner),
            Error::AddListenerError => Error::AddListenerError,
            Error::MessageCastError => Error::MessageCastError,
            Error::TypeTagNotRegistered(tt) => Error::TypeTagNotRegistered(tt),
            Error::NotReady => Error::NotReady,
            Error::Unknown(msg) => Error::Unknown(msg),
        }
    }
}

impl<E: StdSyncSendError> Error<(), E> {
    pub fn specify<M: fmt::Debug>(self) -> Error<M, E> {
        match self {
            Error::SendError(_) => panic!("cannot specify type on typed error"),
            Error::TryAgain(_) => panic!("cannot specify type on typed error"),
            Error::WrongMessageType(_) => panic!("cannot specify type on typed error"),
            Error::NoResponse => Error::NoResponse,
            Error::NoReceivers => Error::NoReceivers,
            Error::Serialization(s) => Error::Serialization(s),
            Error::Other(inner) => Error::Other(inner),
            Error::OtherBoxed(inner) => Error::OtherBoxed(inner),
            Error::AddListenerError => Error::AddListenerError,
            Error::MessageCastError => Error::MessageCastError,
            Error::TypeTagNotRegistered(tt) => Error::TypeTagNotRegistered(tt),
            Error::NotReady => Error::NotReady,
            Error::Unknown(msg) => Error::Unknown(msg),
        }
    }
}

impl<M: fmt::Debug, E: StdSyncSendError> From<oneshot::error::RecvError> for Error<M, E> {
    fn from(_: oneshot::error::RecvError) -> Self {
        Error::NoResponse
    }
}

impl Error<Box<dyn Message>> {
    pub fn from_typed<M: Message>(err: Error<M>) -> Self {
        match err {
            Error::SendError(SendError::Closed(m)) => {
                Error::SendError(SendError::Closed(m.into_boxed()))
            }
            Error::SendError(SendError::Full(m)) => {
                Error::SendError(SendError::Full(m.into_boxed()))
            }
            Error::TryAgain(inner) => Error::TryAgain(inner.into_boxed()),
            Error::WrongMessageType(m) => Error::WrongMessageType(m.into_boxed()),
            Error::NoResponse => Error::NoResponse,
            Error::NoReceivers => Error::NoReceivers,
            Error::Serialization(s) => Error::Serialization(s),
            Error::Other(inner) => Error::Other(inner),
            Error::OtherBoxed(inner) => Error::OtherBoxed(inner),
            Error::AddListenerError => Error::AddListenerError,
            Error::MessageCastError => Error::MessageCastError,
            Error::TypeTagNotRegistered(tt) => Error::TypeTagNotRegistered(tt),
            Error::NotReady => Error::NotReady,
            Error::Unknown(msg) => Error::Unknown(msg),
        }
    }
}

// impl<M: fmt::Debug> Error<M> {
//     pub fn downcast<E>(self) -> Result<E, Self> {
//         match self {
//             Error::OtherBoxed(inner) => Ok(),
//             err => Err(err)
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use std::borrow::Cow;
    use std::sync::Arc;

    use crate::envelop::SharedMessage;

    #[allow(dead_code)]
    #[derive(Debug, Clone)]
    struct TestMsg(i32);

    impl TypeTagged for TestMsg {
        fn type_tag_() -> TypeTag {
            "TestMsg".into()
        }
        fn type_tag(&self) -> TypeTag {
            "TestMsg".into()
        }
        fn type_name(&self) -> Cow<'_, str> {
            "TestMsg".into()
        }
        fn type_layout(&self) -> std::alloc::Layout {
            std::alloc::Layout::for_value(self)
        }
    }

    impl Message for TestMsg {
        fn as_any_ref(&self) -> &dyn Any {
            self
        }
        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
        fn as_any_boxed(self: Box<Self>) -> Box<dyn Any> {
            self
        }
        fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any> {
            self
        }
        fn as_shared_ref(&self) -> Option<&dyn SharedMessage> {
            None
        }
        fn as_shared_mut(&mut self) -> Option<&mut dyn SharedMessage> {
            None
        }
        fn as_shared_boxed(
            self: Box<Self>,
        ) -> Result<Box<dyn SharedMessage>, Box<dyn Message>> {
            Err(self)
        }
        fn as_shared_arc(self: Arc<Self>) -> Option<Arc<dyn SharedMessage>> {
            None
        }
        fn try_clone_into(&self, _into: &mut dyn Any) -> bool {
            false
        }
        fn try_clone_boxed(&self) -> Option<Box<dyn Message>> {
            None
        }
        fn try_clone(&self) -> Option<Self> {
            Some(self.clone())
        }
    }

    #[test]
    fn test_send_error_into_boxed_preserves_closed_variant() {
        let err = SendError::Closed(TestMsg(42));
        let boxed = err.into_boxed();
        assert!(matches!(boxed, SendError::Closed(_)));
    }

    #[test]
    fn test_send_error_into_boxed_preserves_full_variant() {
        let err = SendError::Full(TestMsg(42));
        let boxed = err.into_boxed();
        assert!(matches!(boxed, SendError::Full(_)));
    }
}
