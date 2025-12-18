//! Message types and traits for the message bus.
//!
//! This module defines the core [`Message`] trait that all messages must implement,
//! along with supporting traits for type tagging and serialization.
//!
//! # Message Derive Macro
//!
//! The easiest way to implement [`Message`] is using the derive macro:
//!
//! ```rust,no_run
//! use messagebus::derive::Message;
//!
//! // Basic message
//! #[derive(Debug, Clone, Message)]
//! struct MyMessage {
//!     data: String,
//! }
//!
//! // Message that can be broadcast to multiple receivers
//! #[derive(Debug, Clone, Message)]
//! #[message(clone)]
//! struct BroadcastMessage {
//!     value: i32,
//! }
//!
//! // Message with custom type tag
//! #[derive(Debug, Clone, Message)]
//! #[type_tag("my_app::custom::MyMessage")]
//! struct CustomTagMessage;
//!
//! // Message with namespace prefix
//! #[derive(Debug, Clone, Message)]
//! #[namespace("my_app::messages")]
//! struct NamespacedMessage;
//!
//! fn main() {}
//! ```
//!
//! For messages that need to be serialized for remote transport, use `#[message(shared)]`
//! along with serde's `Serialize` and `Deserialize` derives.
//!
//! # Type Tags
//!
//! Each message type has a unique type tag used for routing and identification.
//! By default, the type tag is the Rust type name, but this can be customized
//! with the `#[type_tag]` or `#[namespace]` attributes.

use core::{
    any::{type_name, Any},
    fmt,
};

use std::{alloc::Layout, borrow::Cow, sync::Arc};

/// Bounds required for all message types.
///
/// This trait is automatically implemented for any type that implements
/// [`TypeTagged`], [`Debug`](fmt::Debug), [`Unpin`], [`Send`], [`Sync`], and is `'static`.
pub trait MessageBounds: TypeTagged + fmt::Debug + Unpin + Send + Sync + 'static {}
impl<T: TypeTagged + fmt::Debug + Unpin + Send + Sync + 'static> MessageBounds for T {}

/// A type tag identifying a message type.
///
/// Type tags are used for message routing and serialization.
/// They are typically the Rust type name but can be customized.
pub type TypeTag = Cow<'static, str>;

/// Trait for types that have a unique type tag.
///
/// This trait provides type identification for messages and errors,
/// enabling dynamic dispatch and routing based on type.
///
/// Usually implemented via the `#[derive(Message)]` or `#[derive(Error)]` macros.
pub trait TypeTagged {
    /// Returns the type tag for this type (static method).
    fn type_tag_() -> TypeTag
    where
        Self: Sized;

    /// Returns the type tag for this instance.
    fn type_tag(&self) -> TypeTag;

    /// Returns the type name for this instance.
    fn type_name(&self) -> Cow<'_, str>;

    /// Returns the memory layout for this instance.
    fn type_layout(&self) -> Layout;
}

/// The core trait for all messages that can be sent through the bus.
///
/// This trait provides the foundation for message passing, including:
/// - Type erasure via [`Any`] for dynamic dispatch
/// - Optional cloning for broadcast to multiple receivers
/// - Optional serialization for remote transport
///
/// # Implementing Message
///
/// Use the derive macro instead of implementing manually:
///
/// ```rust,no_run
/// use messagebus::derive::Message;
///
/// #[derive(Debug, Clone, Message)]
/// #[message(clone)]  // Enable cloning for broadcast
/// struct MyMessage {
///     data: String,
/// }
///
/// fn main() {}
/// ```
///
/// # Cloning
///
/// Messages marked with `#[message(clone)]` can be broadcast to multiple receivers.
/// The bus will clone the message for each receiver. Without this attribute,
/// messages can only be sent to a single receiver.
///
/// # Shared Messages
///
/// Messages marked with `#[message(shared)]` can be serialized for remote transport.
/// This requires the message to implement `serde::Serialize` and `serde::Deserialize`.
pub trait Message: MessageBounds {
    /// Returns a reference to the message as `&dyn Any`.
    fn as_any_ref(&self) -> &dyn Any;

    /// Returns a mutable reference to the message as `&mut dyn Any`.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Converts a boxed message to `Box<dyn Any>`.
    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any>;

    /// Converts an Arc'd message to `Arc<dyn Any>`.
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any>;

    /// Returns a reference to the message as a shared (serializable) message.
    ///
    /// Returns `None` if the message doesn't support serialization.
    fn as_shared_ref(&self) -> Option<&dyn SharedMessage>;

    /// Returns a mutable reference as a shared message.
    fn as_shared_mut(&mut self) -> Option<&mut dyn SharedMessage>;

    /// Converts a boxed message to a shared message.
    fn as_shared_boxed(self: Box<Self>) -> Result<Box<dyn SharedMessage>, Box<dyn Message>>;

    /// Converts an Arc'd message to a shared message.
    fn as_shared_arc(self: Arc<Self>) -> Option<Arc<dyn SharedMessage>>;

    /// Attempts to clone the message into a destination.
    ///
    /// Returns `true` if cloning succeeded, `false` if the message doesn't support cloning.
    fn try_clone_into(&self, into: &mut dyn Any) -> bool;

    /// Attempts to clone the message into a boxed message.
    fn try_clone_boxed(&self) -> Option<Box<dyn Message>>;

    /// Attempts to clone the message.
    ///
    /// Returns `None` if the message doesn't support cloning
    /// (i.e., not marked with `#[message(clone)]`).
    fn try_clone(&self) -> Option<Self>
    where
        Self: Sized;
}

macro_rules! gen_impls {
    ($t:ty, $($rest:tt)*) => {
        impl TypeTagged for $t {
            fn type_tag_() -> TypeTag {
                type_name::<$t>().into()
            }
            fn type_tag(&self) -> TypeTag {
                type_name::<$t>().into()
            }
            fn type_name(&self) -> Cow<'_, str> {
                type_name::<$t>().into()
            }
            fn type_layout(&self) -> Layout {
                Layout::for_value(self)
            }
        }

        gen_impls!{ $($rest)* }
    };

    ($t:ty) => {
        impl TypeTagged for $t {
            fn type_tag_() -> TypeTag {
                type_name::<$t>().into()
            }
            fn type_tag(&self) -> TypeTag {
                type_name::<$t>().into()
            }
            fn type_name(&self) -> Cow<'_, str> {
                type_name::<$t>().into()
            }
            fn type_layout(&self) -> Layout {
                Layout::for_value(self)
            }
        }
    };
}

gen_impls! {
    (), bool,
    i8, u8,
    i16, u16,
    i32, u32,
    i64, u64,
    i128, u128,
    f32, f64,
    String
}

impl<T: TypeTagged> TypeTagged for Arc<T> {
    fn type_tag_() -> TypeTag {
        T::type_tag_()
    }

    fn type_tag(&self) -> TypeTag {
        T::type_tag(&**self)
    }
    fn type_name(&self) -> Cow<'_, str> {
        T::type_name(&**self)
    }
    fn type_layout(&self) -> Layout {
        Layout::for_value(self)
    }
}

impl<T: TypeTagged> TypeTagged for Box<T> {
    fn type_tag_() -> TypeTag {
        T::type_tag_()
    }

    fn type_tag(&self) -> TypeTag {
        T::type_tag(&**self)
    }
    fn type_name(&self) -> Cow<'_, str> {
        T::type_name(&**self)
    }
    fn type_layout(&self) -> Layout {
        Layout::for_value(self)
    }
}

impl Message for () {
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
        Some(self)
    }
    fn as_shared_mut(&mut self) -> Option<&mut dyn SharedMessage> {
        Some(self)
    }
    fn as_shared_boxed(self: Box<Self>) -> Result<Box<dyn SharedMessage>, Box<dyn Message>> {
        Ok(self)
    }
    fn as_shared_arc(self: Arc<Self>) -> Option<Arc<dyn SharedMessage>> {
        Some(self)
    }
    fn try_clone_into(&self, into: &mut dyn Any) -> bool {
        let into = if let Some(inner) = into.downcast_mut::<Option<()>>() {
            inner
        } else {
            return false;
        };

        into.replace(());
        true
    }
    fn try_clone_boxed(&self) -> Option<Box<dyn Message>> {
        Some(Box::new(()))
    }

    fn try_clone(&self) -> Option<Self> {
        Some(())
    }
}

/// Trait for converting a message into a boxed dynamic message.
pub trait IntoBoxedMessage {
    /// Converts this message into a `Box<dyn Message>`.
    fn into_boxed(self) -> Box<dyn Message>;
}

impl<T: Message> IntoBoxedMessage for T {
    fn into_boxed(self) -> Box<dyn Message> {
        Box::new(self)
    }
}

/// Trait for converting a message into a boxed shared (serializable) message.
pub trait IntoSharedMessage {
    /// Converts this message into a `Box<dyn SharedMessage>`.
    fn into_shared(self) -> Box<dyn SharedMessage>;
}

impl<T: Message + serde::Serialize> IntoSharedMessage for T {
    fn into_shared(self) -> Box<dyn SharedMessage> {
        Box::new(self)
    }
}

/// A message that can be serialized for remote transport.
///
/// This trait is automatically implemented for messages that implement
/// both [`Message`] and `serde::Serialize`. Use `#[message(shared)]`
/// in the derive macro to enable this.
pub trait SharedMessage: Message + erased_serde::Serialize {
    /// Converts an Arc'd shared message to an Arc'd Message.
    fn upcast_arc(self: Arc<Self>) -> Arc<dyn Message>;

    /// Converts a boxed shared message to a boxed Message.
    fn upcast_box(self: Box<Self>) -> Box<dyn Message>;

    /// Returns a reference as a Message.
    fn upcast_ref(&self) -> &dyn Message;

    /// Returns a mutable reference as a Message.
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

// pub trait IntoTakeable {
//     fn into_takeable(&mut self) -> Takeable<'_>;
// }

// impl<T: 'static> IntoTakeable for Option<T> {
//     fn into_takeable(&mut self) -> Takeable<'_> {
//         Takeable {
//             inner_ref: self
//         }
//     }
// }

// pub struct Takeable<'a> {
//     inner_ref: &'a mut dyn Any,
// }

// impl Takeable<'_> {
//     pub fn take<M: Message>(&mut self) -> Option<M> {
//         let m = self.inner_ref.downcast_mut::<Option<M>>()?;
//         m.take()
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use erased_serde::Serializer;
    use std::{any::type_name, borrow::Cow};

    #[allow(dead_code)]
    #[derive(Debug, Clone)]
    struct Msg0;

    impl TypeTagged for Msg0 {
        fn type_tag_() -> TypeTag {
            type_name::<Self>().into()
        }
        fn type_tag(&self) -> TypeTag {
            type_name::<Self>().into()
        }
        fn type_name(&self) -> Cow<'_, str> {
            type_name::<Self>().into()
        }
        fn type_layout(&self) -> Layout {
            Layout::for_value(self)
        }
    }

    impl Message for Msg0 {
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
        fn as_shared_boxed(self: Box<Self>) -> Result<Box<dyn SharedMessage>, Box<dyn Message>> {
            Err(self)
        }
        fn as_shared_arc(self: Arc<Self>) -> Option<Arc<dyn SharedMessage>> {
            None
        }
        fn try_clone_into(&self, _: &mut dyn Any) -> bool {
            false
        }
        fn try_clone_boxed(&self) -> Option<Box<dyn Message>> {
            None
        }

        fn try_clone(&self) -> Option<Self> {
            None
        }
    }

    #[derive(Debug, Clone)]
    struct Msg1;

    impl TypeTagged for Msg1 {
        fn type_tag_() -> TypeTag {
            type_name::<Self>().into()
        }
        fn type_tag(&self) -> TypeTag {
            type_name::<Self>().into()
        }
        fn type_name(&self) -> Cow<'_, str> {
            type_name::<Self>().into()
        }
        fn type_layout(&self) -> Layout {
            Layout::for_value(self)
        }
    }

    impl Message for Msg1 {
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
        fn as_shared_boxed(self: Box<Self>) -> Result<Box<dyn SharedMessage>, Box<dyn Message>> {
            Err(self)
        }
        fn as_shared_arc(self: Arc<Self>) -> Option<Arc<dyn SharedMessage>> {
            None
        }
        fn try_clone_into(&self, into: &mut dyn Any) -> bool {
            let into = if let Some(inner) = into.downcast_mut::<Option<Msg1>>() {
                inner
            } else {
                return false;
            };

            into.replace(self.clone());
            true
        }
        fn try_clone_boxed(&self) -> Option<Box<dyn Message>> {
            Some(Box::new(self.clone()))
        }

        fn try_clone(&self) -> Option<Self> {
            Some(self.clone())
        }
    }

    #[derive(Debug, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
    struct Msg2 {
        inner: [i32; 2],
    }

    impl TypeTagged for Msg2 {
        fn type_tag_() -> TypeTag {
            type_name::<Self>().into()
        }
        fn type_tag(&self) -> TypeTag {
            type_name::<Self>().into()
        }
        fn type_name(&self) -> Cow<'_, str> {
            type_name::<Self>().into()
        }
        fn type_layout(&self) -> Layout {
            Layout::for_value(self)
        }
    }

    impl Message for Msg2 {
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
            Some(self)
        }
        fn as_shared_mut(&mut self) -> Option<&mut dyn SharedMessage> {
            Some(self)
        }
        fn as_shared_boxed(self: Box<Self>) -> Result<Box<dyn SharedMessage>, Box<dyn Message>> {
            Ok(self)
        }
        fn as_shared_arc(self: Arc<Self>) -> Option<Arc<dyn SharedMessage>> {
            Some(self)
        }
        fn try_clone_into(&self, into: &mut dyn Any) -> bool {
            let into = if let Some(inner) = into.downcast_mut::<Option<Msg2>>() {
                inner
            } else {
                return false;
            };

            into.replace(self.clone());
            true
        }
        fn try_clone_boxed(&self) -> Option<Box<dyn Message>> {
            Some(Box::new(self.clone()))
        }

        fn try_clone(&self) -> Option<Self> {
            Some(self.clone())
        }
    }

    #[test]
    fn test_static_upcast() {
        let mut buff: Vec<u8> = Vec::new();
        let json = &mut serde_json::Serializer::new(&mut buff);
        let mut json = <dyn Serializer>::erase(json);

        let x = Msg1;
        let y = Msg2 { inner: [12, 13] };

        assert!(x.as_shared_ref().is_none());
        assert!(y.as_shared_ref().is_some());
        assert!(y
            .as_shared_ref()
            .unwrap()
            .erased_serialize(&mut json)
            .is_ok());
        assert_eq!(buff.as_slice(), b"{\"inner\":[12,13]}");
    }

    #[test]
    fn test_dyn_upcast() {
        let mut buff: Vec<u8> = Vec::new();
        let json = &mut serde_json::Serializer::new(&mut buff);
        let mut json = <dyn Serializer>::erase(json);

        let x = Msg1;
        let y = Msg2 { inner: [12, 13] };

        let x_dyn: &dyn Message = &x;
        let y_dyn: &dyn Message = &y;

        assert!(x_dyn.as_shared_ref().is_none());
        assert!(y_dyn.as_shared_ref().is_some());
        assert!(y_dyn
            .as_shared_ref()
            .unwrap()
            .erased_serialize(&mut json)
            .is_ok());
        assert_eq!(buff.as_slice(), b"{\"inner\":[12,13]}");
    }
}
