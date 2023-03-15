#![feature(type_alias_impl_trait)]

pub mod bus;
pub mod cell;
pub mod error;
pub mod handler;
pub mod message;
pub mod permit;
pub mod polling_pool;
pub mod receiver;
pub mod receivers;
pub mod type_tag;

pub use bus::Bus;
pub use handler::*;
pub use message::*;

mod wakelist;
#[macro_export]
macro_rules! derive_message_clone {
    ($const_name: ident, $struct_name: ty, $name: literal) => {
        lazy_static::lazy_static! {
            static ref $const_name: $crate::type_tag::TypeTag = $crate::type_tag::TypeTagInfo::parse($name).unwrap().into();
        }

        impl $crate::Message for $struct_name {
            #[allow(non_snake_case)]
            fn TYPE_TAG() -> $crate::type_tag::TypeTag
            where
                Self: Sized,
            {
                $const_name.clone()
            }

            fn type_tag(&self) -> $crate::type_tag::TypeTag {
                $const_name.clone()
            }

            fn type_layout(&self) -> std::alloc::Layout {
                std::alloc::Layout::new::<Self>()
            }

            fn as_any_ref(&self) -> &dyn std::any::Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }

            fn as_any_boxed(self: Box<Self>) -> Box<dyn std::any::Any> {
                self
            }

            fn as_any_arc(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn std::any::Any> {
                self
            }

            fn as_shared_ref(&self) -> Option<&dyn $crate::message::SharedMessage> {
                None
            }

            fn as_shared_mut(&mut self) -> Option<&mut dyn $crate::message::SharedMessage> {
                None
            }

            fn as_shared_boxed(
                self: Box<Self>,
            ) -> Result<Box<dyn $crate::message::SharedMessage>, Box<dyn $crate::Message>> {
                Err(self)
            }

            fn as_shared_arc(
                self: std::sync::Arc<Self>,
            ) -> Option<std::sync::Arc<dyn $crate::message::SharedMessage>> {
                None
            }

            fn try_clone_into(&self, into: &mut dyn $crate::cell::MessageCell) -> bool {
                into.into_typed::<Self>()
                    .map(|c| c.put(self.clone()))
                    .is_ok()
            }

            fn try_clone_boxed(&self) -> Option<Box<dyn $crate::Message>> {
                Some(Box::new(self.clone()))
            }

            fn is_cloneable(&self) -> bool {
                true
            }

            fn try_clone(&self) -> Option<Self>
            where
                Self: Sized,
            {
                Some(self.clone())
            }
        }
    };
}

#[macro_export]
macro_rules! derive_message{
    ($const_name: ident, $struct_name: ty, $name: literal) => {
        lazy_static::lazy_static! {
            static ref $const_name: $crate::type_tag::TypeTag = $crate::type_tag::TypeTagInfo::parse($name).unwrap().into();
        }

        impl $crate::Message for $struct_name {
            #[allow(non_snake_case)]
            fn TYPE_TAG() -> $crate::type_tag::TypeTag
            where
                Self: Sized,
            {
                $const_name.clone()
            }

            fn type_tag(&self) -> $crate::type_tag::TypeTag {
                $const_name.clone()
            }

            fn type_layout(&self) -> std::alloc::Layout {
                std::alloc::Layout::new::<Self>()
            }

            fn as_any_ref(&self) -> &dyn std::any::Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }

            fn as_any_boxed(self: Box<Self>) -> Box<dyn std::any::Any> {
                self
            }

            fn as_any_arc(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn std::any::Any> {
                self
            }

            fn as_shared_ref(&self) -> Option<&dyn $crate::message::SharedMessage> {
                None
            }

            fn as_shared_mut(&mut self) -> Option<&mut dyn $crate::message::SharedMessage> {
                None
            }

            fn as_shared_boxed(
                self: Box<Self>,
            ) -> Result<Box<dyn $crate::message::SharedMessage>, Box<dyn $crate::Message>> {
                Err(self)
            }

            fn as_shared_arc(
                self: std::sync::Arc<Self>,
            ) -> Option<std::sync::Arc<dyn $crate::message::SharedMessage>> {
                None
            }

            fn try_clone_into(&self, _into: &mut dyn $crate::cell::MessageCell) -> bool {
                false
            }

            fn try_clone_boxed(&self) -> Option<Box<dyn $crate::Message>> {
                None
            }

            fn is_cloneable(&self) -> bool {
                false
            }

            fn try_clone(&self) -> Option<Self>
            where
                Self: Sized,
            {
                None
            }
        }
    };
}

derive_message_clone!(VOID, (), "void");
