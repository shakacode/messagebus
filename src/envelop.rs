use core::{
    any::{type_name, Any},
    fmt,
};

use std::{alloc::Layout, borrow::Cow, sync::Arc};

pub trait MessageBounds: TypeTagged + fmt::Debug + Unpin + Send + Sync + 'static {}
impl<T: TypeTagged + fmt::Debug + Unpin + Send + Sync + 'static> MessageBounds for T {}

pub type TypeTag = Cow<'static, str>;

pub trait TypeTagged {
    fn type_tag_() -> TypeTag
    where
        Self: Sized;

    fn type_tag(&self) -> TypeTag;
    fn type_name(&self) -> Cow<str>;
    fn type_layout(&self) -> Layout;
}

pub trait Message: MessageBounds {
    fn as_any_ref(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any>;
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any>;

    fn as_shared_ref(&self) -> Option<&dyn SharedMessage>;
    fn as_shared_mut(&mut self) -> Option<&mut dyn SharedMessage>;
    fn as_shared_boxed(self: Box<Self>) -> Result<Box<dyn SharedMessage>, Box<dyn Message>>;
    fn as_shared_arc(self: Arc<Self>) -> Option<Arc<dyn SharedMessage>>;

    fn try_clone_into(&self, into: &mut dyn Any) -> bool;
    fn try_clone_boxed(&self) -> Option<Box<dyn Message>>;

    fn try_clone(&self) -> Option<Self> where Self: Sized;
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
            fn type_name(&self) -> Cow<str> {
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
            fn type_name(&self) -> Cow<str> {
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
        T::type_tag(&*self)
    }
    fn type_name(&self) -> Cow<str> {
        T::type_name(&*self)
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
        T::type_tag(&*self)
    }
    fn type_name(&self) -> Cow<str> {
        T::type_name(&*self)
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

pub trait IntoBoxedMessage {
    fn into_boxed(self) -> Box<dyn Message>;
}

impl<T: Message> IntoBoxedMessage for T {
    fn into_boxed(self) -> Box<dyn Message> {
        Box::new(self)
    }
}

pub trait IntoSharedMessage {
    fn into_shared(self) -> Box<dyn SharedMessage>;
}

impl<T: Message + serde::Serialize> IntoSharedMessage for T {
    fn into_shared(self) -> Box<dyn SharedMessage> {
        Box::new(self)
    }
}


pub trait SharedMessage: Message + erased_serde::Serialize {
    fn upcast_arc(self: Arc<Self>) -> Arc<dyn Message>;
    fn upcast_box(self: Box<Self>) -> Box<dyn Message>;
    fn upcast_ref(&self) -> &dyn Message;
    fn upcast_mut(&mut self) -> &mut dyn Message;
}
impl<T: Message + erased_serde::Serialize> SharedMessage for T {
    fn upcast_arc(self: Arc<Self>) -> Arc<dyn Message> { self }
    fn upcast_box(self: Box<Self>) -> Box<dyn Message> { self }
    fn upcast_ref(&self) -> &dyn Message { self }
    fn upcast_mut(&mut self) -> &mut dyn Message { self }
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

    #[derive(Debug, Clone)]
    struct Msg0;

    impl TypeTagged for Msg0 {
        fn type_tag_() -> TypeTag {
            type_name::<Self>().into()
        }
        fn type_tag(&self) -> TypeTag {
            type_name::<Self>().into()
        }
        fn type_name(&self) -> Cow<str> {
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
        fn type_name(&self) -> Cow<str> {
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
        fn type_name(&self) -> Cow<str> {
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
