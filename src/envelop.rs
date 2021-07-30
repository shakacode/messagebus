use core::any::Any;
use core::fmt;
use std::any::type_name;
use std::borrow::Cow;
use std::sync::Arc;

pub trait MessageBounds: fmt::Debug + Unpin + Send + Sync + 'static {}
impl<T: fmt::Debug + Unpin + Send + Sync + 'static> MessageBounds for T {}

pub type TypeTag = Cow<'static, str>;

pub trait TypeTagged {
    fn type_tag_() -> TypeTag
    where
        Self: Sized;

    fn type_tag(&self) -> TypeTag;
    fn type_name(&self) -> Cow<str>;
}

pub trait Message: TypeTagged + MessageBounds {
    fn as_any_ref(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any>;
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any>;

    fn as_shared_ref(&self) -> Option<&dyn SharedMessage>;
    fn as_shared_mut(&mut self) -> Option<&mut dyn SharedMessage>;
    fn as_shared_boxed(self: Box<Self>) -> Option<Box<dyn SharedMessage>>;
    fn as_shared_arc(self: Arc<Self>) -> Option<Arc<dyn SharedMessage>>;

    fn try_clone_into(&self, into: &mut dyn Any) -> bool;
    fn try_clone_boxed(&self) -> Option<Box<dyn Message>>;
}

impl TypeTagged for () {
    fn type_tag_() -> TypeTag {
        type_name::<Self>().into()
    }
    fn type_tag(&self) -> TypeTag {
        type_name::<Self>().into()
    }
    fn type_name(&self) -> Cow<str> {
        type_name::<Self>().into()
    }
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
    fn as_shared_boxed(self: Box<Self>) -> Option<Box<dyn SharedMessage>> {
        Some(self)
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

        into.replace(self.clone());
        true
    }
    fn try_clone_boxed(&self) -> Option<Box<dyn Message>> {
        Some(Box::new(self.clone()))
    }
}

// impl<T: Message> Message for Arc<T> {
//     fn as_any_ref(&self) -> &dyn Any {
//         T::as_any_ref(&*self)
//     }
//     fn as_any_mut(&mut self) -> &mut dyn Any {
//         unimplemented!()
//     }
//     fn as_any_boxed(self: Box<Self>) -> Box<dyn Any> {
//         unimplemented!()
//     }
//     fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any> {
//         self
//     }

//     fn as_shared_ref(&self) -> Option<&dyn SharedMessage> {
//         Some(self)
//     }
//     fn as_shared_mut(&mut self) -> Option<&mut dyn SharedMessage> {
//         Some(self)
//     }
//     fn as_shared_boxed(self: Box<Self>) -> Option<Box<dyn SharedMessage>> {
//         Some(self)
//     }
//     fn as_shared_arc(self: Arc<Self>) -> Option<Arc<dyn SharedMessage>> {
//         Some(self)
//     }
//     fn try_clone_into(&self, into: &mut dyn Any) -> bool {
//         let into = if let Some(inner) = into.downcast_mut::<Option<()>>() {
//             inner
//         } else {
//             return false;
//         };

//         into.replace(self.clone());
//         true
//     }
//     fn try_clone_boxed(&self) -> Option<Box<dyn Message>> {
//         Some(Box::new(self.clone()))
//     }
// }

pub trait IntoBoxedMessage {
    fn into_boxed(self) -> Box<dyn Message>;
}

impl<T: Message> IntoBoxedMessage for T {
    fn into_boxed(self) -> Box<dyn Message> {
        Box::new(self)
    }
}

pub trait SharedMessage: Message + erased_serde::Serialize {}
impl<T: Message + erased_serde::Serialize> SharedMessage for T {}

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
        fn as_shared_boxed(self: Box<Self>) -> Option<Box<dyn SharedMessage>> {
            None
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
        fn as_shared_boxed(self: Box<Self>) -> Option<Box<dyn SharedMessage>> {
            None
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
        fn as_shared_boxed(self: Box<Self>) -> Option<Box<dyn SharedMessage>> {
            Some(self)
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
