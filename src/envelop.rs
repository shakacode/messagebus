use core::any::{self, Any};
use core::fmt;
// use erased_serde::{Deserializer, Serialize};

pub trait Message: Any + fmt::Debug/*Serialize + for<'a> Deserializer<'a> + */ + Unpin + Clone + Send + Sync + 'static {}
impl<T: Any + fmt::Debug + Unpin + Clone + Send + Sync> Message for T {}

trait SafeMessage: Any + fmt::Debug/*+ Serialize + for<'a> Deserializer<'a>*/ + Unpin + Send + Sync + 'static {
    fn type_name(&self) -> &'static str;
    fn clone_boxed(&self) -> Box<dyn SafeMessage>;
}

impl<T: Message> SafeMessage for T {
    fn type_name(&self) -> &'static str {
        any::type_name::<T>()
    }

    fn clone_boxed(&self) -> Box<dyn SafeMessage> {
        Box::new(self.clone())
    }
}

// pub struct BoxedEnvelop {
//     inner: Box<dyn SafeMessage>,
// }

// impl BoxedEnvelop {
//     pub fn from_message<M: Message>(m: M) -> Self {
//         Self {
//             inner: Box::new(m)
//         }
//     }

//     pub fn as_ref(&self) -> Envelop<'_> {
//         Envelop { inner: &*self.inner }
//     }

//     pub fn downcast<T: 'static>(self) -> Option<Box<T>> {
//         if (*self.inner).type_id() == TypeId::of::<T>() {
//             unsafe {
//                 let raw: *mut dyn SafeMessage = Box::into_raw(self.inner);

//                 Some(Box::from_raw(raw as *mut T))
//             }
//         } else {
//             None
//         }
//     }
// }

#[derive(Copy, Clone)]
pub struct Envelop<'inner> {
    inner: &'inner dyn SafeMessage,
}

impl<'inner> fmt::Debug for Envelop<'inner> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Envelop(")?;
        self.inner.fmt(f)?;
        write!(f, ")")?;

        Ok(())
    }
}

impl<'inner> Envelop<'inner> {
    // pub fn new<T: Message>(inner: &'inner T) -> Self {
    //     Self { inner }
    // }

    // #[inline]
    // pub fn downcast_to<T: 'static>(&self) -> Option<&T> {
    //     if self.inner.type_id() == TypeId::of::<T>() {
    //         unsafe { Some(&*(self.inner as *const dyn SafeMessage as *const T)) }
    //     } else {
    //         None
    //     }
    // }

    // #[inline]
    // pub fn type_id(&self) -> TypeId {
    //     self.inner.type_id()
    // }

    // #[inline]
    // pub fn type_name(&self) -> &'static str {
    //     self.inner.type_name()
    // }

    // #[inline]
    // pub fn clone_boxed(&self) -> BoxedEnvelop {
    //     BoxedEnvelop {
    //         inner: self.inner.clone_boxed(),
    //     }
    // }
}

// impl<'inner> serde::Serialize for Envelop<'inner> {
//     fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
//         erased_serde::serialize(self.inner, serializer)
//     }
// }
