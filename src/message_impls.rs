use crate::{Message, TypeTag, TypeTagInfo, TypeTagged};
use lazy_static::lazy_static;

lazy_static! {
    static ref DERIVE_VOID: TypeTag = TypeTagInfo::parse("void").unwrap().into();
}

impl TypeTagged for () {
    fn TYPE_TAG() -> crate::TypeTag
    where
        Self: Sized,
    {
        DERIVE_VOID.clone()
    }

    fn type_tag(&self) -> crate::TypeTag {
        Self::TYPE_TAG()
    }

    fn type_layout(&self) -> std::alloc::Layout {
        std::alloc::Layout::for_value(self)
    }
}

impl Message for () {
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

    fn as_shared_ref(&self) -> Option<&dyn crate::SharedMessage> {
        Some(self)
    }

    fn as_shared_mut(&mut self) -> Option<&mut dyn crate::SharedMessage> {
        Some(self)
    }

    fn as_shared_boxed(self: Box<Self>) -> Result<Box<dyn crate::SharedMessage>, Box<dyn Message>> {
        Ok(self)
    }

    fn as_shared_arc(
        self: std::sync::Arc<Self>,
    ) -> Option<std::sync::Arc<dyn crate::SharedMessage>> {
        Some(self)
    }

    fn try_clone_into(&self, into: &mut dyn crate::cell::MessageCell) -> bool {
        into.into_typed::<Self>()
            .map(|c| c.put(self.clone()))
            .is_ok()
    }

    fn try_clone_boxed(&self) -> Option<Box<dyn Message>> {
        Some(Box::new(self.clone()))
    }

    fn is_cloneable(&self) -> bool {
        true
    }

    fn try_clone(&self) -> Option<Self>
    where
        Self: Sized,
    {
        Some(())
    }
}
