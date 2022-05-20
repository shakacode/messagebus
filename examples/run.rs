#![feature(type_alias_impl_trait)]
#![feature(generic_associated_types)]

use std::{alloc::Layout, any::Any, sync::Arc};

use futures::Future;
use messagebus_new::{
    error::Error,
    handler::Handler,
    message::{self, Message, SharedMessage},
    receivers::wrapper::HandlerWrapper,
    type_tag::TypeTag,
};

#[derive(Debug, Clone)]
struct Msg {}

impl Message for Msg {
    fn TYPE_TAG() -> TypeTag
    where
        Self: Sized,
    {
        todo!()
    }

    fn type_tag(&self) -> TypeTag {
        todo!()
    }

    fn type_layout(&self) -> Layout {
        Layout::for_value(self)
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any> {
        self as _
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any> {
        self as _
    }

    fn as_shared_ref(&self) -> Option<&dyn SharedMessage> {
        todo!()
    }

    fn as_shared_mut(&mut self) -> Option<&mut dyn SharedMessage> {
        todo!()
    }

    fn as_shared_boxed(self: Box<Self>) -> Result<Box<dyn SharedMessage>, Box<dyn Message>> {
        todo!()
    }

    fn as_shared_arc(self: Arc<Self>) -> Option<Arc<dyn SharedMessage>> {
        todo!()
    }

    fn try_clone_into(&self, into: &mut dyn Message) -> bool {
        todo!()
    }

    fn try_clone_boxed(&self) -> Option<Box<dyn Message>> {
        todo!()
    }

    fn try_clone(&self) -> Option<Self>
    where
        Self: Sized,
    {
        todo!()
    }
}

struct Test {
    inner: u32,
}

impl Handler<Msg> for Test {
    type Response = Msg;
    type HandleFuture<'a> = impl Future<Output = Result<Self::Response, Error>> + 'a;

    fn handle(&self, msg: Msg) -> Self::HandleFuture<'_> {
        async move {
            let _x = self.inner;
            Ok(msg)
        }
    }
}

#[tokio::main]
async fn main() {
    let wrapper = HandlerWrapper::new(Test { inner: 12 });
}
