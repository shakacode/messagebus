#![feature(type_alias_impl_trait)]

use std::{alloc::Layout, any::Any, sync::Arc};

use futures::Future;
use messagebus::{
    bus::{Bus, MaskMatch},
    cell::MsgCell,
    error::Error,
    handler::Handler,
    message::{Message, SharedMessage},
    receiver::IntoAbstractReceiver,
    receivers::wrapper::HandlerWrapper,
    type_tag::{TypeTag, TypeTagInfo},
};

#[derive(Debug, Clone)]
struct Msg(pub u32);

impl Message for Msg {
    fn TYPE_TAG() -> TypeTag
    where
        Self: Sized,
    {
        TypeTagInfo::parse("demo::Msg").unwrap().into()
    }

    fn type_tag(&self) -> TypeTag {
        Msg::TYPE_TAG()
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

    fn try_clone_into(&self, into: &mut dyn Message) -> bool {
        false
    }

    fn try_clone_boxed(&self) -> Option<Box<dyn Message>> {
        None
    }

    fn try_clone(&self) -> Option<Self>
    where
        Self: Sized,
    {
        Some(Self(self.0))
    }
}

struct Test {
    inner: u32,
}

impl Handler<Msg> for Test {
    type Response = Msg;
    type HandleFuture<'a> = impl Future<Output = Result<Self::Response, Error>> + 'a;
    type FlushFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;

    fn handle(&self, msg: &mut MsgCell<Msg>, _bus: &Bus) -> Self::HandleFuture<'_> {
        let msg = msg.take().unwrap();

        async move {
            println!("msg {msg:?}");
            let x = self.inner;
            Ok(Msg(x + msg.0))
        }
    }

    fn flush(&mut self, _bus: &Bus) -> Self::FlushFuture<'_> {
        async move { Ok(()) }
    }
}

async fn run() -> Result<(), Error> {
    let bus = Bus::new();

    let wrapper = HandlerWrapper::new(Arc::new(Test { inner: 12 }));
    bus.register(wrapper, MaskMatch::all());

    let res: Msg = bus.request(Msg(13)).await.unwrap();
    println!("request result got {:?}", res);

    bus.send(Msg(12)).await?;

    bus.close().await;
    bus.wait().await;

    Ok(())
}

#[tokio::main]
async fn main() {
    run().await.unwrap();
}
