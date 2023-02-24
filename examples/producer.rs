#![feature(type_alias_impl_trait)]

use std::{
    alloc::Layout,
    any::Any,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::Future;
use messagebus::{
    bus::{Bus, MaskMatch},
    cell::MsgCell,
    error::Error,
    handler::{Handler, MessageProducer},
    message::{Message, SharedMessage},
    receivers::{producer::ProducerWrapper, wrapper::HandlerWrapper},
    type_tag::{TypeTag, TypeTagInfo},
};

#[derive(Debug, Clone)]
struct Msg(pub u64);

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

    fn try_clone_into(&self, _into: &mut dyn Message) -> bool {
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
#[derive(Debug, Clone)]
struct StartMsg;

impl Message for StartMsg {
    fn TYPE_TAG() -> TypeTag
    where
        Self: Sized,
    {
        TypeTagInfo::parse("demo::StartMsg").unwrap().into()
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

    fn try_clone_into(&self, _into: &mut dyn Message) -> bool {
        false
    }

    fn try_clone_boxed(&self) -> Option<Box<dyn Message>> {
        None
    }

    fn try_clone(&self) -> Option<Self>
    where
        Self: Sized,
    {
        Some(Self)
    }
}

struct Test {
    inner: AtomicU64,
}

impl MessageProducer<StartMsg> for Test {
    type Message = Msg;
    type NextFuture<'a> = impl Future<Output = Result<Self::Message, Error>> + 'a;
    type StartFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;

    fn start(&self, _msg: &mut MsgCell<StartMsg>, _: &Bus) -> Self::StartFuture<'_> {
        async move {
            println!("start");

            Ok(())
        }
    }

    fn next(&self, _: &Bus) -> Self::NextFuture<'_> {
        async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let msg = Msg(self.inner.fetch_add(1, Ordering::Relaxed));
            println!("next #{}", msg.0);
            if msg.0 == 25 {
                return Err(Error::ProducerFinished);
            }
            Ok(msg)
        }
    }
}

impl Handler<Msg> for Test {
    type Response = Msg;
    type HandleFuture<'a> = impl Future<Output = Result<Self::Response, Error>> + 'a;
    type FlushFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;

    fn handle(&self, msg: &mut MsgCell<Msg>, bus: &Bus) -> Self::HandleFuture<'_> {
        let msg = msg.take().unwrap();

        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            println!("handing #{}", msg.0);
            Ok(Msg(0))
        }
    }

    fn flush(&mut self, bus: &Bus) -> Self::FlushFuture<'_> {
        async move { Ok(()) }
    }
}

async fn run() -> Result<(), Error> {
    let bus = Bus::new();
    let test = Arc::new(Test {
        inner: AtomicU64::new(0),
    });
    bus.register(ProducerWrapper::new(test.clone()), MaskMatch::all());
    bus.register(HandlerWrapper::new(test), MaskMatch::all());

    bus.start_producer(StartMsg).await?;

    bus.close().await;
    bus.wait().await;

    Ok(())
}

#[tokio::main]
async fn main() {
    run().await.unwrap();
}
