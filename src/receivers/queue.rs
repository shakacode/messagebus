use std::{
    marker::PhantomData,
    task::{Context, Poll, Waker},
};

use crossbeam::queue::{ArrayQueue, SegQueue};

use crate::{
    bus::TaskHandler,
    cell::{MsgCell, ResultCell},
    error::Error,
    message::Message,
    receiver::Receiver,
};

pub struct Queue<'a, M: Message, R: Message, T: Receiver<'a, M, R> + 'static> {
    _inner: T,
    _limit: usize,
    _waker_queue: SegQueue<Waker>,
    _queue: ArrayQueue<MsgCell<M>>,
    _m: PhantomData<&'a R>,
}

impl<'a, M: Message, R: Message, T: Receiver<'a, M, R> + 'static> Queue<'a, M, R, T> {
    pub fn new(_inner: T, _limit: usize) -> Self {
        Self {
            _inner,
            _limit,
            _queue: ArrayQueue::new(_limit),
            _waker_queue: SegQueue::new(),
            _m: Default::default(),
        }
    }
}

impl<'a, M: Message, R: Message, T: Receiver<'a, M, R> + 'static> Receiver<'a, M, R>
    for Queue<'a, M, R, T>
{
    fn poll_send(
        &self,
        _msg: &mut MsgCell<M>,
        _cx: Option<&mut Context<'_>>,
    ) -> Poll<Result<TaskHandler, Error>> {
        Poll::Pending
    }

    fn poll_result(
        &'a self,
        _task: &TaskHandler,
        _resp: Option<&mut ResultCell<R>>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use futures::Future;
    use std::{any::Any, sync::Arc};

    use crate::{
        cell::MsgCell,
        error::Error,
        handler::Handler,
        message::{Message, SharedMessage},
        receiver::{AbstractReceiver, IntoAbstractReceiver},
        receivers::{queue::Queue, wrapper::HandlerWrapper},
        type_tag::{TypeTag, TypeTagInfo},
    };

    #[derive(Debug, Clone, PartialEq)]
    struct Msg(pub u32);

    impl Message for Msg {
        #[allow(non_snake_case)]
        fn TYPE_TAG() -> TypeTag
        where
            Self: Sized,
        {
            TypeTagInfo::parse("demo::Msg").unwrap().into()
        }

        fn type_tag(&self) -> TypeTag {
            Msg::TYPE_TAG()
        }

        fn type_layout(&self) -> std::alloc::Layout {
            std::alloc::Layout::for_value(self)
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

    struct Test {
        inner: u32,
    }

    impl<'a> Handler<'a, Msg> for Test {
        type Response = Msg;
        type HandleFuture = impl Future<Output = Result<Self::Response, Error>> + 'a;

        fn handle(&'a self, msg: &mut MsgCell<Msg>) -> Self::HandleFuture {
            let msg = msg.peek().0;

            async move {
                tokio::time::sleep(std::time::Duration::from_millis(self.inner as _)).await;

                Ok(Msg(msg + 12))
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_queue_exec() -> Result<(), Error> {
        let wrapper = HandlerWrapper::new(Arc::new(Test { inner: 1 }));
        let unordered = Queue::new(wrapper, 100);
        let receiver: Arc<dyn for<'b> AbstractReceiver<'b>> = unordered.into_abstract_arc();
        let mut dest = Vec::new();

        for i in 0..100 {
            let mut cell = MsgCell::new(Msg(i));
            receiver.send(&mut cell).await?;
        }

        for i in 0..100 {
            println!("I {}", i);

            let r: Msg = receiver.result(0).await?;
            println!("pushing result {:?}", r);
            dest.push(r.0);
        }

        assert_eq!(dest, (12..112).collect::<Vec<_>>());

        Ok(())
    }
}
