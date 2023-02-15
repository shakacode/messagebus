use std::{
    marker::PhantomData,
    task::{Context, Poll},
};

use crate::{
    bus::TaskHandler,
    cell::{MsgCell, ResultCell},
    error::Error,
    message::Message,
    receiver::Receiver,
};

// type Fut<'a, M: Message, R: Message, T: Receiver<'a, M, R> + Clone + 'static> =
//     impl std::future::Future<Output = Result<R, Error>> + 'a;

pub struct Unordered<'a, M: Message, R: Message, T: Receiver<'a, M, R> + Clone + 'static> {
    inner: T,
    _m: PhantomData<&'a (M, R, T)>,
    // pool: RwLock<Pin<Box<FuturesUnordered<Fut<'a, M, R, T>>>>>,
}

impl<'a, M: Message, R: Message, T: Receiver<'a, M, R> + Clone + 'static> Unordered<'a, M, R, T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            _m: Default::default(),
            // pool: RwLock::new(Box::pin(FuturesUnordered::new())),
        }
    }
}

impl<'a, M: Message, R: Message, T: Receiver<'a, M, R> + Clone + 'static> Receiver<'a, M, R>
    for Unordered<'a, M, R, T>
{
    fn poll_send(
        &self,
        msg: &mut MsgCell<M>,
        cx: Option<&mut Context<'_>>,
    ) -> Poll<Result<TaskHandler, Error>> {
        let msg = msg.take()?;
        let tt = self.inner.clone();
        // self.request(msg)
        // self.pool.read().push(async move { Ok(todo!()) });

        Poll::Ready(Ok(0))
    }

    fn poll_result(
        &self,
        task: &TaskHandler,
        resp: Option<&mut ResultCell<R>>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        // let res = ready!(self.pool.write().as_mut().poll_next(cx)).unwrap();

        // if let Some(resp) = resp {
        // resp.put(res);
        // }

        Poll::Ready(Ok(()))
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
        receiver::IntoAbstractReceiver,
        receivers::wrapper::HandlerWrapper,
        type_tag::{TypeTag, TypeTagInfo},
    };

    use super::Unordered;

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
    async fn test_unordered_exec() -> Result<(), Error> {
        let wrapper = HandlerWrapper::new(Arc::new(Test { inner: 100 }));
        let unordered = Unordered::new(wrapper);
        let receiver = unordered.into_abstract_arc();
        let mut dest = Vec::new();

        for i in 0..1000 {
            let mut cell = MsgCell::new(Msg(i));
            receiver.send(&mut cell).await?;
        }

        for _ in 0..1000 {
            let r: Msg = receiver.result(0).await?;
            dest.push(r.0);
        }

        dest.sort();

        assert_eq!(dest, (12..1012).collect::<Vec<_>>());

        Ok(())
    }
}
