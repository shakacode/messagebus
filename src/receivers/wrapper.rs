use std::{
    any::TypeId,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{ready, task::AtomicWaker, Future};
use parking_lot::Mutex;

use crate::{
    bus::{Bus, TaskHandler, TaskHandlerVTable},
    cell::{MsgCell, ResultCell},
    error::Error,
    handler::Handler,
    message::Message,
    receiver::Receiver,
};

pub struct HandlerWrapper<M: Message, T: Handler<M> + 'static> {
    inner: Arc<T>,
    current_fut: Arc<Mutex<Option<T::HandleFuture<'static>>>>,
    send_waker: AtomicWaker,
}

impl<M: Message, T: Handler<M>> Clone for HandlerWrapper<M, T> {
    fn clone(&self) -> Self {
        HandlerWrapper {
            inner: self.inner.clone(),
            current_fut: Arc::new(Mutex::new(None)),
            send_waker: AtomicWaker::new(),
        }
    }
}

impl<M: Message, T: Handler<M>> HandlerWrapper<M, T> {
    pub fn new(inner: Arc<T>) -> Self {
        Self {
            inner,
            current_fut: Arc::new(Mutex::new(None)),
            send_waker: AtomicWaker::new(),
        }
    }

    fn start_handle(
        &self,
        handle: &mut Option<T::HandleFuture<'_>>,
        msg: &mut MsgCell<M>,
        bus: &Bus,
    ) {
        drop(std::mem::replace(
            handle,
            // SAFETY:
            // the lifetime of that future start from poll_send and should end in poll_result,
            // when it will be ready. To be safe we need drop that future right after it will
            // be completed in poll_result
            Some(unsafe { std::mem::transmute(self.inner.handle(msg, bus)) }),
        ));
    }
}

impl<M: Message, T: Handler<M> + 'static> Receiver<M, T::Response> for HandlerWrapper<M, T> {
    fn poll_send(
        &self,
        msg: &mut MsgCell<M>,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>> {
        if let Some(mut guard) = self.current_fut.try_lock() {
            if guard.is_none() {
                let task = TaskHandler::new(
                    HandlerWrapperHelper::<M, T>::VTABLE,
                    self.current_fut.clone(),
                    0,
                );

                // SAFETY: Box contains none we've checked it!
                self.start_handle(&mut *guard, msg, bus);

                return Poll::Ready(Ok(task));
            }
        }

        if let Some(cx) = cx {
            self.send_waker.register(cx.waker());
        }

        Poll::Pending
    }

    fn poll_result(
        &self,
        task: &mut TaskHandler,
        resp: Option<&mut ResultCell<T::Response>>,
        cx: &mut Context<'_>,
        _bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        let Some(task_handle) = task.data().downcast_ref::<Mutex<Option<T::HandleFuture<'static>>>>() else {
            println!("cannot cast type");
            return Poll::Ready(Err(Error::ErrorPollWrongTask(String::new())));
        };

        if !std::ptr::eq(&*self.current_fut, task_handle) {
            println!("pointers are mismatch");
            return Poll::Ready(Err(Error::ErrorPollWrongTask(String::new())));
        }

        let mut lock = self.current_fut.lock();
        if let Some(fut) = &mut *lock {
            // SAFETY: in box, safly can poll it
            let res = ready!(unsafe { Pin::new_unchecked(fut) }.poll(cx));

            drop(lock.take());
            drop(lock);

            self.send_waker.wake();

            if let Some(resp_cell) = resp {
                resp_cell.put(res);
            } else if TypeId::of::<T::Response>() != TypeId::of::<()>() {
                println!(
                    "[{}]: unhandled result message of type `{}`",
                    std::any::type_name::<T>(),
                    std::any::type_name::<T::Response>()
                );
            }

            return Poll::Ready(Ok(()));
        }

        Poll::Pending
    }
}

pub(crate) struct HandlerWrapperHelper<M: Message, T: Handler<M>>(PhantomData<(M, T)>);
impl<M: Message, T: Handler<M> + 'static> HandlerWrapperHelper<M, T> {
    pub const VTABLE: &TaskHandlerVTable = &TaskHandlerVTable {
        drop: |data, _| {
            let Ok(res) = data.downcast::<Mutex<Option<T::HandleFuture<'static>>>>() else {
                println!("wrong type");
                return;
            };

            drop(res.lock().take());
        },
    };
}

#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        future::{poll_fn, Future},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        task::{Context, Poll},
    };

    use crate::{
        bus::Bus,
        cell::MsgCell,
        error::Error,
        handler::Handler,
        message::{Message, SharedMessage},
        receiver::IntoAbstractReceiver,
        receivers::wrapper::HandlerWrapper,
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

    impl Handler<Msg> for Test {
        type Response = Msg;
        type HandleFuture<'a> = impl Future<Output = Result<Self::Response, Error>> + 'a;
        type FlushFuture<'a> = std::future::Ready<Result<(), Error>>;

        fn handle(&self, msg: &mut MsgCell<Msg>, _: &Bus) -> Self::HandleFuture<'_> {
            let val = msg.peek().0;

            async move {
                let x = self.inner;
                Ok(Msg(x + val))
            }
        }

        fn flush(&mut self, _: &Bus) -> Self::FlushFuture<'_> {
            std::future::ready(Ok(()))
        }
    }

    struct SleepTest {
        inner: u32,
    }

    impl Handler<Msg> for SleepTest {
        type Response = Msg;
        type HandleFuture<'a> = impl Future<Output = Result<Self::Response, Error>> + 'a;
        type FlushFuture<'a> = std::future::Ready<Result<(), Error>>;

        fn handle(&self, msg: &mut MsgCell<Msg>, _: &Bus) -> Self::HandleFuture<'_> {
            let val = msg.peek().0;
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                let x = self.inner;
                Ok(Msg(x + val))
            }
        }
        fn flush(&mut self, _: &Bus) -> Self::FlushFuture<'_> {
            std::future::ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_default() -> Result<(), Error> {
        let bus = Bus::new();
        let wrapper = HandlerWrapper::new(Arc::new(Test { inner: 12 }));
        let receiver = wrapper.into_abstract_arc();

        let mut cell = MsgCell::new(Msg(12));
        let task = poll_fn(|cx| receiver.poll_send(&mut cell, Some(cx), &bus)).await?;
        let r: Msg = receiver.result(task, bus).await?;

        assert_eq!(r, Msg(24));

        Ok(())
    }

    #[tokio::test]
    async fn test_request() -> Result<(), Error> {
        let bus = Bus::new();
        let wrapper = HandlerWrapper::new(Arc::new(Test { inner: 12 }));
        let receiver = wrapper.into_abstract_arc();
        assert_eq!(receiver.request::<_, Msg>(Msg(13), bus).await?, Msg(25));
        Ok(())
    }

    #[tokio::test]
    async fn test_send_wake() -> Result<(), Error> {
        let bus = Bus::new();
        let wrapper = HandlerWrapper::new(Arc::new(Test { inner: 12 }));
        let receiver = wrapper.into_abstract_arc();

        let flag1 = Arc::new(AtomicBool::new(false));
        let flag1_clone = flag1.clone();

        let waker = waker_fn::waker_fn(move || {
            flag1_clone.store(true, Ordering::SeqCst);
        });
        let mut cx = Context::from_waker(&waker);

        let t1 = receiver.try_send(Msg(12), &bus).unwrap();

        let mut cell = MsgCell::new(Msg(13));
        assert!(matches!(
            receiver.poll_send(&mut cell, Some(&mut cx), &bus),
            Poll::Pending
        ));

        let res1: Msg = receiver.result(t1, bus).await.unwrap();
        assert_eq!(res1, Msg(24));

        assert!(flag1.load(Ordering::SeqCst));

        Ok(())
    }

    #[tokio::test]
    async fn test_slow_receiver_fast_producer() -> Result<(), Error> {
        let bus = Bus::new();
        let wrapper = HandlerWrapper::new(Arc::new(SleepTest { inner: 12 }));
        let receiver = wrapper.into_abstract_arc();
        let src = (0u32..128).map(Msg).collect::<Vec<_>>();
        let dst = (0u32..128).map(|x| x + 12).map(Msg).collect::<Vec<_>>();
        let mut target = Vec::<Msg>::new();

        for i in src {
            target.push(receiver.request(i, bus.clone()).await?);
        }

        assert_eq!(target, dst);

        Ok(())
    }
}
