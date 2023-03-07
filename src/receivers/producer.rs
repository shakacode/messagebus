use std::{
    any::TypeId,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use futures::{ready, task::AtomicWaker, Future};
use parking_lot::Mutex;

use crate::{
    bus::{Bus, TaskHandler, TaskHandlerVTable},
    cell::{MsgCell, ResultCell},
    error::Error,
    handler::MessageProducer,
    message::Message,
    receiver::Receiver,
};

type SendFuture<M: Message, T: MessageProducer<M> + 'static> =
    impl Future<Output = Result<(), Error>> + Send;

pub struct ProducerWrapper<M: Message, T: MessageProducer<M> + 'static> {
    inner: Arc<T>,
    start_fut: Arc<Mutex<Option<T::StartFuture<'static>>>>,
    next_fut: Arc<Mutex<Option<T::NextFuture<'static>>>>,
    send_fut: Mutex<Pin<Box<Option<SendFuture<M, T>>>>>,
    send_waker: AtomicWaker,
}

impl<M: Message, T: MessageProducer<M>> Clone for ProducerWrapper<M, T> {
    fn clone(&self) -> Self {
        ProducerWrapper {
            inner: self.inner.clone(),
            start_fut: Arc::new(Mutex::new(None)),
            next_fut: Arc::new(Mutex::new(None)),
            send_fut: Mutex::new(Box::pin(None)),
            send_waker: AtomicWaker::new(),
        }
    }
}

impl<M: Message, T: MessageProducer<M>> ProducerWrapper<M, T> {
    pub fn new(inner: Arc<T>) -> Self {
        Self {
            inner,
            start_fut: Arc::new(Mutex::new(None)),
            next_fut: Arc::new(Mutex::new(None)),
            send_fut: Mutex::new(Box::pin(None)),
            send_waker: AtomicWaker::new(),
        }
    }

    fn start_start(
        &self,
        handle: &mut Option<T::StartFuture<'_>>,
        msg: &mut MsgCell<M>,
        bus: &Bus,
    ) {
        drop(std::mem::replace(
            handle,
            // SAFETY:
            // the lifetime of that future start from poll_send and should end in poll_result,
            // when it will be ready. To be safe we need drop that future right after it will
            // be completed in poll_result
            Some(unsafe { std::mem::transmute(self.inner.start(msg, bus)) }),
        ));
    }

    fn start_next(&self, handle: &mut Option<T::NextFuture<'_>>, bus: &Bus) {
        drop(std::mem::replace(
            handle,
            // SAFETY:
            // the lifetime of that future start from poll_send and should end in poll_result,
            // when it will be ready. To be safe we need drop that future right after it will
            // be completed in poll_result
            Some(unsafe { std::mem::transmute(self.inner.next(bus)) }),
        ));
    }
}

impl<M: Message, T: MessageProducer<M> + 'static> Receiver<M, T::Message>
    for ProducerWrapper<M, T>
{
    fn poll_send(
        &self,
        msg: &mut MsgCell<M>,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>> {
        let mut guard = self.start_fut.lock();

        if guard.is_none() {
            // SAFETY: Box contains none we've checked it!
            self.start_start(&mut *guard, msg, bus);
        }

        let Some(fut) = &mut *guard else {
            return Poll::Pending;
        };

        let pinned_fut = unsafe { Pin::new_unchecked(fut) };
        let _ = ready!(if let Some(cx) = cx {
            pinned_fut.poll(cx)
        } else {
            let mut null_cx = NullContext::new();
            pinned_fut.poll(&mut null_cx)
        })?;

        let task = TaskHandler::new(
            HandlerWrapperHelper::<M, T>::VTABLE,
            self.next_fut.clone(),
            0,
        );

        Poll::Ready(Ok(task))
    }

    fn poll_result(
        &self,
        task: &mut TaskHandler,
        resp: Option<&mut ResultCell<T::Message>>,
        cx: &mut Context<'_>,
        bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        let Some(task_handle) = task.data().downcast_ref::<Mutex<Option<T::NextFuture<'static>>>>() else {
            println!("cannot cast type");
            return Poll::Ready(Err(Error::ErrorPollWrongTask(String::new())));
        };

        if !std::ptr::eq(&*self.next_fut, task_handle) {
            println!("pointers are mismatch");
            return Poll::Ready(Err(Error::ErrorPollWrongTask(String::new())));
        }

        loop {
            {
                let mut lock = self.send_fut.lock();
                let mb_fut = lock.as_mut();
                if let Some(fut) = mb_fut.as_pin_mut() {
                    ready!(fut.poll(cx)).unwrap();
                }
                drop(unsafe { lock.as_mut().get_unchecked_mut() }.take());
            }

            let mut lock = self.next_fut.lock();
            if let Some(fut) = &mut *lock {
                // SAFETY: in box, safly can poll it
                let res = ready!(unsafe { Pin::new_unchecked(fut) }.poll(cx));

                drop(lock.take());
                drop(lock);

                self.send_waker.wake();

                let res = if let Some(resp_cell) = resp {
                    resp_cell.put(res);
                    Ok(())
                } else if TypeId::of::<T::Message>() != TypeId::of::<()>() {
                    match res {
                        Ok(msg) => {
                            let mut lock = self.send_fut.lock();
                            let bus = bus.clone();
                            drop(
                                unsafe { (&mut *lock).as_mut().get_unchecked_mut() }
                                    .replace(async move { bus.send(msg).await }),
                            );
                            Ok(())
                        }
                        Err(err) => {
                            task.finish();
                            match err {
                                Error::ProducerFinished => Err(Error::ProducerFinished),
                                err => {
                                    println!(
                                        "[{}]: unhandled error result message of type `{}`: {}",
                                        std::any::type_name::<T>(),
                                        std::any::type_name::<T::Message>(),
                                        err
                                    );
                                    Ok(())
                                }
                            }
                        }
                    }
                } else {
                    Ok(())
                };

                return Poll::Ready(res);
            } else {
                self.start_next(&mut *lock, bus);
            }
        }
    }

    fn poll_flush(&self, cx: &mut Context<'_>, bus: &Bus) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

lazy_static::lazy_static! {
    static ref WAKER: Waker = unsafe {
        Waker::from_raw(RawWaker::new(std::ptr::null(), NullContext::VTABLE))
    };
}

pub struct NullContext;
impl NullContext {
    const VTABLE: &RawWakerVTable =
        &RawWakerVTable::new(Self::clone_stub, Self::stub, Self::stub, Self::stub);

    pub fn clone_stub(data: *const ()) -> RawWaker {
        RawWaker::new(data, Self::VTABLE)
    }

    pub fn stub(_: *const ()) {}
    pub fn new() -> Context<'static> {
        Context::from_waker(&WAKER)
    }
}

pub(crate) struct HandlerWrapperHelper<M: Message, T: MessageProducer<M>>(PhantomData<(M, T)>);
impl<M: Message, T: MessageProducer<M> + 'static> HandlerWrapperHelper<M, T> {
    pub const VTABLE: &TaskHandlerVTable = &TaskHandlerVTable {
        drop: |data, _| {
            let Ok(res) = data.downcast::<Mutex<Option<T::NextFuture<'static>>>>() else {
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
        cell::{MessageCell, MsgCell},
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

        fn try_clone_into(&self, _into: &mut dyn MessageCell) -> bool {
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

        fn is_cloneable(&self) -> bool {
            false
        }
    }

    struct Test {
        inner: u32,
    }

    impl Handler<Msg> for Test {
        type Response = Msg;
        type HandleFuture<'a> = impl Future<Output = Result<Self::Response, Error>> + 'a;
        type FlushFuture<'a> = std::future::Ready<Result<(), Error>>;
        type CloseFuture<'a> = std::future::Ready<Result<(), Error>>;

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

        fn close(&mut self) -> Self::CloseFuture<'_> {
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
        type CloseFuture<'a> = std::future::Ready<Result<(), Error>>;

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

        fn close(&mut self) -> Self::CloseFuture<'_> {
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
