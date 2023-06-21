use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{ready, Future};
use parking_lot::Mutex;

use crate::{
    bus::Bus,
    cell::{MsgCell, ResultCell},
    error::{Error, ErrorKind},
    handler::Handler,
    message::Message,
    receiver::Receiver,
    utils::wakelist::WakeList,
    TaskHandler,
};

pub trait IntoAsyncReceiver<M: Message, R: Message>
where
    Self: Handler<M, Response = R> + Send + Sync + 'static,
{
    fn into_async_receiver(self: Arc<Self>) -> HandlerWrapper<M, Self>
    where
        Self: Sized + 'static;
}

impl<M: Message, R: Message, H: Handler<M, Response = R> + Send + Sync + 'static>
    IntoAsyncReceiver<M, R> for H
{
    fn into_async_receiver(self: Arc<Self>) -> HandlerWrapper<M, H>
    where
        Self: Sized + 'static,
    {
        HandlerWrapper::new(self)
    }
}

pub struct HandlerWrapper<M: Message, T: Handler<M> + 'static> {
    inner: Arc<T>,
    current_fut: Arc<Mutex<Option<T::HandleFuture<'static>>>>,
    send_waker: WakeList,
}

impl<M: Message, T: Handler<M>> Clone for HandlerWrapper<M, T> {
    fn clone(&self) -> Self {
        HandlerWrapper {
            inner: self.inner.clone(),
            current_fut: Arc::new(Mutex::new(None)),
            send_waker: WakeList::new(),
        }
    }
}

impl<M: Message, T: Handler<M>> HandlerWrapper<M, T> {
    pub fn new(inner: Arc<T>) -> Self {
        Self {
            inner,
            current_fut: Arc::new(Mutex::new(None)),
            send_waker: WakeList::new(),
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
    type InitFuture<'a> = T::InitFuture<'a>;
    type CloseFuture<'a> = T::CloseFuture<'a>;
    type FlushFuture<'a> = T::FlushFuture<'a>;

    #[inline]
    fn close(&self) -> Self::CloseFuture<'_> {
        self.inner.close()
    }

    #[inline]
    fn flush(&self, bus: &Bus) -> Self::FlushFuture<'_> {
        self.inner.flush(bus)
    }

    #[inline]
    fn init(&self, bus: &Bus) -> Self::InitFuture<'_> {
        self.inner.init(bus)
    }

    fn poll_send(
        &self,
        msg: &mut MsgCell<M>,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>> {
        if let Some(mut guard) = self.current_fut.try_lock() {
            if guard.is_none() {
                let task = TaskHandler::new(self.current_fut.clone(), 0, |data, _| {
                    let Ok(res) = data.downcast::<Mutex<Option<T::HandleFuture<'static>>>>() else {
                        println!("wrong type");
                        return;
                    };

                    drop(res.lock().take());
                });

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
        resp: &mut ResultCell<T::Response>,
        cx: &mut Context<'_>,
        _bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        let Some(task_handle) = task.data().downcast_ref::<Mutex<Option<T::HandleFuture<'static>>>>() else {
            return Poll::Ready(Err(ErrorKind::ErrorPollWrongTask(String::from("cannot cast type")).into()));
        };

        if !std::ptr::eq(&*self.current_fut, task_handle) {
            return Poll::Ready(Err(ErrorKind::ErrorPollWrongTask(String::from(
                "pointers are mismatch",
            ))
            .into()));
        }

        let mut lock = self.current_fut.lock();
        if let Some(fut) = &mut *lock {
            // SAFETY: in box, safly can poll it
            resp.put(ready!(unsafe { Pin::new_unchecked(fut) }.poll(cx)));

            drop(lock.take());
            drop(lock);

            task.finish();
            self.send_waker.wake_all();

            return Poll::Ready(Ok(()));
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::{poll_fn, Future},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        task::{Context, Poll},
    };

    use crate::{
        bus::Bus, cell::MsgCell, error::Error, handler::Handler, receiver::IntoAbstractReceiver,
        receivers::wrapper::HandlerWrapper,
    };

    use crate as messagebus;
    use messagebus::derive::Message;

    #[derive(Debug, Clone, PartialEq, Message)]
    struct Msg(pub u32);

    struct Test {
        inner: u32,
    }

    impl Handler<Msg> for Test {
        type Response = Msg;
        type HandleFuture<'a> = impl Future<Output = Result<Self::Response, Error>> + 'a;
        type FlushFuture<'a> = std::future::Ready<Result<(), Error>>;
        type CloseFuture<'a> = std::future::Ready<Result<(), Error>>;
        type InitFuture<'a> = std::future::Ready<Result<(), Error>>;

        fn init(&self, _: &Bus) -> Self::InitFuture<'_> {
            std::future::ready(Ok(()))
        }

        fn handle(&self, msg: &mut MsgCell<Msg>, _: &Bus) -> Self::HandleFuture<'_> {
            let val = msg.peek().0;

            async move {
                let x = self.inner;
                Ok(Msg(x + val))
            }
        }

        fn flush(&self, _: &Bus) -> Self::FlushFuture<'_> {
            std::future::ready(Ok(()))
        }

        fn close(&self) -> Self::CloseFuture<'_> {
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
        type InitFuture<'a> = std::future::Ready<Result<(), Error>>;

        fn init(&self, _: &Bus) -> Self::InitFuture<'_> {
            std::future::ready(Ok(()))
        }

        fn handle(&self, msg: &mut MsgCell<Msg>, _: &Bus) -> Self::HandleFuture<'_> {
            let val = msg.peek().0;
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                let x = self.inner;
                Ok(Msg(x + val))
            }
        }
        fn flush(&self, _: &Bus) -> Self::FlushFuture<'_> {
            std::future::ready(Ok(()))
        }

        fn close(&self) -> Self::CloseFuture<'_> {
            std::future::ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_default() -> Result<(), Error> {
        let bus = Bus::new();
        let wrapper = HandlerWrapper::new(Arc::new(Test { inner: 12 }));
        let receiver = wrapper.into_abstract_arc();

        receiver.initialize(&bus).await.unwrap();

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
        receiver.initialize(&bus).await.unwrap();
        assert_eq!(receiver.request::<_, Msg>(Msg(13), bus).await?, Msg(25));
        Ok(())
    }

    #[tokio::test]
    async fn test_send_wake() -> Result<(), Error> {
        let bus = Bus::new();
        let wrapper = HandlerWrapper::new(Arc::new(Test { inner: 12 }));
        let receiver = wrapper.into_abstract_arc();
        receiver.initialize(&bus).await.unwrap();

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
        receiver.initialize(&bus).await.unwrap();

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
