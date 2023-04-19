use std::{
    sync::Arc,
    task::{Context, Poll},
};

use futures::{ready, Future};
use parking_lot::Mutex;
use sharded_slab::{Clear, Pool};

use crate::{
    bus::Bus,
    cell::{MsgCell, ResultCell},
    error::{Error, ErrorKind},
    handler::MessageProducer,
    message::Message,
    receiver::Receiver,
    utils::{future_cell::FutureCell, wakelist::WakeList, NullContext},
    TaskHandler,
};

pub trait IntoAsyncProducer<M: Message, R: Message>
where
    Self: crate::MessageProducer<M, Message = R> + Send + Sync + 'static,
{
    fn into_async_producer(self) -> ProducerWrapper<M, Self>
    where
        Self: Sized + 'static;
}

impl<M: Message, H: MessageProducer<M> + Send + Sync + 'static> IntoAsyncProducer<M, H::Message>
    for H
{
    fn into_async_producer(self) -> ProducerWrapper<M, H>
    where
        Self: Sized + 'static,
    {
        ProducerWrapper::new(Arc::new(self))
    }
}

struct ProducerContextInner<M: Message, T: MessageProducer<M> + 'static> {
    context: Option<T::Context>,
    next_fut: FutureCell<T::NextFuture<'static>>,
    close_fut: FutureCell<T::CloseFuture<'static>>,
}

impl<M: Message, T: MessageProducer<M>> Default for ProducerContextInner<M, T> {
    fn default() -> Self {
        Self {
            context: None,
            next_fut: Default::default(),
            close_fut: Default::default(),
        }
    }
}

impl<M: Message, T: MessageProducer<M>> Clear for ProducerContextInner<M, T> {
    fn clear(&mut self) {
        self.context = None;
        self.next_fut.clear();
        self.close_fut.clear();
    }
}

struct ProducerContext<M: Message, T: MessageProducer<M> + 'static> {
    inner: Mutex<ProducerContextInner<M, T>>,
}

impl<M: Message, T: MessageProducer<M> + 'static> ProducerContext<M, T> {
    #[inline]
    pub fn lock(&self) -> parking_lot::MutexGuard<'_, ProducerContextInner<M, T>> {
        self.inner.lock()
    }

    fn init(&mut self, ctx: T::Context) {
        self.inner.get_mut().start(ctx)
    }
}

impl<M: Message, T: MessageProducer<M>> Clear for ProducerContext<M, T> {
    fn clear(&mut self) {
        self.inner.get_mut().clear();
    }
}

impl<M: Message, T: MessageProducer<M>> Default for ProducerContext<M, T> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Default::default()),
        }
    }
}

impl<M: Message, T: MessageProducer<M>> ProducerContextInner<M, T> {
    #[inline]
    pub fn start(&mut self, ctx: T::Context) {
        drop(self.context.replace(ctx));
        self.next_fut.clear();
        self.close_fut.clear();
    }

    #[inline]
    pub fn start_next(&mut self, producer: &T, bus: &Bus) {
        if let Some(ctx) = &mut self.context {
            // SAFETY:
            // the lifetime of that future start from poll_send and should end in poll_result,
            // when it will be ready. To be safe we need drop that future right after it will
            // be completed in poll_result
            self.next_fut
                .set(unsafe { std::mem::transmute(producer.next(ctx, bus)) })
        }
    }

    #[inline]
    pub fn start_close(&mut self, producer: &T) {
        self.next_fut.clear();

        if let Some(ctx) = self.context.take() {
            // SAFETY:
            // the lifetime of that future start from poll_send and should end in poll_result,
            // when it will be ready. To be safe we need drop that future right after it will
            // be completed in poll_result
            self.close_fut
                .set(unsafe { std::mem::transmute(producer.close(ctx)) })
        }
    }
}

pub struct ProducerWrapper<M: Message, T: MessageProducer<M> + 'static> {
    inner: Arc<T>,
    start_fut: Mutex<FutureCell<T::StartFuture<'static>>>,
    producers: Arc<Pool<ProducerContext<M, T>>>,
    send_wakelist: WakeList,
}

impl<M: Message, T: MessageProducer<M>> Clone for ProducerWrapper<M, T> {
    fn clone(&self) -> Self {
        ProducerWrapper {
            inner: self.inner.clone(),
            start_fut: Mutex::new(FutureCell::new()),
            producers: Arc::new(Pool::new()),
            send_wakelist: WakeList::new(),
        }
    }
}

impl<M: Message, T: MessageProducer<M>> ProducerWrapper<M, T> {
    pub fn new(inner: Arc<T>) -> Self {
        Self {
            inner,
            start_fut: Mutex::new(FutureCell::new()),
            producers: Arc::new(Pool::new()),
            send_wakelist: WakeList::new(),
        }
    }

    fn start_start(
        &self,
        handle: &mut FutureCell<T::StartFuture<'static>>,
        msg: &mut MsgCell<M>,
        bus: &Bus,
    ) {
        // SAFETY:
        // the lifetime of that future start from poll_send and should end in poll_result,
        // when it will be ready. To be safe we need drop that future right after it will
        // be completed in poll_result
        handle.set(unsafe { std::mem::transmute(self.inner.start(msg, bus)) });
    }
}

impl<M: Message, T: MessageProducer<M> + 'static> Receiver<M, T::Message>
    for ProducerWrapper<M, T>
{
    type InitFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;
    type CloseFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;
    type FlushFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;

    #[inline]
    fn close(&self) -> Self::CloseFuture<'_> {
        async move { Ok(()) }
    }

    #[inline]
    fn flush(&self, _bus: &Bus) -> Self::FlushFuture<'_> {
        async move { Ok(()) }
    }

    #[inline]
    fn init(&self, _bus: &Bus) -> Self::InitFuture<'_> {
        async move { Ok(()) }
    }

    fn poll_send(
        &self,
        msg: &mut MsgCell<M>,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>> {
        let guard = self.start_fut.try_lock();
        let Some(mut guard) = guard else {
            if let Some(cx) = cx {
                self.send_wakelist.register(cx.waker()) ;
            }

            return Poll::Pending;
        };

        if !guard.is_set() {
            self.start_start(&mut *guard, msg, bus);
        }

        let ctx = ready!(if let Some(cx) = cx {
            guard.poll_unpin(cx)
        } else {
            guard.poll_unpin(&mut NullContext::new())
        })?;

        drop(guard);

        let index = self
            .producers
            .create_with(|producer| producer.init(ctx))
            .unwrap();

        let task = TaskHandler::new(self.producers.clone(), index as _, |data, idx| {
            let Ok(res) = data.downcast::<Pool<ProducerContext<M, T>>>() else {
                println!("wrong type");
                return;
            };

            res.clear(idx as _);
        });

        Poll::Ready(Ok(task))
    }

    fn poll_result(
        &self,
        task: &mut TaskHandler,
        resp: &mut ResultCell<T::Message>,
        cx: &mut Context<'_>,
        bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        let Some(task_handle) = task.data().downcast_ref::<Pool<ProducerContext<M, T>>>() else {
            return Poll::Ready(Err(ErrorKind::ErrorPollWrongTask(String::from("cannot cast type")).into()));
        };

        if !std::ptr::eq(&*self.producers, task_handle) {
            return Poll::Ready(Err(ErrorKind::ErrorPollWrongTask(String::from(
                "pointers are mismatch",
            ))
            .into()));
        }

        loop {
            let Some(producer) = self.producers.get(task.index() as _) else {
                break Poll::Ready(Err(ErrorKind::ErrorPollWrongTask(format!("task index is incorrect {}", task.index())).into()));
            };

            let mut lock = producer.lock();
            if lock.next_fut.is_set() {
                resp.put(ready!(lock.next_fut.poll_unpin(cx)));

                break Poll::Ready(Ok(()));
            } else {
                lock.start_next(&self.inner, bus)
            }
        }
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
        bus::Bus, cell::MsgCell, derive, error::Error, handler::Handler,
        receiver::IntoAbstractReceiver, receivers::wrapper::HandlerWrapper,
    };

    use crate as messagebus;

    #[derive(Debug, Clone, PartialEq, derive::Message)]
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

        fn init(&self, _: &Bus) -> Self::FlushFuture<'_> {
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

        fn init(&self, _: &Bus) -> Self::FlushFuture<'_> {
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
