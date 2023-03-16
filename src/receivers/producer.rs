use std::{
    marker::PhantomData,
    sync::Arc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use futures::ready;
use parking_lot::Mutex;
use sharded_slab::{Clear, Pool};

use crate::{
    bus::Bus,
    cell::{MsgCell, ResultCell},
    error::Error,
    handler::MessageProducer,
    message::Message,
    receiver::Receiver,
    utils::{future_cell::FutureCell, wakelist::WakeList},
    TaskHandler, TaskHandlerVTable,
};

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
        self.next_fut.unset();
        self.close_fut.unset();
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
        self.next_fut.unset();
        self.close_fut.unset();
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
        self.next_fut.unset();

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

        let task = TaskHandler::new(
            HandlerWrapperHelper::<M, T>::VTABLE,
            self.producers.clone(),
            index as _,
        );

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
            return Poll::Ready(Err(Error::ErrorPollWrongTask(String::from("cannot cast type"))));
        };

        if !std::ptr::eq(&*self.producers, task_handle) {
            return Poll::Ready(Err(Error::ErrorPollWrongTask(String::from(
                "pointers are mismatch",
            ))));
        }

        loop {
            let Some(producer) = self.producers.get(task.index() as _) else {
                break Poll::Ready(Err(Error::ErrorPollWrongTask(format!("task index is incorrect {}", task.index()))));
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

    fn poll_flush(&self, _cx: &mut Context<'_>, _bus: &Bus) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(&self, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
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
        drop: |data, idx| {
            let Ok(res) = data.downcast::<Pool<ProducerContext<M, T>>>() else {
                println!("wrong type");
                return;
            };

            res.clear(idx as _);
        },
    };
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
        bus::Bus, cell::MsgCell, derive_message_clone, error::Error, handler::Handler,
        receiver::IntoAbstractReceiver, receivers::wrapper::HandlerWrapper,
    };

    #[derive(Debug, Clone, PartialEq)]
    struct Msg(pub u32);
    derive_message_clone!(TEST_PRODUCER_MSG, Msg, "test::Msg");

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
