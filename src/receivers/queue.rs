use std::{
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    task::{ready, Context, Poll, Waker},
};

use crossbeam::queue::ArrayQueue;
use futures::{task::AtomicWaker, Future};
use parking_lot::Mutex;

use crate::{
    bus::{Bus, TaskHandler},
    cell::{MsgCell, ResultCell},
    error::Error,
    message::Message,
    receiver::{Receiver, ReceiverEx},
    wakelist::WakeList,
};

struct QueueItem<M: Message> {
    index: usize,
    generation: u64,
    message: MsgCell<M>,
}

impl<M: Message> QueueItem<M> {
    fn new(index: usize, generation: u64, message: MsgCell<M>) -> QueueItem<M> {
        Self {
            index,
            generation,
            message,
        }
    }
}

type SendFuture<M: Message, R: Message, T: Receiver<M, R> + 'static> =
    impl Future<Output = Result<R, Error>> + Send;

pub struct Queue<M: Message, R: Message, T: Receiver<M, R> + 'static> {
    inner: Arc<T>,
    free: ArrayQueue<usize>,
    queue: ArrayQueue<QueueItem<M>>,
    wakers: Arc<[AtomicWaker]>,
    limit: usize,
    wakelist: Mutex<WakeList>,
    current: Mutex<Pin<Box<Option<SendFuture<M, R, T>>>>>,
    generation_sequence: AtomicU64,
}

impl<M: Message, R: Message, T: Receiver<M, R> + 'static> Queue<M, R, T> {
    pub fn new(inner: T, limit: usize) -> Self {
        let free = ArrayQueue::new(limit);
        for i in 0..limit {
            let _ = free.push(i);
        }

        Self {
            limit,
            inner: Arc::new(inner),
            free,
            queue: ArrayQueue::new(limit),
            wakers: (0..limit).map(|_| AtomicWaker::new()).collect(),
            wakelist: Mutex::new(WakeList::new()),
            current: Mutex::new(Box::pin(None)),
            generation_sequence: AtomicU64::new(1),
        }
    }

    #[inline(always)]
    fn next_gen(&self) -> u64 {
        self.generation_sequence.fetch_add(1, Ordering::Relaxed)
    }
}

impl<'a, M: Message, R: Message, T: Receiver<M, R> + 'static> Receiver<M, R> for Queue<M, R, T> {
    fn poll_send(
        &self,
        msg: &mut MsgCell<M>,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>> {
        // trying fast track
        if self.free.is_full() {
            if let Some(mut lock) = self.current.try_lock() {
                if lock.is_none() {
                    let inner = self.inner.clone();
                    let bus = bus.clone();

                    enum Val<M: Message> {
                        Task(TaskHandler),
                        Cell(MsgCell<M>),
                    }

                    let val = if let Ok(task) = inner.try_send(msg, &bus) {
                        Val::Task(task)
                    } else {
                        Val::Cell(msg.take_cell())
                    };

                    drop(
                        unsafe { (&mut *lock).as_mut().get_unchecked_mut() }.replace(async move {
                            match val {
                                Val::Task(task) => inner.result(task, bus).await,
                                Val::Cell(cell) => inner.request(cell, bus).await,
                            }
                        }),
                    );

                    let task = todo!();

                    return Poll::Ready(Ok(task));
                }
            }
        }

        // enqueuing the message
        if let Some(index) = self.free.pop() {
            let generation = self.next_gen();
            assert!(self
                .queue
                .push(QueueItem {
                    index,
                    generation,
                    message: msg.take_cell(),
                })
                .is_ok());

            let task = todo!();

            return Poll::Ready(Ok(task));
        }

        if let Some(cx) = cx {
            self.wakelist.lock().push(cx.waker().clone());
        }

        Poll::Pending
    }

    fn poll_result(
        &self,
        task: &mut TaskHandler,
        resp: Option<&mut ResultCell<R>>,
        cx: &mut Context<'_>,
        bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        let mut lock = self.current.lock();
        let mb_fut = lock.as_mut();
        if let Some(fut) = mb_fut.as_pin_mut() {
            let result = ready!(fut.poll(cx));
        }

        drop(unsafe { lock.as_mut().get_unchecked_mut() }.take());

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use futures::Future;
    use std::{any::Any, sync::Arc};

    use crate::{
        bus::Bus,
        cell::MsgCell,
        error::Error,
        handler::Handler,
        message::{Message, SharedMessage},
        receiver::IntoAbstractReceiver,
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
        type FlushFuture<'a> = impl Future<Output = Result<(), Error>> + 'a
        where
            Self: 'a;

        fn handle(&self, msg: &mut MsgCell<Msg>, _bus: &Bus) -> Self::HandleFuture<'_> {
            let msg = msg.peek().0;

            async move {
                tokio::time::sleep(std::time::Duration::from_millis(self.inner as _)).await;

                Ok(Msg(msg + 12))
            }
        }

        fn flush(&mut self, _bus: &Bus) -> Self::FlushFuture<'_> {
            async move { Ok(()) }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_queue_exec() -> Result<(), Error> {
        let bus = Bus::new();
        let wrapper = HandlerWrapper::new(Arc::new(Test { inner: 1 }));
        let unordered = Queue::new(wrapper, 100);
        let receiver = unordered.into_abstract_arc();
        let mut dest = Vec::new();

        let mut handlers = Vec::new();
        for i in 0..100 {
            let mut cell = MsgCell::new(Msg(i));
            handlers.push(receiver.send(&mut cell, bus.clone()).await?);
        }

        for h in handlers {
            let r: Msg = receiver.result(h, bus.clone()).await?;
            println!("pushing result {:?}", r);
            dest.push(r.0);
        }

        assert_eq!(dest, (12..112).collect::<Vec<_>>());

        Ok(())
    }
}
