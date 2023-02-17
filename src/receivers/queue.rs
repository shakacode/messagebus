use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    task::{ready, Context, Poll, Waker},
};

use arc_swap::{ArcSwapAny, ArcSwapOption};
use crossbeam::{atomic::AtomicCell, queue::ArrayQueue};
use futures::task::AtomicWaker;
use parking_lot::Mutex;
use sharded_slab::Slab;

use crate::{
    bus::{Bus, TaskHandler},
    cell::{MsgCell, ResultCell},
    error::Error,
    message::Message,
    receiver::Receiver,
};

enum QueueTaskInner<M> {
    Vacant,
    Sending(MsgCell<M>),
    WaitingResult,
}

struct QueueTask<M: Message> {
    index: usize,
    waker: AtomicWaker,
    generation: AtomicU64,
    message: Mutex<QueueTaskInner<M>>,
}

impl<M: Message> QueueTask<M> {
    fn check_task(&self, handler: &TaskHandler) -> bool {
        false
    }
}

pub struct Queue<M: Message, R: Message, T: Receiver<M, R> + 'static> {
    inner: T,

    queue: ArrayQueue<usize>,
    free: ArrayQueue<usize>,
    enqueued: AtomicUsize,

    generation_sequence: AtomicU64,

    tasks: Arc<[Arc<QueueTask<M>>]>,
    current_task: AtomicUsize,
    _m: PhantomData<(M, R, T)>,
}

impl<M: Message, R: Message, T: Receiver<M, R> + 'static> Queue<M, R, T> {
    pub fn new(inner: T, limit: usize) -> Self {
        let free = ArrayQueue::new(limit);
        for i in 0..limit {
            free.push(i);
        }

        Self {
            inner,
            free,
            queue: ArrayQueue::new(limit),
            enqueued: AtomicUsize::new(0),
            generation_sequence: AtomicU64::new(0),
            tasks: (0..limit)
                .map(|index| {
                    Arc::new(QueueTask {
                        index,
                        waker: AtomicWaker::new(),
                        generation: AtomicU64::new(0),
                        message: Mutex::new(QueueTaskInner::Vacant),
                    })
                })
                .collect(),
            current_task: AtomicUsize::new(usize::MAX),
            _m: PhantomData::default(),
        }
    }

    #[inline]
    fn update_task_waker(&self, _task: &TaskHandler, _wakerr: &Waker) {
        // TODO
    }
}

impl<'a, M: Message, R: Message, T: Receiver<M, R> + 'static> Receiver<M, R> for Queue<M, R, T> {
    fn poll_send(
        &self,
        msg: &mut MsgCell<M>,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>> {
        if let Some(index) = self.free.pop() {
            if let Ok(_) = self.current_task.compare_exchange(
                usize::MAX,
                index,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                // fast track
                self.tasks[index].start(msg);
                let th = TaskHandler::new(vtable, self.tasks.clone(), index);

                Poll::Ready(Ok(th))
            }
        }

        if current.is_none() {
            let Some(index) = self.free.pop() else {
                // TODO
                // self.send_wakelist.push();
                return Poll::Pending;
            };

            // .start(msg);
            drop(current);
        }

        Poll::Pending
    }

    fn poll_result(
        &self,
        task: &TaskHandler,
        _resp: Option<&mut ResultCell<R>>,
        cx: &mut Context<'_>,
        bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        let current_task = self.current_task.lock();
        let index = if let Some(hash) = *current_task {
            if hash != task.hash() {
                self.update_task_waker(task, cx.waker());
                return Poll::Pending;
            }

            task.index()
        } else {
            if let Some(index) = self.queue.pop() {
                index
            } else {
                return Poll::Ready(Err(Error::TrySendError));
            }
        };

        let entry = self.tasks.get(index).unwrap();

        // let task = self.tasks.remove(idx)

        let res = ready!(self
            .inner
            .poll_send(&mut *entry.message.lock(), Some(cx), bus))?;

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
