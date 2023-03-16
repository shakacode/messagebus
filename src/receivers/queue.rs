use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{ready, Context, Poll},
};

use crossbeam::queue::ArrayQueue;
use futures::{task::AtomicWaker, Future};
use parking_lot::Mutex;

use crate::{
    bus::Bus,
    cell::{MsgCell, ResultCell},
    error::Error,
    message::Message,
    receiver::{Receiver, ReceiverEx},
    utils::{future_cell::FutureCell, wakelist::WakeList},
    TaskHandler, TaskHandlerVTable,
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

pub struct Current<M: Message, R: Message, T: Receiver<M, R> + 'static> {
    fut: FutureCell<SendFuture<M, R, T>>,
    index: usize,
    generation: u64,
    result: ResultCell<R>,
}
impl<M: Message, R: Message, T: Receiver<M, R> + 'static> Current<M, R, T> {
    fn new(index: usize, generation: u64) -> Current<M, R, T> {
        Self {
            fut: FutureCell::new(),
            index,
            generation,
            result: ResultCell::empty(),
        }
    }
}

type SendFuture<M: Message, R: Message, T: Receiver<M, R> + 'static> =
    impl Future<Output = Result<R, Error>> + Send;

pub struct Queue<M: Message, R: Message, T: Receiver<M, R> + 'static> {
    inner: Arc<T>,
    free: ArrayQueue<usize>,
    queue: ArrayQueue<QueueItem<M>>,
    wakers: Arc<Box<[Option<AtomicWaker>]>>,
    limit: usize,
    wakelist: WakeList,
    current: Arc<Mutex<Current<M, R, T>>>,
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
            wakers: Arc::new((0..limit).map(|_| None).collect()),
            wakelist: WakeList::new(),
            current: Arc::new(Mutex::new(Current::new(0, 0))),
            generation_sequence: AtomicU64::new(1),
        }
    }

    #[inline(always)]
    fn next_gen(&self) -> u64 {
        self.generation_sequence.fetch_add(1, Ordering::Relaxed)
    }

    fn split_index(&self, index: u64) -> (u64, usize) {
        (index >> 32, (index & u32::MAX as u64) as usize)
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
                if !lock.fut.is_set() {
                    lock.generation = self.next_gen();

                    let inner = self.inner.clone();
                    let bus = bus.clone();

                    enum TaskState<M: Message> {
                        Sending(MsgCell<M>),
                        PollingResult(TaskHandler),
                    }

                    let val = if let Ok(task) = self.inner.try_send(msg, &bus) {
                        TaskState::PollingResult(task)
                    } else {
                        TaskState::Sending(msg.take_cell())
                    };

                    lock.fut.set(async move {
                        match val {
                            TaskState::PollingResult(task) => inner.result(task, bus).await,
                            TaskState::Sending(cell) => inner.request(cell, bus).await,
                        }
                    });

                    return Poll::Ready(Ok(TaskHandler::new(
                        TaskHelperCancel::<M, R, T>::VTABLE,
                        self.current.clone(),
                        lock.generation,
                    )));
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

            let task = TaskHandler::new(
                TaskHelperUnqueue::<M, R, T>::VTABLE,
                self.wakers.clone(),
                (generation << 32) & index as u64,
            );

            return Poll::Ready(Ok(task));
        }

        if let Some(cx) = cx {
            self.wakelist.register(cx.waker());
        }

        Poll::Pending
    }

    fn poll_result(
        &self,
        task: &mut TaskHandler,
        resp: &mut ResultCell<R>,
        cx: &mut Context<'_>,
        bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        if let Some(mut lock) = self.current.try_lock() {
            if let Some(fut) = lock.fut.try_poll_unpin(cx) {
                let res = ready!(fut);
                if task.index() == lock.generation {
                    resp.put(res);
                } else {
                    lock.result.put(res);
                }
                if let Some(item) = self.queue.pop() {}
            }
        } else {
            let (gen, idx) = self.split_index(task.index());
            if idx as u32 != u32::MAX {
                // self.wakers[idx].waker.register(cx.waker());
            }

            return Poll::Pending;
        }

        Poll::Pending
    }

    fn poll_flush(&self, cx: &mut Context<'_>, bus: &Bus) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

struct TaskHelperCancel<M: Message, R: Message, T: Receiver<M, R> + 'static>(
    PhantomData<(M, R, T)>,
);
impl<M: Message, R: Message, T: Receiver<M, R> + 'static> TaskHelperCancel<M, R, T> {
    pub const VTABLE: &TaskHandlerVTable = &TaskHandlerVTable {
        drop: |data, gen| {
            let Ok(res) = data.downcast::<Mutex<Current<M, R, T>>>() else {
                return;
            };

            let lock = res.lock();
            if lock.generation == gen {
                // lock.fut.unset();
            }
        },
    };
}

struct TaskHelperUnqueue<M: Message, R: Message, T: Receiver<M, R> + 'static>(
    PhantomData<(M, R, T)>,
);
impl<M: Message, R: Message, T: Receiver<M, R> + 'static> TaskHelperUnqueue<M, R, T> {
    pub const VTABLE: &TaskHandlerVTable = &TaskHandlerVTable {
        drop: |data, indexgen| {
            let Ok(res) = data.downcast::<Box<[Option<AtomicWaker>]>>() else {
                return;
            };

            // res[(indexgen & u32::MAX as u64) as usize].clear();
        },
    };
}
#[cfg(test)]
mod tests {
    use futures::Future;
    use std::sync::Arc;

    use crate::{
        bus::Bus,
        cell::MsgCell,
        derive_message_clone,
        error::Error,
        handler::Handler,
        receiver::IntoAbstractReceiver,
        receivers::{queue::Queue, wrapper::HandlerWrapper},
    };

    #[derive(Debug, Clone, PartialEq)]
    struct Msg(pub u32);

    derive_message_clone!(MSG1, Msg, "test::Msg");

    struct Test {
        inner: u32,
    }

    impl Handler<Msg> for Test {
        type Response = Msg;
        type HandleFuture<'a> = impl Future<Output = Result<Self::Response, Error>> + 'a;
        type FlushFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;
        type CloseFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;

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

        fn close(&mut self) -> Self::CloseFuture<'_> {
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
