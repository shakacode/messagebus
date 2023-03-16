use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    task::{ready, Context, Poll},
};

use crossbeam::queue::ArrayQueue;
use parking_lot::Mutex;

use crate::{
    cell::{MsgCell, ResultCell},
    error::Error,
    message::Message,
    receiver::Receiver,
    utils::wakelist::WakeList,
    Bus, TaskHandler, TaskHandlerVTable,
};

enum OrderedSlotState<M: Message, R: Message> {
    Vacant,
    Sending(MsgCell<M>),
    PollingResult(TaskHandler),
    Done(ResultCell<R>),
}

impl<M: Message, R: Message> OrderedSlotState<M, R> {
    #[inline]
    pub fn set_vacant(&mut self) -> Self {
        std::mem::replace(self, Self::Vacant)
    }

    #[inline]
    pub fn set_sending(&mut self, msg: MsgCell<M>) -> Self {
        std::mem::replace(self, Self::Sending(msg))
    }

    #[inline]
    pub fn set_polling(&mut self, task: TaskHandler) -> Self {
        std::mem::replace(self, Self::PollingResult(task))
    }

    #[inline]
    pub fn set_done(&mut self, res: ResultCell<R>) -> Self {
        std::mem::replace(self, Self::Done(res))
    }

    /// Returns `true` if the unordered slot state is [`Vacant`].
    ///
    /// [`Vacant`]: UnorderedSlotState::Vacant
    #[must_use]
    #[inline]
    pub fn is_vacant(&self) -> bool {
        matches!(self, Self::Vacant)
    }

    /// Returns `true` if the ordered slot state is [`Done`].
    ///
    /// [`Done`]: OrderedSlotState::Done
    #[must_use]
    #[inline]
    fn is_done(&self) -> bool {
        matches!(self, Self::Done(..))
    }
}

struct OrderedSlot<M: Message, R: Message, T: Receiver<M, R> + 'static> {
    inner: T,
    generation: AtomicU64,
    state: Mutex<OrderedSlotState<M, R>>,
    _m: PhantomData<R>,
}

impl<M: Message, R: Message, T: Receiver<M, R> + 'static> OrderedSlot<M, R, T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            generation: AtomicU64::new(0),
            state: Mutex::new(OrderedSlotState::Vacant),
            _m: PhantomData,
        }
    }

    pub fn send(&self, msg: &mut MsgCell<M>, bus: &Bus) -> Result<u64, Error> {
        let mut lock = self.state.lock();
        assert!(lock.is_vacant());
        let gen = self.generation.fetch_add(1, Ordering::Relaxed);

        if let Poll::Ready(res) = self.inner.poll_send(msg, None, bus) {
            lock.set_polling(res?);
        } else {
            lock.set_sending(msg.take_cell());
        }

        Ok(gen + 1)
    }

    pub fn poll_result(
        &self,
        cx: &mut Context<'_>,
        res: &mut ResultCell<R>,
        bus: &Bus,
        result: bool,
    ) -> Poll<Result<(), Error>> {
        let mut lock = self.state.lock();

        loop {
            break match lock.set_vacant() {
                OrderedSlotState::Vacant => Poll::Ready(Ok(())),
                OrderedSlotState::Sending(mut msg) => {
                    match self.inner.poll_send(&mut msg, Some(cx), bus) {
                        Poll::Ready(Ok(task)) => {
                            lock.set_polling(task);
                            continue;
                        }
                        Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                        _ => {
                            lock.set_sending(msg);
                            Poll::Pending
                        }
                    }
                }
                OrderedSlotState::PollingResult(mut task) => {
                    if result {
                        if let Poll::Ready(res) = self.inner.poll_result(&mut task, res, cx, bus) {
                            break Poll::Ready(res);
                        }

                        lock.set_polling(task);
                    } else {
                        let mut cell = ResultCell::empty();
                        match self.inner.poll_result(&mut task, &mut cell, cx, bus) {
                            Poll::Ready(Ok(_)) => {
                                lock.set_done(cell);
                            }
                            Poll::Ready(res) => break Poll::Ready(res),
                            _ => {
                                lock.set_polling(task);
                            }
                        }
                    }

                    Poll::Pending
                }
                OrderedSlotState::Done(cell) => {
                    if result {
                        lock.set_done(cell);
                    }
                    Poll::Pending
                }
            };
        }
    }

    pub fn cancel(&self) {
        self.state.lock().set_vacant();
    }
}

pub struct OrderedInner<M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> {
    free_queue: ArrayQueue<usize>,
    slots: Box<[OrderedSlot<M, R, T>]>,
}

impl<M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> OrderedInner<M, R, T> {
    pub fn new(inner: T, capacity: usize) -> Self {
        let mut vec = Vec::with_capacity(capacity);
        let free_queue = ArrayQueue::new(capacity);
        for i in 0..capacity {
            free_queue.push(i).unwrap();
            vec.push(OrderedSlot::new(inner.clone()))
        }

        Self {
            free_queue,
            slots: vec.into(),
        }
    }
}

pub struct Ordered<M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> {
    inner: Arc<OrderedInner<M, R, T>>,
    queue: ArrayQueue<usize>,
    current: AtomicU64,
    send_wakelist: WakeList,
}

impl<'a, M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> Ordered<M, R, T> {
    pub fn new(inner: T, capacity: usize) -> Self {
        Self {
            inner: Arc::new(OrderedInner::new(inner, capacity)),
            queue: ArrayQueue::new(capacity),
            current: AtomicU64::new(u64::MAX),
            send_wakelist: WakeList::new(),
        }
    }

    #[inline]
    fn create_task(&self, index: usize, gen: u64) -> TaskHandler {
        TaskHandler::new(
            TaskHelper::<M, R, T>::VTABLE,
            self.inner.clone(),
            (gen << 32) | index as u64,
        )
    }
}

impl<'a, M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> Receiver<M, R>
    for Ordered<M, R, T>
{
    fn poll_send(
        &self,
        msg: &mut MsgCell<M>,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>> {
        let Some(index) = self.inner.free_queue.pop() else {
            if let Some(cx) = cx {
                self.send_wakelist.register(cx.waker());
            }

            return Poll::Pending;
        };

        let gen = self.inner.slots[index].send(msg, bus)?;

        Poll::Ready(Ok(self.create_task(index, gen)))
    }

    fn poll_result(
        &self,
        task: &mut TaskHandler,
        res: &mut ResultCell<R>,
        cx: &mut Context<'_>,
        bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        if !std::ptr::eq(task.data_ptr(), Arc::as_ptr(&self.inner) as *const ()) {
            return Poll::Ready(Err(Error::ErrorPollWrongTask(String::from("ptr not eq"))));
        }

        if let Some(slot) = self.inner.slots.get(task.index() as usize) {
            if self.current.load(Ordering::Relaxed) == task.index() {

                // if slot.generation.load(Ordering::Relaxed) != task.generation() {
                //     return Poll::Ready(Ok(())); // TaskHandler
                // }
            }

            let current = if self.current.load(Ordering::Relaxed) == task.index() {
                true
            } else {
                false
            };

            ready!(slot.poll_result(cx, res, bus, current))?;

            task.finish();
            self.send_wakelist.wake_all();
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(Error::ErrorPollWrongTask(format!(
                "index not found {}",
                task.index()
            ))))
        }
    }

    fn poll_flush(&self, _cx: &mut Context<'_>, _bus: &Bus) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(&self, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

struct TaskHelper<M: Message, R: Message, T: Receiver<M, R> + Clone + 'static>(
    PhantomData<(M, R, T)>,
);
impl<M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> TaskHelper<M, R, T> {
    pub const VTABLE: &TaskHandlerVTable = &TaskHandlerVTable {
        drop: |data, index| {
            let Ok(res) = data.downcast::<OrderedInner<M, R, T>>() else {
                return;
            };

            if let Some(slot) = res.slots.get(index as usize) {
                slot.cancel();
            }

            let _ = res.free_queue.push(index as _);
        },
    };
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{stream::FuturesUnordered, Future, TryStreamExt};

    use crate::{
        cell::MsgCell, derive_message_clone, error::Error, handler::Handler, receiver::ReceiverEx,
        receivers::wrapper::HandlerWrapper, Bus,
    };

    use super::Ordered;

    #[derive(Debug, Clone, PartialEq)]
    struct Msg(pub u32);
    derive_message_clone!(TEST_UNORDERED_MSG, Msg, "test::Msg");

    #[derive(Clone)]
    struct Test {
        inner: u32,
    }

    impl Handler<Msg> for Test {
        type Response = Msg;
        type HandleFuture<'a> = impl Future<Output = Result<Self::Response, Error>> + 'a;
        type FlushFuture<'a> = std::future::Ready<Result<(), Error>>;
        type CloseFuture<'a> = std::future::Ready<Result<(), Error>>;

        fn handle(&self, msg: &mut MsgCell<Msg>, _bus: &Bus) -> Self::HandleFuture<'_> {
            let msg = msg.peek().0;

            async move {
                let wait = match msg {
                    1 => 40,
                    2 => 20,
                    3 => 10,
                    4 => 30,
                    _ => 1,
                };
                tokio::time::sleep(std::time::Duration::from_millis(wait)).await;

                Ok(Msg(msg + self.inner))
            }
        }

        fn flush(&mut self, _bus: &Bus) -> Self::FlushFuture<'_> {
            std::future::ready(Ok(()))
        }

        fn close(&mut self) -> Self::CloseFuture<'_> {
            std::future::ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_unordered_exec() -> Result<(), Error> {
        let bus = Bus::new();
        let un = Ordered::new(HandlerWrapper::new(Arc::new(Test { inner: 12 })), 4);
        let h1 = un.try_send(&mut MsgCell::new(Msg(1)), &bus).unwrap();
        let h2 = un.try_send(&mut MsgCell::new(Msg(2)), &bus).unwrap();
        let h3 = un.try_send(&mut MsgCell::new(Msg(3)), &bus).unwrap();
        let h4 = un.try_send(&mut MsgCell::new(Msg(4)), &bus).unwrap();
        let _ = un.try_send(&mut MsgCell::new(Msg(5)), &bus).unwrap_err();

        let u = FuturesUnordered::new();
        u.push(un.result(h1, bus.clone()));
        u.push(un.result(h2, bus.clone()));
        u.push(un.result(h3, bus.clone()));
        u.push(un.result(h4, bus));

        assert_eq!(
            u.try_collect::<Vec<_>>().await?,
            vec![Msg(15), Msg(14), Msg(16), Msg(13)]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_unordered_exec_with_cancel() -> Result<(), Error> {
        let bus = Bus::new();
        let un = Ordered::new(HandlerWrapper::new(Arc::new(Test { inner: 12 })), 4);
        let h1 = un.try_send(&mut MsgCell::new(Msg(1)), &bus).unwrap();
        let h2 = un.try_send(&mut MsgCell::new(Msg(2)), &bus).unwrap();
        let h3 = un.try_send(&mut MsgCell::new(Msg(3)), &bus).unwrap();
        let h4 = un.try_send(&mut MsgCell::new(Msg(4)), &bus).unwrap();
        let _ = un.try_send(&mut MsgCell::new(Msg(5)), &bus).unwrap_err();
        drop(h3);

        let u = FuturesUnordered::new();
        u.push(un.result(h1, bus.clone()));
        u.push(un.result(h2, bus.clone()));
        u.push(un.result(h4, bus));

        assert_eq!(
            u.try_collect::<Vec<_>>().await?,
            vec![Msg(14), Msg(16), Msg(13)]
        );

        Ok(())
    }
}
