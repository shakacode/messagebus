use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{ready, Context, Poll},
};

use crossbeam::queue::ArrayQueue;
use futures::Future;
use parking_lot::Mutex;

use crate::{
    cell::{MsgCell, ResultCell},
    error::{Error, ErrorKind},
    message::Message,
    receiver::Receiver,
    utils::wakelist::WakeList,
    Bus, TaskHandler,
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
    current: Mutex<usize>,
    send_wakelist: WakeList,
}

impl<'a, M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> Ordered<M, R, T> {
    pub fn new(inner: T, capacity: usize) -> Self {
        Self {
            inner: Arc::new(OrderedInner::new(inner, capacity)),
            queue: ArrayQueue::new(capacity),
            current: Mutex::new(usize::MAX),
            send_wakelist: WakeList::new(),
        }
    }

    #[inline]
    fn create_task(&self, index: usize, gen: u64) -> TaskHandler {
        TaskHandler::new(
            self.inner.clone(),
            (gen << 32) | index as u64,
            |data, index| {
                let Ok(res) = data.downcast::<OrderedInner<M, R, T>>() else {
                return;
            };

                if let Some(slot) = res.slots.get(index as usize) {
                    slot.cancel();
                }

                let _ = res.free_queue.push(index as _);
            },
        )
    }
}

impl<M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> Receiver<M, R>
    for Ordered<M, R, T>
{
    type InitFuture<'a> = impl Future<Output = Result<(), Error>> + Send + 'a;
    type CloseFuture<'a> = impl Future<Output = Result<(), Error>> + Send + 'a;
    type FlushFuture<'a> = impl Future<Output = Result<(), Error>> + Send + 'a;

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
            return Poll::Ready(Err(ErrorKind::ErrorPollWrongTask(String::from(
                "ptr not eq",
            ))
            .into()));
        }

        let (gen, idx) = (task.index() >> 32, task.index() & u32::MAX as u64);

        if let Some(slot) = self.inner.slots.get(idx as usize) {
            let mut current = self.current.lock();
            if *current == usize::MAX {
                match self.queue.pop() {
                    None => {
                        *current = usize::MAX;
                    }
                    Some(idx) => *current = idx,
                }
            }
            println!("polling {} {} {}", idx, gen, *current);

            let res = ready!(slot.poll_result(cx, res, bus, *current == idx as usize));
            println!("res {:?}", res);

            self.queue.push(idx as _).unwrap();

            task.finish();
            self.send_wakelist.wake_all();
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(ErrorKind::ErrorPollWrongTask(format!(
                "index not found {}; gen {}",
                idx, gen
            ))
            .into()))
        }
    }

    fn init(&self, bus: &Bus) -> Self::InitFuture<'_> {
        async move { Ok(()) }
    }

    fn flush(&self, bus: &Bus) -> Self::FlushFuture<'_> {
        async move { Ok(()) }
    }

    fn close(&self) -> Self::CloseFuture<'_> {
        async move { Ok(()) }
    }
}

#[cfg(test)]
mod tests {
    use futures::{
        stream::{FuturesOrdered, FuturesUnordered},
        Future, TryStreamExt,
    };
    use std::sync::Arc;

    use crate::{
        cell::MsgCell, derive, error::Error, handler::Handler, receiver::ReceiverEx,
        receivers::wrapper::HandlerWrapper, Bus,
    };

    use super::Ordered;
    use crate as messagebus;

    #[derive(Debug, Clone, PartialEq, derive::Message)]
    struct Msg(pub u32);

    #[derive(Clone)]
    struct Test {
        inner: u32,
    }

    impl Handler<Msg> for Test {
        type Response = Msg;
        type InitFuture<'a> = std::future::Ready<Result<(), Error>>;
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

        fn init(&self, _bus: &Bus) -> Self::InitFuture<'_> {
            std::future::ready(Ok(()))
        }

        fn flush(&self, _bus: &Bus) -> Self::FlushFuture<'_> {
            std::future::ready(Ok(()))
        }

        fn close(&self) -> Self::CloseFuture<'_> {
            std::future::ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_ordered_exec() -> Result<(), Error> {
        let bus = Bus::new();
        let un = Ordered::new(HandlerWrapper::new(Arc::new(Test { inner: 12 })), 4);
        let h1 = un.try_send(&mut MsgCell::new(Msg(1)), &bus).unwrap();
        let h2 = un.try_send(&mut MsgCell::new(Msg(2)), &bus).unwrap();
        let h3 = un.try_send(&mut MsgCell::new(Msg(3)), &bus).unwrap();
        let h4 = un.try_send(&mut MsgCell::new(Msg(4)), &bus).unwrap();
        let _ = un.try_send(&mut MsgCell::new(Msg(5)), &bus).unwrap_err();

        println!("1111");
        let mut u = FuturesOrdered::new();
        u.push(un.result(h1, bus.clone()));
        u.push(un.result(h2, bus.clone()));
        u.push(un.result(h3, bus.clone()));
        u.push(un.result(h4, bus));

        println!("2222");
        assert_eq!(
            u.try_collect::<Vec<_>>().await?,
            vec![Msg(15), Msg(14), Msg(16), Msg(13)]
        );

        Ok(())
    }

    // #[tokio::test]
    async fn test_ordered_exec_with_cancel() -> Result<(), Error> {
        let bus = Bus::new();
        let un = Ordered::new(HandlerWrapper::new(Arc::new(Test { inner: 12 })), 4);
        let h1 = un.try_send(&mut MsgCell::new(Msg(1)), &bus).unwrap();
        let h2 = un.try_send(&mut MsgCell::new(Msg(2)), &bus).unwrap();
        let h3 = un.try_send(&mut MsgCell::new(Msg(3)), &bus).unwrap();
        let h4 = un.try_send(&mut MsgCell::new(Msg(4)), &bus).unwrap();
        let _ = un.try_send(&mut MsgCell::new(Msg(5)), &bus).unwrap_err();
        drop(h3);

        let mut u = FuturesOrdered::new();
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
