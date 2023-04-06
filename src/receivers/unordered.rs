use std::{
    marker::PhantomData,
    sync::Arc,
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
    Bus, TaskHandler,
};

enum UnorderedSlotState<M: Message> {
    Vacant,
    Sending(MsgCell<M>),
    PollingResult(TaskHandler),
}

impl<M: Message> UnorderedSlotState<M> {
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

    /// Returns `true` if the unordered slot state is [`Vacant`].
    ///
    /// [`Vacant`]: UnorderedSlotState::Vacant
    #[must_use]
    #[inline]
    pub fn is_vacant(&self) -> bool {
        matches!(self, Self::Vacant)
    }
}

struct UnorderedSlot<M: Message, R: Message, T: Receiver<M, R> + 'static> {
    inner: T,
    state: Mutex<UnorderedSlotState<M>>,
    _m: PhantomData<R>,
}

impl<M: Message, R: Message, T: Receiver<M, R> + 'static> UnorderedSlot<M, R, T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            state: Mutex::new(UnorderedSlotState::Vacant),
            _m: PhantomData,
        }
    }

    pub fn send(&self, msg: &mut MsgCell<M>, bus: &Bus) -> Result<(), Error> {
        let mut lock = self.state.lock();
        assert!(lock.is_vacant());

        if let Poll::Ready(res) = self.inner.poll_send(msg, None, bus) {
            lock.set_polling(res?);
        } else {
            lock.set_sending(msg.take_cell());
        }

        Ok(())
    }

    pub fn poll_result(
        &self,
        cx: &mut Context<'_>,
        res: &mut ResultCell<R>,
        bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        let mut lock = self.state.lock();
        loop {
            break match lock.set_vacant() {
                UnorderedSlotState::Vacant => Poll::Ready(Ok(())),
                UnorderedSlotState::Sending(mut msg) => {
                    if let Poll::Ready(res) = self.inner.poll_send(&mut msg, Some(cx), bus) {
                        match res {
                            Ok(task) => {
                                lock.set_polling(task);
                                continue;
                            }
                            Err(err) => break Poll::Ready(Err(err)),
                        }
                    }
                    lock.set_sending(msg);
                    Poll::Pending
                }
                UnorderedSlotState::PollingResult(mut task) => {
                    if let Poll::Ready(res) = self.inner.poll_result(&mut task, res, cx, bus) {
                        Poll::Ready(res)
                    } else {
                        lock.set_polling(task);
                        Poll::Pending
                    }
                }
            };
        }
    }

    pub fn cancel(&self) {
        self.state.lock().set_vacant();
    }
}

pub struct UnorderedInner<M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> {
    free_queue: ArrayQueue<usize>,
    slots: Box<[UnorderedSlot<M, R, T>]>,
}

impl<M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> UnorderedInner<M, R, T> {
    pub fn new(inner: T, capacity: usize) -> Self {
        let mut vec = Vec::with_capacity(capacity);
        let free_queue = ArrayQueue::new(capacity);
        for i in 0..capacity {
            free_queue.push(i).unwrap();
            vec.push(UnorderedSlot::new(inner.clone()))
        }

        Self {
            free_queue,
            slots: vec.into(),
        }
    }
}

pub struct Unordered<M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> {
    inner: Arc<UnorderedInner<M, R, T>>,
    send_wakelist: WakeList,
}

impl<'a, M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> Unordered<M, R, T> {
    pub fn new(inner: T, capacity: usize) -> Self {
        Self {
            inner: Arc::new(UnorderedInner::new(inner, capacity)),
            send_wakelist: WakeList::new(),
        }
    }

    fn create_task(&self, index: usize) -> TaskHandler {
        TaskHandler::new(self.inner.clone(), index as _, |data, index| {
            let Ok(res) = data.downcast::<UnorderedInner<M, R, T>>() else {
                return;
            };

            if let Some(slot) = res.slots.get(index as usize) {
                slot.cancel();
            }

            let _ = res.free_queue.push(index as usize);
        })
    }
}

impl<'a, M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> Receiver<M, R>
    for Unordered<M, R, T>
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

        self.inner.slots[index].send(msg, bus)?;

        Poll::Ready(Ok(self.create_task(index)))
    }

    fn poll_result(
        &self,
        task: &mut TaskHandler,
        res: &mut ResultCell<R>,
        cx: &mut Context<'_>,
        bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        ready!(self.inner.slots[task.index() as usize].poll_result(cx, res, bus))?;
        task.finish();
        self.send_wakelist.wake_all();
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&self, _cx: &mut Context<'_>, _bus: &Bus) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(&self, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{stream::FuturesUnordered, Future, TryStreamExt};

    use crate::{
        cell::MsgCell, derive::Message, error::Error, handler::Handler, receiver::ReceiverEx,
        receivers::wrapper::HandlerWrapper, Bus,
    };

    use super::Unordered;
    use crate as messagebus;

    #[derive(Debug, Clone, PartialEq, Message)]
    struct Msg(pub u32);

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

        fn flush(&self, _bus: &Bus) -> Self::FlushFuture<'_> {
            std::future::ready(Ok(()))
        }

        fn close(&self) -> Self::CloseFuture<'_> {
            std::future::ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_unordered_exec() -> Result<(), Error> {
        let bus = Bus::new();
        let un = Unordered::new(HandlerWrapper::new(Arc::new(Test { inner: 12 })), 4);
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
        let un = Unordered::new(HandlerWrapper::new(Arc::new(Test { inner: 12 })), 4);
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
