use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    task::{ready, Context, Poll},
};

use futures::{task::AtomicWaker, Future};
use sharded_slab::{Clear, Pool};
use tokio::sync::mpsc;

use crate::{
    bus::Bus,
    cell::{MsgCell, ResultCell},
    error::{Error, ErrorKind},
    message::Message,
    receiver::{Receiver, ReceiverEx},
    utils::{
        future_cell::{FutureCell, SyncFutureCell},
        Mutex, MutexGuard,
    },
    TaskHandler,
};

pub struct Current<M: Message, R: Message, T: Receiver<M, R> + 'static> {
    fut: FutureCell<ResultFuture<M, R, T>>,
    recv: mpsc::Receiver<QueueItem<M>>,
    index: usize,
}
impl<M: Message, R: Message, T: Receiver<M, R> + 'static> Current<M, R, T> {
    fn new(recv: mpsc::Receiver<QueueItem<M>>) -> Current<M, R, T> {
        Self {
            fut: FutureCell::new(),
            index: usize::MAX,
            recv,
        }
    }

    #[inline]
    pub(crate) fn clear(&mut self) {
        self.fut.clear();
        self.index = usize::MAX;
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        !self.fut.is_set()
    }

    #[inline]
    pub fn set(&mut self, index: usize, fut: ResultFuture<M, R, T>) {
        self.fut.set(fut);
        self.index = index;
    }
}

type SendFuture<M: Message, R: Message, T: Receiver<M, R> + 'static> =
    impl Future<Output = Result<usize, Error>> + Send;

pub type ResultFuture<M: Message, R: Message, T: Receiver<M, R> + 'static> =
    impl Future<Output = Result<R, Error>> + Send;

pub struct QueueItem<M> {
    msg: MsgCell<M>,
    index: usize,
}

impl<M> Drop for QueueItem<M> {
    fn drop(&mut self) {
        SLOTS.clear(self.index);
    }
}

impl<M> QueueItem<M> {
    pub fn new(msg: MsgCell<M>) -> Self {
        Self {
            msg,
            index: SLOTS.create_with(|_| ()).unwrap(),
        }
    }
}

pub struct QueueSlot {
    waker: AtomicWaker,
    dropped: AtomicBool,
}

impl QueueSlot {
    fn mark_dropped(&self) {
        self.waker.take();
        self.dropped.store(true, Ordering::Release);
    }
}

impl Default for QueueSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl Clear for QueueSlot {
    fn clear(&mut self) {
        self.waker.take();
        self.dropped.store(false, Ordering::Release);
    }
}

lazy_static::lazy_static! {
    static ref SLOTS: Arc<Pool<QueueSlot>> = Arc::new(Pool::new());
}

impl QueueSlot {
    pub fn new() -> Self {
        Self {
            waker: AtomicWaker::new(),
            dropped: AtomicBool::new(false),
        }
    }
}

pub struct Queue<M: Message, R: Message, T: Receiver<M, R> + 'static> {
    inner: Arc<T>,
    queue_send: mpsc::Sender<QueueItem<M>>,
    queue_send_fut: SyncFutureCell<SendFuture<M, R, T>>,
    current: Arc<Mutex<Current<M, R, T>>>,
    generation_sequence: AtomicU64,
}

impl<M: Message, R: Message, T: Receiver<M, R> + 'static> Queue<M, R, T> {
    pub fn new(inner: T, limit: usize) -> Self {
        let (queue_send, queue_recv) = mpsc::channel(limit);
        Self {
            inner: Arc::new(inner),
            queue_send,
            queue_send_fut: SyncFutureCell::new(),
            current: Arc::new(Mutex::new(Current::new(queue_recv))),
            generation_sequence: AtomicU64::new(1),
        }
    }

    fn set_current(
        &self,
        current: &mut MutexGuard<'_, Current<M, R, T>>,
        mut item: QueueItem<M>,
        bus: &Bus,
    ) -> bool {
        if let Some(slot) = SLOTS.get(item.index) {
            if !slot.dropped.load(Ordering::Acquire) {
                current.set(
                    item.index as _,
                    gen_request_fut::<M, R, T>(
                        self.inner.clone(),
                        None,
                        item.msg.take_cell(),
                        bus.clone(),
                    ),
                );
                drop(current);

                slot.waker.wake();

                return true;
            }
        }

        false
    }
}

async fn gen_request_fut<M: Message, R: Message, T: Receiver<M, R> + 'static>(
    inner: Arc<T>,
    task: Option<TaskHandler>,
    msg: MsgCell<M>,
    bus: Bus,
) -> Result<R, Error> {
    let task = if let Some(task) = task {
        task
    } else {
        inner.send(msg, bus.clone()).await?
    };

    inner.result(task, bus).await
}

impl<M: Message, R: Message, T: Receiver<M, R> + 'static> Receiver<M, R> for Queue<M, R, T> {
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
        // trying fast track
        if self.queue_send.capacity() == self.queue_send.max_capacity() {
            if let Some(mut current) = self.current.try_lock() {
                if current.is_empty() {
                    match self.inner.poll_send(msg, None, bus) {
                        Poll::Ready(Ok(task)) => {
                            current.set(
                                usize::MAX,
                                gen_request_fut::<M, R, T>(
                                    self.inner.clone(),
                                    Some(task),
                                    msg.take_cell(),
                                    bus.clone(),
                                ),
                            );

                            return Poll::Ready(Ok(TaskHandler::new(
                                self.current.clone(),
                                usize::MAX as _,
                                |data, idx| {
                                    if let Ok(d) = data.downcast::<Mutex<Current<M, R, T>>>() {
                                        let mut lock = d.blocking_lock();
                                        if lock.index == idx as _ {
                                            lock.clear();
                                        }
                                    }
                                },
                            )));
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Pending => {
                            current.set(
                                usize::MAX,
                                gen_request_fut::<M, R, T>(
                                    self.inner.clone(),
                                    None,
                                    msg.take_cell(),
                                    bus.clone(),
                                ),
                            );

                            return Poll::Ready(Ok(TaskHandler::new(
                                self.current.clone(),
                                usize::MAX as _,
                                |data, idx| {
                                    if let Ok(d) = data.downcast::<Mutex<Current<M, R, T>>>() {
                                        let mut lock = d.blocking_lock();
                                        if lock.index == idx as _ {
                                            lock.clear();
                                        }
                                    }
                                },
                            )));
                        }
                    }
                }
            }
        }

        let index = if let Some(cx) = cx {
            ready!(self.queue_send_fut.poll_unpin_with(cx, || {
                let msg = msg.take_cell();
                let sender = self.queue_send.clone();

                async move {
                    let item = QueueItem::new(msg);
                    let index = item.index;

                    sender.send(item).await.map_err(|_| ErrorKind::SendError)?;
                    Ok(index)
                }
            }))?
        } else {
            let item = QueueItem::new(msg.take_cell());
            let index = item.index;

            self.queue_send
                .try_send(item)
                .map_err(|_| ErrorKind::SendError)?;

            index
        };

        Poll::Ready(Ok(TaskHandler::new(
            SLOTS.clone(),
            index as _,
            |data, index| {
                if let Ok(slots) = data.downcast::<Pool<QueueSlot>>() {
                    if let Some(item) = slots.get(index as _) {
                        item.mark_dropped();
                    }
                }
            },
        )))
    }

    fn poll_result(
        &self,
        task: &mut TaskHandler,
        resp: &mut ResultCell<R>,
        cx: &mut Context<'_>,
        bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        if !(std::ptr::eq(task.data_ptr(), Arc::as_ptr(&SLOTS) as _)
            || std::ptr::eq(task.data_ptr(), Arc::as_ptr(&self.current) as _))
        {
            return Poll::Ready(Err(ErrorKind::ErrorPollWrongTask("".into()).into()));
        }

        let mut current = ready!(self.current.poll_lock(cx));
        if !current.fut.is_set() || (task.index() == current.index as u64) {
            if let Some(poll) = current.fut.try_poll_unpin(cx) {
                resp.put(ready!(poll));

                loop {
                    if let Ok(item) = current.recv.try_recv() {
                        if !self.set_current(&mut current, item, bus) {
                            continue;
                        }
                    }

                    break;
                }

                Poll::Ready(Ok(()))
            } else {
                loop {
                    let item = ready!(current.recv.poll_recv(cx)).ok_or(ErrorKind::SendError)?;

                    if task.index() as usize == item.index {
                        cx.waker().wake_by_ref();
                    }

                    if !self.set_current(&mut current, item, bus) {
                        continue;
                    }

                    return Poll::Pending;
                }
            }
        } else {
            if let Some(slot) = SLOTS.get(task.index() as usize) {
                if slot.dropped.load(Ordering::Acquire) {
                    current.fut.clear();
                    cx.waker().wake_by_ref();
                } else {
                    slot.waker.register(cx.waker());
                }
            } else {
                return Poll::Ready(Err(ErrorKind::ErrorPollWrongTask("".into()).into()));
            }

            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{join, Future};
    use rand::RngCore;
    use std::{sync::Arc, task::Poll};
    use tokio::sync::mpsc;

    use crate::{
        bus::Bus,
        cell::MsgCell,
        derive::Message,
        error::{Error, ErrorKind},
        handler::Handler,
        receiver::{Receiver, ReceiverEx},
        receivers::{queue::Queue, wrapper::HandlerWrapper},
    };

    use crate as messagebus;

    #[derive(Debug, Clone, PartialEq, Message)]
    struct Msg(pub u32);

    struct Test {
        inner: u32,
        other: bool,
    }

    impl Handler<Msg> for Test {
        type Response = Msg;
        type HandleFuture<'a> = impl Future<Output = Result<Self::Response, Error>> + 'a;
        type FlushFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;
        type CloseFuture<'a> = impl Future<Output = Result<(), Error>> + 'a;
        type InitFuture<'a> = std::future::Ready<Result<(), Error>>;

        fn init(&self, _: &Bus) -> Self::InitFuture<'_> {
            std::future::ready(Ok(()))
        }

        fn handle(&self, msg: &mut MsgCell<Msg>, _bus: &Bus) -> Self::HandleFuture<'_> {
            let msg = msg.peek().0;

            async move {
                tokio::time::sleep(std::time::Duration::from_millis(self.inner as _)).await;
                if self.other {
                    let val = {
                        let mut rng = rand::thread_rng();
                        rng.next_u32() % 10
                    };

                    tokio::time::sleep(std::time::Duration::from_millis((self.inner + val) as _))
                        .await;
                }

                Ok(Msg(msg + 12))
            }
        }

        fn flush(&self, _bus: &Bus) -> Self::FlushFuture<'_> {
            async move { Ok(()) }
        }

        fn close(&self) -> Self::CloseFuture<'_> {
            async move { Ok(()) }
        }
    }

    #[tokio::test]
    async fn test_queue_send_bound() -> Result<(), Error> {
        let bus = Bus::new();
        let wrapper = HandlerWrapper::new(Arc::new(Test {
            inner: 10,
            other: false,
        }));
        let queue = Queue::new(wrapper, 2);

        let poll = queue.poll_send(&mut MsgCell::new(Msg(1)), None, &bus);
        let _handler = match poll {
            Poll::Ready(Ok(task)) => task,
            _ => unreachable!(),
        };

        let poll = queue.poll_send(&mut MsgCell::new(Msg(2)), None, &bus);
        let _handler = match poll {
            Poll::Ready(Ok(task)) => task,
            _ => unreachable!(),
        };

        let poll = queue.poll_send(&mut MsgCell::new(Msg(3)), None, &bus);
        let _handler = match poll {
            Poll::Ready(Ok(task)) => task,
            _ => unreachable!(),
        };

        let poll = queue.poll_send(&mut MsgCell::new(Msg(4)), None, &bus);
        assert!(matches!(
            poll,
            Poll::Ready(Err(Error {
                kind: ErrorKind::SendError,
                ..
            }))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_queue_poll_result() -> Result<(), Error> {
        let bus = Bus::new();
        let wrapper = HandlerWrapper::new(Arc::new(Test {
            inner: 10,
            other: false,
        }));
        let queue = Queue::new(wrapper, 2);
        // let waker = waker_fn::waker_fn(move || {});
        // let cx = Context::from_waker(&waker);

        println!("\n\n  <>  \n\n");

        let poll = queue.poll_send(&mut MsgCell::new(Msg(1)), None, &bus);
        let h1 = match poll {
            Poll::Ready(Ok(task)) => task,
            _ => unreachable!(),
        };

        println!("h1 {}\n\n", h1.index());

        let poll = queue.poll_send(&mut MsgCell::new(Msg(2)), None, &bus);
        let h2 = match poll {
            Poll::Ready(Ok(task)) => task,
            _ => unreachable!(),
        };
        println!("h2 {}\n\n", h2.index());

        let poll = queue.poll_send(&mut MsgCell::new(Msg(3)), None, &bus);
        let h3 = match poll {
            Poll::Ready(Ok(task)) => task,
            _ => unreachable!(),
        };
        println!("h3 {}\n\n", h3.index());
        // let mut res = ResultCell::empty();

        println!("\n\n ------------ \n\n");

        let (r2, r1, r3) = futures::join! {
            queue.result(h2, bus.clone()),
            queue.result(h1, bus.clone()),
            queue.result(h3, bus.clone()),
        };

        assert_eq!((r1?, r2?, r3?), (Msg(13), Msg(14), Msg(15)));

        Ok(())
    }

    async fn test_complex(bus: Bus, offset: u32, spread: u32, count: u32, queue: usize) {
        let wrapper = HandlerWrapper::new(Arc::new(Test {
            inner: 10,
            other: true,
        }));
        let queue = Arc::new(Queue::new(wrapper, queue));
        let prod = queue.clone();
        let bus1 = bus.clone();
        let (tx, mut rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            for val in 0..count {
                let rand = {
                    let mut rng = rand::thread_rng();
                    rng.next_u32() % spread
                };

                tokio::time::sleep(std::time::Duration::from_millis((offset + rand) as _)).await;
                let task = prod
                    .send(MsgCell::new(Msg(val)), bus1.clone())
                    .await
                    .unwrap();

                tx.send(task).unwrap();
            }
        });

        let cons = queue.clone();
        let vec = tokio::spawn(async move {
            let mut vec = Vec::new();
            while let Some(task) = rx.recv().await {
                vec.push(cons.result(task, bus.clone()).await.unwrap().0);
            }
            vec
        })
        .await
        .unwrap();

        assert_eq!(vec, (12..).take(count as _).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_queue_complex() -> Result<(), Error> {
        let bus = Bus::new();

        let t1 = tokio::spawn(test_complex(bus.clone(), 10, 10, 100, 10));
        let t2 = tokio::spawn(test_complex(bus.clone(), 0, 10, 100, 10));
        let t3 = tokio::spawn(test_complex(bus.clone(), 20, 10, 100, 10));
        let t4 = tokio::spawn(test_complex(bus.clone(), 10, 20, 200, 100));

        let (r1, r2, r3, r4) = join!(t1, t2, t3, t4);
        let _ = (r1.unwrap(), r2.unwrap(), r3.unwrap(), r4.unwrap());

        Ok(())
    }

    #[tokio::test]
    async fn test_queue_current_dropped() -> Result<(), Error> {
        let bus = Bus::new();
        let wrapper = HandlerWrapper::new(Arc::new(Test {
            inner: 10,
            other: false,
        }));
        let queue = Queue::new(wrapper, 2);
        // let waker = waker_fn::waker_fn(move || {});
        // let cx = Context::from_waker(&waker);

        println!("\n\n  <>  \n\n");

        let poll = queue.poll_send(&mut MsgCell::new(Msg(1)), None, &bus);
        let h1 = match poll {
            Poll::Ready(Ok(task)) => task,
            _ => unreachable!(),
        };

        println!("h1 {}\n\n", h1.index());

        let poll = queue.poll_send(&mut MsgCell::new(Msg(2)), None, &bus);
        let h2 = match poll {
            Poll::Ready(Ok(task)) => task,
            _ => unreachable!(),
        };
        println!("h2 {}\n\n", h2.index());

        let poll = queue.poll_send(&mut MsgCell::new(Msg(3)), None, &bus);
        let h3 = match poll {
            Poll::Ready(Ok(task)) => task,
            _ => unreachable!(),
        };
        println!("h3 {}\n\n", h3.index());
        // let mut res = ResultCell::empty();

        drop(h1);

        println!("\n\n ------------ \n\n");

        let (r2, r3) = futures::join! {
            queue.result(h2, bus.clone()),
            queue.result(h3, bus.clone()),
        };

        assert_eq!((r2?, r3?), (Msg(14), Msg(15)));

        Ok(())
    }

    #[tokio::test]
    async fn test_queue_pending_dropped() -> Result<(), Error> {
        let bus = Bus::new();
        let wrapper = HandlerWrapper::new(Arc::new(Test {
            inner: 10,
            other: false,
        }));
        let queue = Queue::new(wrapper, 2);
        // let waker = waker_fn::waker_fn(move || {});
        // let cx = Context::from_waker(&waker);

        let poll = queue.poll_send(&mut MsgCell::new(Msg(1)), None, &bus);
        let h1 = match poll {
            Poll::Ready(Ok(task)) => task,
            _ => unreachable!(),
        };

        let poll = queue.poll_send(&mut MsgCell::new(Msg(2)), None, &bus);
        let h2 = match poll {
            Poll::Ready(Ok(task)) => task,
            _ => unreachable!(),
        };

        let poll = queue.poll_send(&mut MsgCell::new(Msg(3)), None, &bus);
        let h3 = match poll {
            Poll::Ready(Ok(task)) => task,
            _ => unreachable!(),
        };
        // let mut res = ResultCell::empty();

        drop(h2);

        let (r1, r3) = futures::join! {
            queue.result(h1, bus.clone()),
            queue.result(h3, bus.clone()),
        };

        assert_eq!((r1?, r3?), (Msg(13), Msg(15)));

        Ok(())
    }
}
