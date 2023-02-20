use std::{
    future::poll_fn,
    marker::PhantomData,
    sync::Arc,
    task::{ready, Context, Poll, Waker},
};

use crossbeam::queue::ArrayQueue;
use futures::task::AtomicWaker;
use parking_lot::Mutex;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    bus::{Bus, TaskHandler},
    cell::{MsgCell, ResultCell},
    error::Error,
    message::Message,
    receiver::Receiver,
    wakelist::WakeList,
};

struct FreeIndexQueue {
    queue: ArrayQueue<usize>,
    wakelist: Mutex<WakeList>,
}

impl FreeIndexQueue {
    #[inline]
    fn push(&self, index: usize) {
        self.queue.push(index).unwrap();
        self.wakelist.lock().wake_all();
    }

    #[inline]
    fn pop(&self) -> Option<usize> {
        self.queue.pop()
    }

    #[inline]
    fn register(&self, waker: &Waker) {
        self.wakelist.lock().wake_all();
    }

    #[inline]
    fn new(count: usize) -> FreeIndexQueue {
        let queue = ArrayQueue::new(count);
        for i in 0..count {
            queue.push(i);
        }

        Self {
            queue,
            wakelist: Mutex::new(WakeList::new()),
        }
    }
}

struct SpawnerTaskState<M: Message, R: Message, T: Receiver<M, R> + Send + Sync + 'static> {
    task: TaskHandler,
    result: Option<ResultCell<R>>,
    _m: PhantomData<(M, R, T)>,
}

struct SpawnerTask<M: Message, R: Message, T: Receiver<M, R> + Send + Sync + 'static> {
    index: usize,
    inner: T,
    free_index_queue: Arc<FreeIndexQueue>,
    state: Mutex<Option<SpawnerTaskState<M, R, T>>>,
    result_waker: AtomicWaker,
    send_waker: AtomicWaker,
    _m: PhantomData<(M, R)>,
}

impl<M: Message, R: Message, T: Receiver<M, R> + Send + Sync + 'static> SpawnerTask<M, R, T> {
    fn new(index: usize, inner: T, free_index_queue: Arc<FreeIndexQueue>) -> SpawnerTask<M, R, T> {
        Self {
            index,
            inner,
            free_index_queue,
            state: Mutex::new(None),
            result_waker: AtomicWaker::new(),
            send_waker: AtomicWaker::new(),
            _m: PhantomData,
        }
    }

    fn send(&self, msg: &mut MsgCell<M>) {
        todo!()
    }

    fn poll_result(
        &self,
        resp: Option<&mut ResultCell<R>>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        Poll::Pending
    }
}
impl<M: Message, R: Message, T: Receiver<M, R> + Send + Sync + 'static> SpawnerTask<M, R, T> {
    #[inline]
    fn poll(&self, cx: &mut Context<'_>, bus: &Bus) -> Poll<Result<(), Error>> {
        self.send_waker.register(cx.waker());

        if let Some(state) = self.state.lock().as_mut() {
            let res =
                ready!(self
                    .inner
                    .poll_result(&mut state.task, state.result.as_mut(), cx, bus));

            self.free_index_queue.push(self.index);
            Poll::Ready(res)
        } else {
            Poll::Pending
        }
    }
}

struct SpawnerSlot<M: Message, R: Message, T: Receiver<M, R> + Send + Sync + 'static> {
    // handler: JoinHandle<()>,
    _m: PhantomData<(M, R, T)>,
}

impl<M: Message, R: Message, T: Receiver<M, R> + Send + Sync + 'static> SpawnerSlot<M, R, T> {
    fn new(slots: Arc<[SpawnerTask<M, R, T>]>, index: usize) -> Self {
        Self {
            // handler: tokio::spawn(poll_fn(move |cx| {
            //     if let Err(err) = ready!(slots[index].poll(cx)) {
            //         println!("error: {}", err);
            //     }

            //     Poll::Pending
            // })),
            _m: Default::default(),
        }
    }
}

pub struct Spawner<M: Message, R: Message, T: Receiver<M, R> + Send + Sync + 'static> {
    sender: async_channel::Sender<(TaskHandler, MsgCell<M>)>,
    receiver: mpsc::Receiver<(TaskHandler, Result<R, Error>)>,
    _m: PhantomData<T>,
}

impl<M: Message, R: Message, T: Receiver<M, R> + Clone + Send + Sync + 'static> Spawner<M, R, T> {
    pub fn new(inner: T, count: usize) -> Self {
        let (sender, rx) = async_channel::bounded(count);
        let (tx, receiver) = mpsc::channel(2);

        for i in 0..count {
            let rx = rx.clone();
            let inner = inner.clone();
            let tx = tx.clone();

            tokio::spawn(async move {
                while let Ok((task, msg)) = rx.recv().await {
                    // tx.send((task, inner.request(msg).await)).await.unwrap();
                }
            });
        }

        Self {
            sender,
            receiver,
            _m: Default::default(),
        }
    }
}

impl<M: Message, R: Message, T: Receiver<M, R> + Send + Sync + 'static> Receiver<M, R>
    for Spawner<M, R, T>
{
    #[inline]
    fn poll_send(
        &self,
        msg: &mut MsgCell<M>,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>> {
        // let Some(index) = self.free_index_queue.pop() else {
        //     if let Some(cx) = cx.as_ref() {
        //         self.send_wakelist.lock().push(cx.waker().clone());
        //     }

        //     return Poll::Pending;
        // };

        // self.start(msg);

        // Poll::Ready(Ok(index))

        Poll::Pending
    }

    #[inline]
    fn poll_result(
        &self,
        task: &mut TaskHandler,
        resp: Option<&mut ResultCell<R>>,
        cx: &mut Context<'_>,
        bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        // self.results
        //     .get(task)
        //     .ok_or(Error::ErrorPollWrongTask(*task))?
        //     .poll(resp, cx)
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {}
