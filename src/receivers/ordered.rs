use std::{
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    task::{ready, Context, Poll},
};

use futures::{stream::FuturesOrdered, Future, StreamExt};
use parking_lot::Mutex;

use crate::{
    cell::{MsgCell, ResultCell},
    error::{Error, ErrorKind},
    message::Message,
    receiver::{Receiver, ReceiverEx},
    Bus, TaskHandler, utils::wakelist::WakeList,
};

type Fut<M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> =
    impl Future<Output = (u64, Result<R, Error>)> + Send;

pub struct Ordered<M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> {
    capacity: usize,
    inner: Arc<T>,
    ordered: Mutex<FuturesOrdered<Fut<M, R, T>>>,
    sequence: AtomicU64,
    pending: Mutex<Option<(u64, Result<R, Error>)>>,
    pending_wakelist: WakeList,
}

impl<'a, M: Message, R: Message, T: Receiver<M, R> + Clone + 'static> Ordered<M, R, T> {
    pub fn new(inner: T, capacity: usize) -> Self {
        Self {
            capacity,
            inner: Arc::new(inner),
            ordered: Mutex::new(FuturesOrdered::new()),
            sequence: AtomicU64::new(1),
            pending: Mutex::new(None),
            pending_wakelist: WakeList::new()
        }
    }

    #[inline]
    fn create_task(&self, index: u64) -> TaskHandler {
        TaskHandler::new(self.inner.clone(), index, |_, _| {})
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
        let bus = bus.clone();
        let msg = msg.take_cell();
        let inner = self.inner.clone();
        let index = self.sequence.fetch_add(1, Ordering::Relaxed);

        self.ordered
            .lock()
            .push_back(async move { (index, inner.request(msg, bus).await) });

        Poll::Ready(Ok(self.create_task(index)))
    }

    fn poll_result(
        &self,
        task: &mut TaskHandler,
        res: &mut ResultCell<R>,
        cx: &mut Context<'_>,
        _bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        if !std::ptr::eq(task.data_ptr(), Arc::as_ptr(&self.inner) as *const ()) {
            return Poll::Ready(Err(ErrorKind::ErrorPollWrongTask(String::from(
                "ptr not eq",
            ))
            .into()));
        }

        println!("poll {}", task.index());

        let pend_lock = &mut *self.pending.lock();
        if let Some((idx, ..)) = *pend_lock { 
            println!("xxx");
            return if idx != task.index() {
                self.pending_wakelist.register(cx.waker());
                Poll::Pending
            } else {
                let Some((_, r)) = pend_lock.take() else { unreachable!()};
                res.put(r);
                self.pending_wakelist.wake_all();
                Poll::Ready(Ok(()))
            }
        }

        let Some((idx, r)) = ready!(self.ordered.lock().poll_next_unpin(cx)) else {
           return Poll::Pending; 
        };

        println!("yyy {}", idx);

        if task.index() == idx {
            
            res.put(r);
            self.pending_wakelist.wake_all();
            println!("xxxxx");
            Poll::Ready(Ok(()))
        } else {
            self.pending.lock().replace((idx, r));
            self.pending_wakelist.register(cx.waker());
            Poll::Pending
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
        Future, TryStreamExt, join,
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

        println!("1111");

        let (a, b, c, d) = join!(
            un.result(h1, bus.clone()),
            un.result(h2, bus.clone()),
            un.result(h3, bus.clone()),
            un.result(h4, bus.clone()),
        );

        println!("{:?} {:?} {:?} {:?}", a,b,c,d)       ;

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
