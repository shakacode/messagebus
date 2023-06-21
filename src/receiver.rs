use std::{
    any::TypeId,
    marker::PhantomData,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    task::{ready, Context, Poll},
};

use futures::{future::poll_fn, Future};
use parking_lot::Mutex;
use smallvec::SmallVec;

use crate::{
    bus::Bus,
    cell::{MessageCell, MsgCell, ResultCell},
    error::{Error, ErrorKind},
    message::Message,
    type_tag::TypeTagQuery,
    utils::future_cell::{FutureCell, SyncFutureCell},
    TaskHandler,
};

#[allow(non_snake_case)]
pub mod STATE {
    pub const UNINIT: u8 = 0;
    pub const INITIALIZING: u8 = 1;
    pub const NORMAL: u8 = 2;
    pub const FLUSHING: u8 = 3;
    pub const CLOSING: u8 = 4;
    pub const CLOSED: u8 = 5;
    pub const FAILED: u8 = 10;
}

pub trait Receiver<M: Message, R: Message>: Send + Sync {
    type InitFuture<'a>: Future<Output = Result<(), Error>> + Send + 'a
    where
        Self: 'a;

    type CloseFuture<'a>: Future<Output = Result<(), Error>> + Send + 'a
    where
        Self: 'a;

    type FlushFuture<'a>: Future<Output = Result<(), Error>> + Send + 'a
    where
        Self: 'a;

    fn init(&self, bus: &Bus) -> Self::InitFuture<'_>;
    fn flush(&self, bus: &Bus) -> Self::FlushFuture<'_>;
    fn close(&self) -> Self::CloseFuture<'_>;

    fn poll_send(
        &self,
        msg: &mut MsgCell<M>,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>>;

    fn poll_result(
        &self,
        task: &mut TaskHandler,
        res: &mut ResultCell<R>,
        cx: &mut Context<'_>,
        bus: &Bus,
    ) -> Poll<Result<(), Error>>;
}

pub trait ReceiverEx<M: Message, R: Message>: Receiver<M, R> {
    type SendFut<'a>: Future<Output = Result<TaskHandler, Error>> + Send + 'a
    where
        Self: 'a;

    type RequestFut<'a>: Future<Output = Result<R, Error>> + Send + 'a
    where
        Self: 'a;

    type ResultFut<'a>: Future<Output = Result<R, Error>> + Send + 'a
    where
        Self: 'a;

    type ProcessFut<'a>: Future<Output = Result<(), Error>> + Send + 'a
    where
        Self: 'a;

    fn try_send(&self, msg: &mut MsgCell<M>, bus: &Bus) -> Result<TaskHandler, Error>;
    fn send(&self, msg: MsgCell<M>, bus: Bus) -> Self::SendFut<'_>;
    fn request(&self, msg: MsgCell<M>, bus: Bus) -> Self::RequestFut<'_>;
    fn process(&self, task: TaskHandler, bus: Bus) -> Self::ProcessFut<'_>;
    fn result(&self, task: TaskHandler, bus: Bus) -> Self::ResultFut<'_>;
}

impl<M: Message, R: Message, H: Receiver<M, R> + Send + Sync + 'static> ReceiverEx<M, R> for H {
    type SendFut<'a> = impl Future<Output = Result<TaskHandler, Error>> + Send + 'a;
    type RequestFut<'a> = impl Future<Output = Result<R, Error>> + Send + 'a;
    type ResultFut<'a> = impl Future<Output = Result<R, Error>> + Send + 'a;
    type ProcessFut<'a> = impl Future<Output = Result<(), Error>> + Send + 'a;

    fn try_send(&self, cell: &mut MsgCell<M>, bus: &Bus) -> Result<TaskHandler, Error> {
        match self.poll_send(cell, None, bus) {
            Poll::Ready(handler) => handler,
            Poll::Pending => Err(ErrorKind::TrySendError.into()),
        }
    }

    fn send(&self, mut cell: MsgCell<M>, bus: Bus) -> Self::SendFut<'_> {
        poll_fn(move |cx| self.poll_send(&mut cell, Some(cx), &bus))
    }

    fn process(&self, task: TaskHandler, bus: Bus) -> Self::ProcessFut<'_> {
        async move {
            let result = self.result(task, bus.clone()).await?;
            if TypeId::of::<R>() != TypeId::of::<()>() {
                bus.send(result).await?;
            }

            Ok(())
            //  {
            //     Ok(_) => {
            //         Ok(())
            //     }
            //     Err(err) => {
            //         task.finish();
            //         match err {
            //             Error::ProducerFinished => Err(Error::ProducerFinished),
            //             err => {
            //                 println!(
            //                     "[{}]: unhandled error result message of type `{}`: {}",
            //                     std::any::type_name::<T>(),
            //                     std::any::type_name::<T::Message>(),
            //                     err
            //                 );
            //                 Ok(())
            //             }
            //         }
            //     }
            // }
        }
    }

    fn result(&self, mut task: TaskHandler, bus: Bus) -> Self::ResultFut<'_> {
        async move {
            let mut cell = ResultCell::empty();
            poll_fn(|cx| self.poll_result(&mut task, &mut cell, cx, &bus)).await?;
            cell.unwrap()
        }
    }

    fn request(&self, cell: MsgCell<M>, bus: Bus) -> Self::RequestFut<'_> {
        async move {
            let task = self.send(cell, bus.clone()).await?;
            self.result(task, bus).await
        }
    }
}

pub trait AbstractReceiver: Send + Sync + 'static {
    fn message_types(&self) -> SmallVec<[(TypeTagQuery<'static>, TypeTagQuery<'static>); 1]>;
    fn state(&self) -> u8;

    fn poll_send(
        &self,
        msg: &mut dyn MessageCell,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>>;

    fn poll_result(
        &self,
        task: &mut TaskHandler,
        res: Option<&mut dyn MessageCell>,
        cx: &mut Context<'_>,
        bus: &Bus,
    ) -> Poll<Result<(), Error>>;

    fn poll_init(&self, cx: &mut Context<'_>, bus: &Bus) -> Poll<Result<(), Error>>;
    fn poll_flush(&self, cx: &mut Context<'_>, bus: &Bus) -> Poll<Result<(), Error>>;
    fn poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Error>>;
}

pub trait IntoAbstractReceiver<M: Message, R: Message> {
    fn into_abstract_arc(self) -> Arc<dyn AbstractReceiver>
    where
        Self: Sized + 'static;
}

impl<M: Message, R: Message, H: Receiver<M, R> + Send + Sync + 'static> IntoAbstractReceiver<M, R>
    for H
{
    fn into_abstract_arc(self) -> Arc<dyn AbstractReceiver>
    where
        Self: Sized + 'static,
    {
        Arc::new(Stub {
            inner: self,
            sending_fut: Mutex::new(FutureCell::new()),
            initializing_fut: SyncFutureCell::new(),
            closing_fut: SyncFutureCell::new(),
            flushing_fut: SyncFutureCell::new(),
            state: AtomicU8::new(STATE::UNINIT),
            _m: Default::default(),
        })
    }
}

type SendFuture<M: Message, R: Message, T: Receiver<M, R> + 'static> =
    impl Future<Output = Result<(), Error>> + Send;

pub struct Stub<M: Message, R: Message, H: Receiver<M, R> + 'static> {
    inner: H,
    sending_fut: Mutex<FutureCell<SendFuture<M, R, H>>>,
    initializing_fut: SyncFutureCell<H::InitFuture<'static>>,
    closing_fut: SyncFutureCell<H::CloseFuture<'static>>,
    flushing_fut: SyncFutureCell<H::FlushFuture<'static>>,
    state: AtomicU8,
    _m: PhantomData<(M, R)>,
}

impl<M: Message, R: Message, H: Receiver<M, R> + Send + Sync + 'static> AbstractReceiver
    for Stub<M, R, H>
{
    #[inline]
    fn message_types(&self) -> SmallVec<[(TypeTagQuery<'static>, TypeTagQuery<'static>); 1]> {
        SmallVec::from_const([(M::TYPE_TAG().info().into(), R::TYPE_TAG().info().into())])
    }

    #[inline]
    fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    #[inline]
    fn poll_send(
        &self,
        msg: &mut dyn MessageCell,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>> {
        if self.state.load(Ordering::Acquire) != STATE::NORMAL {
            return Poll::Ready(Err(ErrorKind::BusClosed.into()));
        }

        self.inner.poll_send(msg.into_typed()?, cx, bus)
    }

    #[inline]
    fn poll_result(
        &self,
        task: &mut TaskHandler,
        res: Option<&mut dyn MessageCell>,
        cx: &mut Context<'_>,
        bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        let st = self.state.load(Ordering::Acquire);
        if st != STATE::FLUSHING && st != STATE::NORMAL {
            return Poll::Ready(Err(ErrorKind::BusClosed.into()));
        }

        loop {
            if let Some(poll) = self.sending_fut.lock().try_poll_unpin(cx) {
                let _ = ready!(poll);
                if task.is_finished() {
                    break Poll::Ready(Ok(()));
                }
            }

            break match res {
                Some(cell) => self
                    .inner
                    .poll_result(task, cell.into_typed_result()?, cx, bus),
                None => {
                    let mut res = ResultCell::empty();
                    match self.inner.poll_result(task, &mut res, cx, bus) {
                        Poll::Ready(Ok(_)) => match res.take() {
                            Some(Ok(msg)) => {
                                let bus = bus.clone();
                                self.sending_fut
                                    .lock()
                                    .set(async move { bus.send(msg).await });
                                continue;
                            }
                            Some(Err(err)) => Poll::Ready(Err(err)),
                            _ => Poll::Ready(Ok(())),
                        },
                        Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                        Poll::Pending => Poll::Pending,
                    }
                }
            };
        }
    }

    fn poll_init(&self, cx: &mut Context<'_>, bus: &Bus) -> Poll<Result<(), Error>> {
        match self.state.compare_exchange(
            STATE::UNINIT,
            STATE::INITIALIZING,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => {
                self.initializing_fut
                    .set(unsafe { std::mem::transmute(self.inner.init(bus)) });
            }
            Err(STATE::INITIALIZING) => (),
            _ => return Poll::Ready(Err(ErrorKind::AlreadyInitialized.into())),
        }

        match ready!(self.initializing_fut.poll_unpin(cx)) {
            Ok(_) => {
                self.state.store(STATE::NORMAL, Ordering::Release);
                Poll::Ready(Ok(()))
            }
            Err(err) => {
                self.state.store(STATE::FAILED, Ordering::Release);
                Poll::Ready(Err(err))
            }
        }
    }

    fn poll_flush(&self, cx: &mut Context<'_>, bus: &Bus) -> Poll<Result<(), Error>> {
        match self.state.compare_exchange(
            STATE::NORMAL,
            STATE::FLUSHING,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => {
                self.flushing_fut
                    .set(unsafe { std::mem::transmute(self.inner.flush(bus)) });
            }
            Err(STATE::FLUSHING) => (),
            _ => return Poll::Ready(Err(ErrorKind::BusClosed.into())),
        }

        let res = ready!(self.flushing_fut.poll_unpin(cx));
        self.state.store(STATE::NORMAL, Ordering::Release);
        Poll::Ready(res)
    }

    fn poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.state.load(Ordering::Acquire) != STATE::CLOSED {
            self.state.store(STATE::CLOSING, Ordering::Release);
            self.closing_fut
                .set(unsafe { std::mem::transmute(self.inner.close()) });

            let res = ready!(self.closing_fut.poll_unpin(cx));
            self.state.store(STATE::CLOSED, Ordering::Release);

            Poll::Ready(res)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl dyn AbstractReceiver {
    #[inline]
    pub async fn initialize(&self, bus: &Bus) -> Result<(), Error> {
        poll_fn(move |cx| self.poll_init(cx, bus)).await
    }

    #[inline]
    pub fn try_send<M: Message>(&self, msg: M, bus: &Bus) -> Result<TaskHandler, Error> {
        let mut cell = MsgCell::new(msg);
        self.try_send_dyn(&mut cell, bus)
    }

    #[inline]
    pub fn try_send_dyn(
        &self,
        cell: &mut dyn MessageCell,
        bus: &Bus,
    ) -> Result<TaskHandler, Error> {
        match self.poll_send(cell, None, bus) {
            Poll::Ready(task) => task,
            Poll::Pending => Err(ErrorKind::TrySendError.into()),
        }
    }

    #[inline]
    pub async fn send<M: Message>(
        &self,
        msg: &mut MsgCell<M>,
        bus: Bus,
    ) -> Result<TaskHandler, Error> {
        self.send_dyn(msg, bus).await
    }

    #[inline]
    pub async fn send_msg<M: Message>(&self, msg: M, bus: Bus) -> Result<TaskHandler, Error> {
        let mut cell = MsgCell::new(msg);
        self.send_dyn(&mut cell, bus).await
    }

    #[inline]
    pub async fn send_dyn(
        &self,
        msg: &mut dyn MessageCell,
        bus: Bus,
    ) -> Result<TaskHandler, Error> {
        poll_fn(move |cx| self.poll_send(msg, Some(cx), &bus)).await
    }

    #[inline]
    pub async fn process(&self, mut task: TaskHandler, bus: Bus) -> Result<(), Error> {
        poll_fn(|cx| self.poll_result(&mut task, None, cx, &bus)).await
    }

    #[inline]
    pub async fn result<R: Message>(&self, mut task: TaskHandler, bus: Bus) -> Result<R, Error> {
        let mut cell = ResultCell::empty();
        poll_fn(|cx| self.poll_result(&mut task, Some(&mut cell), cx, &bus)).await?;
        cell.unwrap()
    }

    #[inline]
    pub async fn request<M: Message, R: Message>(&self, msg: M, bus: Bus) -> Result<R, Error> {
        let mut cell = MsgCell::new(msg);
        let task = self.send(&mut cell, bus.clone()).await?;
        self.result(task, bus).await
    }

    #[inline]
    pub async fn flush(&self, bus: Bus) -> Result<(), Error> {
        poll_fn(move |cx| self.poll_flush(cx, &bus)).await
    }

    #[inline]
    pub async fn close(&self) -> Result<(), Error> {
        poll_fn(move |cx| self.poll_close(cx)).await
    }
}

impl<M: Message, R: Message, T: Receiver<M, R> + 'static> Receiver<M, R> for Arc<T> {
    type InitFuture<'a> = T::InitFuture<'a>;
    type CloseFuture<'a> = T::CloseFuture<'a>;
    type FlushFuture<'a> = T::FlushFuture<'a>;

    #[inline]
    fn poll_send(
        &self,
        msg: &mut MsgCell<M>,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>> {
        (**self).poll_send(msg, cx, bus)
    }

    #[inline]
    fn poll_result(
        &self,
        task: &mut TaskHandler,
        res: &mut ResultCell<R>,
        cx: &mut Context<'_>,
        bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        (**self).poll_result(task, res, cx, bus)
    }

    #[inline]
    fn init(&self, bus: &Bus) -> Self::InitFuture<'_> {
        (**self).init(bus)
    }

    #[inline]
    fn flush(&self, bus: &Bus) -> Self::FlushFuture<'_> {
        (**self).flush(bus)
    }

    #[inline]
    fn close(&self) -> Self::CloseFuture<'_> {
        (**self).close()
    }
}
