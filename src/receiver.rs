use std::{
    any::TypeId,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
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
    utils::future_cell::FutureCell,
    TaskHandler,
};

pub trait Receiver<M: Message, R: Message>: Send + Sync {
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

    fn poll_flush(&self, cx: &mut Context<'_>, bus: &Bus) -> Poll<Result<(), Error>>;
    fn poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Error>>;
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
    type FlushFut<'a>: Future<Output = Result<(), Error>> + Send + 'a
    where
        Self: 'a;
    type CloseFut<'a>: Future<Output = Result<(), Error>> + Send + 'a
    where
        Self: 'a;

    fn try_send(&self, msg: &mut MsgCell<M>, bus: &Bus) -> Result<TaskHandler, Error>;
    fn send(&self, msg: MsgCell<M>, bus: Bus) -> Self::SendFut<'_>;
    fn request(&self, msg: MsgCell<M>, bus: Bus) -> Self::RequestFut<'_>;
    fn process(&self, task: TaskHandler, bus: Bus) -> Self::ProcessFut<'_>;
    fn result(&self, task: TaskHandler, bus: Bus) -> Self::ResultFut<'_>;
    fn flush(&self, bus: Bus) -> Self::FlushFut<'_>;
    fn close(&self) -> Self::CloseFut<'_>;
}

impl<M: Message, R: Message, H: Receiver<M, R> + Send + Sync + 'static> ReceiverEx<M, R> for H {
    type SendFut<'a> = impl Future<Output = Result<TaskHandler, Error>> + Send + 'a;
    type RequestFut<'a> = impl Future<Output = Result<R, Error>> + Send + 'a;
    type ResultFut<'a> = impl Future<Output = Result<R, Error>> + Send + 'a;
    type ProcessFut<'a> = impl Future<Output = Result<(), Error>> + Send + 'a;
    type FlushFut<'a> = impl Future<Output = Result<(), Error>> + Send + 'a;
    type CloseFut<'a> = impl Future<Output = Result<(), Error>> + Send + 'a;

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

    fn flush(&self, bus: Bus) -> Self::FlushFut<'_> {
        poll_fn(move |cx| self.poll_flush(cx, &bus))
    }

    fn close(&self) -> Self::CloseFut<'_> {
        poll_fn(move |cx| self.poll_close(cx))
    }
}

pub trait AbstractReceiver: Send + Sync + 'static {
    fn message_types(&self) -> SmallVec<[(TypeTagQuery<'static>, TypeTagQuery<'static>); 1]>;

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
            closed: AtomicBool::new(false),
            _m: Default::default(),
        })
    }
}

type SendFuture<M: Message, R: Message, T: Receiver<M, R> + 'static> =
    impl Future<Output = Result<(), Error>> + Send;

pub struct Stub<M: Message, R: Message, H: Receiver<M, R> + 'static> {
    inner: H,
    sending_fut: Mutex<FutureCell<SendFuture<M, R, H>>>,
    closed: AtomicBool,
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
    fn poll_send(
        &self,
        msg: &mut dyn MessageCell,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>> {
        if self.closed.load(Ordering::Relaxed) {
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
        if self.closed.load(Ordering::Acquire) {
            return Poll::Ready(Err(ErrorKind::BusClosed.into()));
        }

        loop {
            if let Some(poll) = self.sending_fut.lock().try_poll_unpin(cx) {
                let _ = ready!(poll);
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

    fn poll_flush(&self, cx: &mut Context<'_>, bus: &Bus) -> Poll<Result<(), Error>> {
        self.inner.poll_flush(cx, bus)
    }

    fn poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.closed.store(true, Ordering::Release);
        self.inner.poll_close(cx)
    }
}

impl dyn AbstractReceiver {
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
    fn poll_flush(&self, cx: &mut Context<'_>, bus: &Bus) -> Poll<Result<(), Error>> {
        (**self).poll_flush(cx, bus)
    }

    #[inline]
    fn poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        (**self).poll_close(cx)
    }
}
