use std::{
    marker::PhantomData,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{future::poll_fn, Future};
use smallvec::SmallVec;

use crate::{
    bus::{Bus, TaskHandler},
    cell::{MessageCell, MsgCell, ResultCell},
    error::Error,
    message::Message,
    type_tag::TypeTagQuery,
};

pub trait Receiver<M: Message, R: Message> {
    fn poll_send(
        &self,
        msg: &mut MsgCell<M>,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>>;

    fn poll_result(
        &self,
        task: &TaskHandler,
        res: Option<&mut ResultCell<R>>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>>;
}

pub trait ReceiverEx<M: Message, R: Message>: Receiver<M, R> {
    type SendFut<'a>: Future<Output = Result<TaskHandler, Error>> + 'a
    where
        Self: 'a;
    type RequestFut<'a>: Future<Output = Result<R, Error>> + 'a
    where
        Self: 'a;
    type ResultFut<'a>: Future<Output = Result<R, Error>> + 'a
    where
        Self: 'a;
    type ProcessFut<'a>: Future<Output = Result<(), Error>> + 'a
    where
        Self: 'a;

    fn try_send(&self, msg: &mut MsgCell<M>, bus: &Bus) -> Result<(), Error>;
    fn send(&self, msg: MsgCell<M>, bus: Bus) -> Self::SendFut<'_>;
    fn request(&self, msg: MsgCell<M>, bus: Bus) -> Self::RequestFut<'_>;
    fn process(&self, task: TaskHandler) -> Self::ProcessFut<'_>;
    fn result(&self, task: TaskHandler) -> Self::ResultFut<'_>;
}

impl<M: Message, R: Message, H: Receiver<M, R> + 'static> ReceiverEx<M, R> for H {
    type SendFut<'a> = impl Future<Output = Result<TaskHandler, Error>> + 'a;
    type RequestFut<'a> = impl Future<Output = Result<R, Error>> + 'a;
    type ResultFut<'a> = impl Future<Output = Result<R, Error>> + 'a;
    type ProcessFut<'a> = impl Future<Output = Result<(), Error>> + 'a;

    fn try_send(&self, cell: &mut MsgCell<M>, bus: &Bus) -> Result<(), Error> {
        match self.poll_send(cell, None, bus) {
            Poll::Ready(_) => Ok(()),
            Poll::Pending => Err(Error::TrySendError),
        }
    }

    fn send(&self, mut cell: MsgCell<M>, bus: Bus) -> Self::SendFut<'_> {
        poll_fn(move |cx| self.poll_send(&mut cell, Some(cx), &bus))
    }

    fn process(&self, task: TaskHandler) -> Self::ProcessFut<'_> {
        poll_fn(move |cx| self.poll_result(&task, None, cx))
    }

    fn result(&self, task: TaskHandler) -> Self::ResultFut<'_> {
        async move {
            let mut cell = ResultCell::empty();
            poll_fn(|cx| self.poll_result(&task, Some(&mut cell), cx)).await?;
            cell.unwrap()
        }
    }

    fn request(&self, cell: MsgCell<M>, bus: Bus) -> Self::RequestFut<'_> {
        async move {
            let task = self.send(cell, bus).await?;
            self.result(task).await
        }
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
        task: &TaskHandler,
        res: Option<&mut dyn MessageCell>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>>;
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
            _m: Default::default(),
        })
    }
}

pub struct Stub<M: Message, R: Message, H: Receiver<M, R>> {
    inner: H,
    _m: PhantomData<(M, R)>,
}

impl<M: Message, R: Message, H: Receiver<M, R> + Send + Sync + 'static> AbstractReceiver
    for Stub<M, R, H>
{
    #[inline]
    fn poll_send(
        &self,
        msg: &mut dyn MessageCell,
        cx: Option<&mut Context<'_>>,
        bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>> {
        self.inner.poll_send(msg.into_typed()?, cx, bus)
    }

    #[inline]
    fn message_types(&self) -> SmallVec<[(TypeTagQuery<'static>, TypeTagQuery<'static>); 1]> {
        SmallVec::from_const([(M::TYPE_TAG().info().into(), R::TYPE_TAG().info().into())])
    }

    #[inline]
    fn poll_result(
        &self,
        task: &TaskHandler,
        res: Option<&mut dyn MessageCell>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        let res = match res {
            Some(cell) => Some(cell.into_typed_result()?),
            None => None,
        };

        self.inner.poll_result(task, res, cx)
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
            Poll::Pending => Err(Error::TrySendError),
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
    pub async fn process(&self, task: TaskHandler) -> Result<(), Error> {
        poll_fn(|cx| self.poll_result(&task, None, cx)).await
    }

    #[inline]
    pub async fn result<R: Message>(&self, task: TaskHandler) -> Result<R, Error> {
        let mut cell = ResultCell::empty();
        poll_fn(|cx| self.poll_result(&task, Some(&mut cell), cx)).await?;
        cell.unwrap()
    }

    #[inline]
    pub async fn request<M: Message, R: Message>(&self, msg: M, bus: Bus) -> Result<R, Error> {
        let mut cell = MsgCell::new(msg);
        let task = self.send(&mut cell, bus).await?;
        self.result(task).await
    }
}
