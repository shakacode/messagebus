use std::{
    marker::PhantomData,
    task::{Context, Poll},
};

use futures::Future;

use crate::{
    bus::Bus,
    cell::{MsgCell, ResultCell},
    error::Error,
    message::Message,
    receiver::Receiver,
    TaskHandler,
};

pub struct Spawner<M: Message, R: Message, T: Receiver<M, R> + Send + Sync + 'static> {
    _m: PhantomData<(M, R, T)>,
}

impl<M: Message, R: Message, T: Receiver<M, R> + Clone + Send + Sync + 'static> Spawner<M, R, T> {
    pub fn new(_inner: T) -> Self {
        Self {
            _m: Default::default(),
        }
    }
}

impl<M: Message, R: Message, T: Receiver<M, R> + Send + Sync + 'static> Receiver<M, R>
    for Spawner<M, R, T>
{
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

    #[inline]
    fn poll_send(
        &self,
        _msg: &mut MsgCell<M>,
        _cx: Option<&mut Context<'_>>,
        _bus: &Bus,
    ) -> Poll<Result<TaskHandler, Error>> {
        Poll::Pending
        // let cell = if let Some(cx) = cx {
        //     ready!(self.cell.poll_lock(cx))
        // } else {
        //     self.cell.try_lock()
        // };

        // cell.put(msg);
        // self.notifier.notify_one();

        // Poll::Ready(Ok(TaskHandler::new(data, 0, drop)))
    }

    #[inline]
    fn poll_result(
        &self,
        _task: &mut TaskHandler,
        _resp: &mut ResultCell<R>,
        _cx: &mut Context<'_>,
        _bus: &Bus,
    ) -> Poll<Result<(), Error>> {
        // let res = ready!(self.output.poll_lock());
        // if resp.id == task.index() {
        //     resp.put(res.take_result());
        //     Poll::Ready(Ok(()))
        // } else {
        Poll::Pending
        // }
    }
}

#[cfg(test)]
mod tests {}
