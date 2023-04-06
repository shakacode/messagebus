use std::sync::Arc;

use futures::Future;

use crate::{bus::Bus, cell::MsgCell, error::Error, message::Message};

pub trait Handler<M: Message>: Send + Sync {
    type Response: Message;
    type HandleFuture<'a>: Future<Output = Result<Self::Response, Error>> + Send + 'a
    where
        Self: 'a;

    type FlushFuture<'a>: Future<Output = Result<(), Error>> + Send + 'a
    where
        Self: 'a;

    type CloseFuture<'a>: Future<Output = Result<(), Error>> + Send + 'a
    where
        Self: 'a;

    fn handle(&self, msg: &mut MsgCell<M>, bus: &Bus) -> Self::HandleFuture<'_>;
    fn flush(&self, bus: &Bus) -> Self::FlushFuture<'_>;
    fn close(&self) -> Self::CloseFuture<'_>;
}

impl<M: Message, H: Handler<M> + 'static> Handler<M> for Arc<H> {
    type Response = H::Response;
    type HandleFuture<'a> = H::HandleFuture<'a>;
    type FlushFuture<'a> = H::FlushFuture<'a>;
    type CloseFuture<'a> = H::CloseFuture<'a>;

    #[inline]
    fn handle(&self, msg: &mut MsgCell<M>, bus: &Bus) -> Self::HandleFuture<'_> {
        (**self).handle(msg, bus)
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

pub trait MessageProducer<M: Message>: Send + Sync {
    type Message: Message;
    type Context: Send;

    type StartFuture<'a>: Future<Output = Result<Self::Context, Error>> + Send + 'a
    where
        Self: 'a;

    type NextFuture<'a>: Future<Output = Result<Self::Message, Error>> + Send + 'a
    where
        Self: 'a;

    type CloseFuture<'a>: Future<Output = Result<(), Error>> + Send + 'a
    where
        Self: 'a;

    fn start(&self, msg: &mut MsgCell<M>, bus: &Bus) -> Self::StartFuture<'_>;
    fn next<'a>(&'a self, ctx: &'a mut Self::Context, bus: &Bus) -> Self::NextFuture<'a>;
    fn close(&self, ctx: Self::Context) -> Self::CloseFuture<'_>;
}

impl<M: Message, H: MessageProducer<M> + 'static> MessageProducer<M> for Arc<H> {
    type Message = H::Message;
    type Context = H::Context;

    type StartFuture<'a> = H::StartFuture<'a>;
    type NextFuture<'a> = H::NextFuture<'a>;
    type CloseFuture<'a> = H::CloseFuture<'a>;

    #[inline]
    fn start(&self, msg: &mut MsgCell<M>, bus: &Bus) -> Self::StartFuture<'_> {
        (**self).start(msg, bus)
    }

    #[inline]
    fn next<'a>(&'a self, ctx: &'a mut Self::Context, bus: &Bus) -> Self::NextFuture<'a> {
        (**self).next(ctx, bus)
    }

    #[inline]
    fn close(&self, ctx: Self::Context) -> Self::CloseFuture<'_> {
        (**self).close(ctx)
    }
}
