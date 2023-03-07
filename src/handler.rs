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
    fn flush(&mut self, bus: &Bus) -> Self::FlushFuture<'_>;
    fn close(&mut self) -> Self::CloseFuture<'_>;
}

pub trait MessageProducer<M: Message>: Send + Sync {
    type Message: Message;

    type StartFuture<'a>: Future<Output = Result<(), Error>> + Send + 'a
    where
        Self: 'a;

    type NextFuture<'a>: Future<Output = Result<Self::Message, Error>> + Send + 'a
    where
        Self: 'a;

    type CloseFuture<'a>: Future<Output = Result<(), Error>> + Send + 'a
    where
        Self: 'a;

    fn start(&self, msg: &mut MsgCell<M>, bus: &Bus) -> Self::StartFuture<'_>;
    fn next(&self, bus: &Bus) -> Self::NextFuture<'_>;
    fn close(&mut self) -> Self::CloseFuture<'_>;
}
