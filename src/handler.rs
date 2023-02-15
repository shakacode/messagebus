use futures::Future;

use crate::{bus::Bus, cell::MsgCell, error::Error, message::Message};

pub trait Handler<M: Message> {
    type Response: Message;
    type HandleFuture<'a>: Future<Output = Result<Self::Response, Error>> + Send + 'a
    where
        Self: 'a;

    type FlushFuture<'a>: Future<Output = Result<(), Error>> + Send + 'a
    where
        Self: 'a;

    fn handle(&self, msg: &mut MsgCell<M>, bus: &Bus) -> Self::HandleFuture<'_>;
    fn flush(&mut self, bus: &Bus) -> Self::FlushFuture<'_>;
}

pub trait MessageProvider {
    type Message: Message;

    type NextFuture<'a>: Future<Output = Result<Self::Message, Error>> + Send + 'a
    where
        Self: 'a;

    fn next(&self, bus: &Bus) -> Self::NextFuture<'_>;
}
