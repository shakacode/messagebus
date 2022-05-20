use futures::Future;

use crate::{error::Error, message::Message};

pub trait Handler<M: Message> {
    type Response: Message;
    type HandleFuture<'a>: Future<Output = Result<Self::Response, Error>> + 'a
    where
        Self: 'a;

    // type InitFuture<'a>: Future<Output = Result<(), Self::Error>> + 'a
    // where
    //     Self: 'a;

    // type FlushFuture<'a>: Future<Output = Result<(), Self::Error>> + 'a
    // where
    //     Self: 'a;

    fn handle(&self, msg: M) -> Self::HandleFuture<'_>;

    // fn init(&mut self) -> Self::InitFuture<'_>;
    // fn flush(&mut self) -> Self::FlushFuture<'_>;
}

pub trait HandlerMut<M: Message> {
    type Response: Message;
    type HandleFuture<'a>: Future<Output = Result<Self::Response, Error>> + 'a
    where
        Self: 'a;

    // type InitFuture<'a>: Future<Output = Result<(), Self::Error>> + 'a
    // where
    //     Self: 'a;

    // type FlushFuture<'a>: Future<Output = Result<(), Self::Error>> + 'a
    // where
    //     Self: 'a;

    fn handle(&mut self, msg: M) -> Self::HandleFuture<'_>;

    // fn init(&mut self) -> Self::InitFuture<'_>;
    // fn flush(&mut self) -> Self::FlushFuture<'_>;
}
