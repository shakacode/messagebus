use std::{
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{future::poll_fn, ready, Future};

use crate::{
    cell::{MessageCell, SharedCell},
    error::Error,
    message::Message,
    permit::Permit,
    type_tag::TypeTag,
};

pub enum Event {
    Completed(u32),
}

pub trait Receiver {
    fn message_types(&self, cb: &mut dyn FnMut(TypeTag, Option<TypeTag>) -> ControlFlow<()>);
    // fn metadata(&self) -> ReceiverMetadata;

    // fn start_init(&self) -> Result<(), Error>;
    // fn poll_initialized(&self, cx: &mut Context<'_>) -> Poll<Result<(), Error>>;

    // fn start_destroy(&self) -> Result<(), Error>;
    // fn poll_destroyed(&self, cx: &mut Context<'_>) -> Poll<Result<(), Error>>;

    // fn start_subscribe(&self, query: &MessageTypeQuery) -> Result<usize, Error>;
    // fn poll_subscribed(&self, tok: usize, cx: &mut Context<'_>) -> Poll<Result<(), Error>>;

    // fn start_flush(&self) -> Result<(), Error>;
    // fn poll_flushed(&self, ctx: &mut Context<'_>) -> Poll<Result<(), Error>>;

    fn poll_ready(&self, cx: Option<&mut Context<'_>>) -> Poll<Result<Permit, Error>>;
    fn handle(
        &self,
        pmt: Permit,
        msg: &mut dyn MessageCell,
        resp: Option<SharedCell>,
    ) -> Result<(), Error>;

    fn poll_event(self: Pin<&Self>, cx: &mut Context<'_>) -> Poll<Result<Event, Error>>;
}

pub trait ReceiverEx {
    type SendFuture<'a, 'c, M>: Future<Output = Result<(), Error>>
    where
        M: Message + 'a + 'c,
        Self: 'a + 'c;

    fn send<'c, M: Message>(
        &self,
        msg: &'c mut dyn MessageCell,
        resp: Option<SharedCell>,
    ) -> Self::SendFuture<'_, 'c, M>;
}

impl<T: Receiver> ReceiverEx for T {
    type SendFuture<'a, 'c, M> = impl Future<Output = Result<(), Error>>
    where
        M: Message + 'a + 'c,
        Self: 'a + 'c;

    fn send<'c, M: Message>(
        &self,
        msg: &'c mut dyn MessageCell,
        mut resp: Option<SharedCell>,
    ) -> Self::SendFuture<'_, 'c, M> {
        async move {
            poll_fn(move |cx| {
                let pmt = match ready!(self.poll_ready(Some(cx))) {
                    Ok(pmt) => pmt,
                    Err(err) => return Poll::Ready(Err(err)),
                };

                Poll::Ready(self.handle(pmt, msg, resp.take()))
            })
            .await
        }
    }
}

impl dyn Receiver {
    pub fn try_send<M: Message>(
        &self,
        msg: &mut dyn MessageCell,
        resp: Option<SharedCell>,
    ) -> Result<(), Error> {
        match self.poll_ready(None) {
            Poll::Ready(pmt) => self.handle(pmt?, msg, resp),
            Poll::Pending => Err(Error::TrySendError),
        }
    }

    pub async fn send<M: Message>(
        &self,
        msg: &mut dyn MessageCell,
        mut resp: Option<SharedCell>,
    ) -> Result<(), Error> {
        poll_fn(move |cx| {
            let pmt = match ready!(self.poll_ready(Some(cx))) {
                Ok(pmt) => pmt,
                Err(err) => return Poll::Ready(Err(err)),
            };

            Poll::Ready(self.handle(pmt, msg, resp.take()))
        })
        .await
    }
}
