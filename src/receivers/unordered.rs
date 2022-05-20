use std::{
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};

use crossbeam::atomic::AtomicCell;
use futures::{ready, stream::FuturesUnordered, Future, FutureExt};

use crate::{
    cell::{AnyCell, SharedCell},
    error::Error,
    handler::Handler,
    message::Message,
    permit::Permit,
    receiver::{Event, Receiver},
    wakelist::WakeList,
};

struct Fut<M: Message> {
    inner: M,
    resp: Option<SharedCell>,
}

pub struct Unordered<M: Message, T: Handler<M>> {
    inner: T,
    futur: FuturesUnordered<Fut<M>>,
    initializing: AtomicBool,
    init_future: AtomicCell<Option<Pin<Box<dyn Future<Output = Result<(), T::Error>>>>>>,
    flush_future: AtomicCell<Option<Pin<Box<dyn Future<Output = Result<(), T::Error>>>>>>,
    is_flushing: AtomicBool,
    flush_waitlist: WakeList,
}

impl<M: Message, T: Handler<M>> Receiver for Unordered<M, T> {
    // fn metadata(&self) -> ReceiverMetadata {
    //     self.inner.metadata()
    // }

    fn start_init(&self) -> Result<(), Error> {
        if self.initializing.load(Ordering::SeqCst) {
            return Ok(());
        }

        // self.init_future.store(Some(self.inner.init()));
        Ok(())
    }

    fn poll_initialized(&self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if let Some(fut) = self.init_future.swap(None) {
            match fut.poll_unpin(cx) {
                Poll::Ready(res) => Poll::Ready(res.map_err(|err| Error::handler_init_error(err))),
                Poll::Pending => {
                    self.init_future.store(Some(fut));
                    Poll::Pending
                }
            }
        } else {
            Poll::Ready(Err(Error::already_initialized()))
        }
    }

    // fn start_subscribe(&self, query: &MessageTypeQuery) -> Result<usize, Error> {
    //     Ok(())
    // }

    // fn poll_subscribed(&self, tok: usize, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
    //     Poll::Pending
    // }

    fn start_flush(&self) -> Result<(), Error> {
        let inner = self.inner.clone();
        self.flush_future
            .store(Some(Box::pin(async move { inner.flush().await })));

        Ok(())
    }

    fn poll_flushed(&self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.is_flushing.load(Ordering::SeqCst) {
            self.flush_waitlist.lock().push(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<Permit, Error>> {
        if self.is_flushing.load(Ordering::SeqCst) {
            Poll::Pending
        } else {
            Poll::Ready(Ok(self.root_permit.derive()))
        }
    }

    fn handle(&self, pmt: Permit, msg: AnyCell<'_>, resp: Option<SharedCell>) -> Result<(), Error> {
        let msg = msg.take::<M>().ok_or_else(|| Error::no_message())?;

        let lock = ready!(self.send_futur.poll_lock());
        lock.push(Fut { inner: msg, resp });

        self.inner.send_msg(pmt, msg, resp);

        Ok(())
    }

    fn poll_event(&self, cx: &mut Context<'_>) -> Poll<Result<Event, Error>> {
        let lock = ready!(self.poll_futur.poll_lock());
        let result = ready!(lock.poll_next_unpin());

        Poll::Pending
    }
}
