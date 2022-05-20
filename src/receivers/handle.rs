use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use crossbeam::atomic::AtomicCell;
use futures::{ready, Future, FutureExt};

use crate::{
    cell::{AnyCell, SharedCell},
    error::Error,
    handler::Handler,
    message::{ErrorMessage, Message},
    permit::Permit,
    receiver::{Event, Receiver},
};

mod state {
    pub const UNINIT: usize = 0;
    pub const INITIALIZING: usize = 1;
    pub const IDLE: usize = 2;
    pub const CLOSED: usize = 3;
    pub const HANDLING: usize = 4;

    pub const FLUSHING_HANDLING: usize = 5;
    pub const FLUSHING: usize = 6;

    pub const CLOSING_HANDLING: usize = 7;
    pub const CLOSING: usize = 8;
}

pub struct Handle<M: Message, T: Handler<M>> {
    inner: Arc<T>,
    state: AtomicUsize,
    response_cell: AtomicCell<Option<SharedCell>>,
    init_fut: AtomicCell<Option<Pin<Box<dyn Future<Output = Result<(), T::Error>>>>>>,
    flush_fut: AtomicCell<Option<Pin<Box<dyn Future<Output = Result<(), T::Error>>>>>>,
    handle_fut: AtomicCell<Option<Pin<Box<dyn Future<Output = Result<T::Response, T::Error>>>>>>,
}

impl<M: Message, T: Handler<M> + 'static> Receiver for Handle<M, T> {
    // fn metadata(&self) -> ReceiverMetadata {
    //     self.inner.metadata()
    // }

    fn start_init(&self) -> Result<(), Error> {
        match self.state.compare_exchange(
            state::UNINIT,
            state::INITIALIZING,
            Ordering::Acquire,
            Ordering::Release,
        ) {
            Ok(_) => {
                let inner = self.inner.clone();

                self.init_fut
                    .swap(Some(Box::pin(async move { inner.init().await })));

                Ok(())
            }
            Err(1) => Ok(()),
            Err(_) => Err(Error::AlreadyInitialized),
        }
    }

    fn poll_initialized(&self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match self.state.load(Ordering::SeqCst) {
            state::UNINIT => {
                return Poll::Ready(Err(Error::InternalError(
                    "`poll_initialized` called before `start_init`",
                )))
            }
            state::INITIALIZING => (),
            _ => return Poll::Ready(Ok(())),
        }

        if let Some(fut) = self.init_fut.swap(None) {
            match fut.poll_unpin(cx) {
                Poll::Ready(res) => Poll::Ready(res.map_err(|err| Error::handler_init_error(err))),
                Poll::Pending => {
                    self.init_fut.store(Some(fut));
                    Poll::Pending
                }
            }
        } else {
            cx.waker().wake_by_ref();

            Poll::Pending
        }
    }

    // fn start_subscribe(&self, query: &MessageTypeQuery) -> Result<usize, Error> {
    //     Ok(())
    // }

    // fn poll_subscribed(&self, tok: usize, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
    //     Poll::Pending
    // }

    fn start_flush(&self) -> Result<(), Error> {
        loop {
            match self.state.compare_exchange(
                state::IDLE,
                state::FLUSHING,
                Ordering::Acquire,
                Ordering::Release,
            ) {
                Ok(_) => {
                    let inner = self.inner.clone();

                    self.flush_fut
                        .swap(Some(Box::pin(async move { inner.flush().await })));

                    break Ok(());
                }
                Err(state::UNINIT | state::INITIALIZING) => {
                    break Err(Error::InternalError("`start_flush` called before init"));
                }
                Err(state::CLOSED | state::CLOSING | state::CLOSING_HANDLING) => {
                    break Err(Error::InternalError("`start_flush` called after close"));
                }
                Err(state::FLUSHING | state::FLUSHING_HANDLING) => break Ok(()),
                Err(state::HANDLING) => {
                    match self.state.compare_exchange(
                        state::HANDLING,
                        state::FLUSHING_HANDLING,
                        Ordering::Acquire,
                        Ordering::Release,
                    ) {
                        Ok(_) => break Ok(()),
                        Err(_) => continue,
                    }
                }
            }
        }
    }

    fn poll_flushed(&self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match self.state.load(Ordering::SeqCst) {
            state::FLUSHING_HANDLING => {
                self.wakelist.lock().push(cx.waker().clone());
                Poll::Pending
            }
            state::FLUSHING => {
                if let Some(fut) = self.flush_fut.swap(None) {
                    match fut.poll_unpin(cx) {
                        Poll::Ready(res) => {
                            Poll::Ready(res.map_err(|err| Error::handler_init_error(err)))
                        }
                        Poll::Pending => {
                            self.flush_fut.store(Some(fut));
                            self.wakelist.lock().wake_all();
                            Poll::Pending
                        }
                    }
                } else {
                    self.wakelist.lock().push(cx.waker().clone());
                    Poll::Pending
                }
            }
            _ => Poll::Ready(Ok(())),
        }
    }

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<Permit, Error>> {
        match self.state.load(Ordering::SeqCst) {
            state::CLOSING | state::CLOSED | state::CLOSING_HANDLING => {
                Poll::Ready(Err(Error::InternalError("called `poll_ready` after close")))
            }
            state::INITIALIZING | state::UNINIT => {
                Poll::Ready(Err(Error::InternalError("called `poll_ready` before init")))
            }
            state::IDLE => Poll::Ready(Ok(self.root_permit.derive())),
            _ => {
                let waker = cx.waker().clone();
                self.ready_wakelist.lock().push(waker);

                Poll::Pending
            }
        }
    }

    fn handle(&self, pmt: Permit, msg: AnyCell<'_>, resp: Option<SharedCell>) -> Result<(), Error> {
        match self.state.load(Ordering::SeqCst) {
            state::CLOSING | state::CLOSED | state::CLOSING_HANDLING => {
                return Err(Error::InternalError("called `handle` after close"))
            }
            state::INITIALIZING | state::UNINIT => {
                return Err(Error::InternalError("called `handle` before init"))
            }
            _ => (),
        }

        let inner = self.inner.clone();
        let msg = msg.take::<M>().unwrap();
        self.response_cell.swap(resp);

        self.handle_fut
            .swap(Some(Box::pin(async move { inner.handle(msg) })));

        self.poll_wakelist.lock().wake_all();

        Ok(())
    }

    fn poll_event(&self, cx: &mut Context<'_>) -> Poll<Result<Event, Error>> {
        if let Some(fut) = self.flush_fut.swap(None) {
            self.flush_fut.swap(Some(fut));
        }

        if let Some(fut) = self.handle_fut.swap(None) {
            if let Poll::Ready(res) = fut.poll_unpin(cx) {
                if let Some(res) = self.response_cell.take() {
                    res.set(res);
                } else {
                    // warn!("Unhandled response!");
                }

                self.ready_wakelist.lock().wake_all();

                return Poll::Ready(Ok(Event::Completed(1)));
            } else {
                self.handle_fut.swap(Some(fut));
            }
        } else {
            let waker = cx.waker().clone();
            self.poll_wakelist.lock().push(waker);
        }

        Poll::Pending
    }
}
