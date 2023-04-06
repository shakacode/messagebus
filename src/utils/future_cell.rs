use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use crate::error::{Error, ErrorKind};

use super::wakelist::WakeList;
use parking_lot::Mutex;

pub struct FutureCell<F: Future> {
    inner: Pin<Box<Option<F>>>,
}

impl<F: Future> Default for FutureCell<F> {
    fn default() -> Self {
        Self {
            inner: Box::pin(None),
        }
    }
}

impl<F: Future> FutureCell<F> {
    pub fn new() -> Self {
        Self {
            inner: Box::pin(None),
        }
    }

    pub fn wrap(fut: F) -> Self {
        Self {
            inner: Box::pin(Some(fut)),
        }
    }

    #[inline]
    pub fn is_set(&self) -> bool {
        self.inner.is_some()
    }

    #[inline]
    pub fn set(&mut self, fut: F) {
        self.inner.set(Some(fut))
    }

    #[inline]
    pub fn try_set(&mut self, fut: impl FnOnce() -> F) -> bool {
        if !self.is_set() {
            self.set(fut());
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.inner.set(None)
    }

    #[inline]
    pub fn poll_unpin(&mut self, cx: &mut Context<'_>) -> Poll<F::Output> {
        let Some(fut) = self.inner.as_mut().as_pin_mut() else {
            return Poll::Pending;
        };

        let res = ready!(fut.poll(cx));
        self.clear();

        Poll::Ready(res)
    }

    #[inline]
    pub fn try_poll_unpin(&mut self, cx: &mut Context<'_>) -> Option<Poll<F::Output>> {
        let fut = self.inner.as_mut().as_pin_mut()?;
        let res = match fut.poll(cx) {
            Poll::Ready(res) => res,
            Poll::Pending => return Some(Poll::Pending),
        };

        self.clear();

        Some(Poll::Ready(res))
    }
}

pub struct SyncFutureCell<F: Future> {
    inner: Mutex<FutureCell<F>>,
    wakelist: WakeList,
}

impl<F: Future> SyncFutureCell<F> {
    pub fn new() -> SyncFutureCell<F> {
        Self {
            inner: Mutex::new(FutureCell::new()),
            wakelist: WakeList::new(),
        }
    }

    pub fn poll_unpin(&self, cx: &mut Context<'_>) -> Poll<F::Output> {
        if let Some(mut lock) = self.inner.try_lock() {
            if let Some(poll) = lock.try_poll_unpin(cx) {
                if poll.is_ready() {
                    self.wakelist.wake_all();
                }

                return poll;
            }
        }

        self.wakelist.register(cx.waker());
        Poll::Pending
    }

    pub fn poll_unpin_with<N: FnMut() -> F>(
        &self,
        cx: &mut Context<'_>,
        mut next_fn: N,
    ) -> Poll<F::Output> {
        if let Some(mut lock) = self.inner.try_lock() {
            if let Some(poll) = lock.try_poll_unpin(cx) {
                if poll.is_ready() {
                    self.wakelist.wake_all();
                }

                poll
            } else {
                lock.set(next_fn());
                lock.poll_unpin(cx)
            }
        } else {
            self.wakelist.register(cx.waker());
            Poll::Pending
        }
    }

    pub fn try_poll_unpin_with<E, N: FnMut() -> Result<F, E>>(
        &self,
        cx: &mut Context<'_>,
        mut next_fn: N,
    ) -> Result<Poll<F::Output>, E> {
        if let Some(mut lock) = self.inner.try_lock() {
            if let Some(poll) = lock.try_poll_unpin(cx) {
                drop(lock);

                if poll.is_ready() {
                    self.wakelist.wake_all();
                }

                Ok(poll)
            } else {
                lock.set(next_fn()?);
                Ok(lock.poll_unpin(cx))
            }
        } else {
            self.wakelist.register(cx.waker());
            Ok(Poll::Pending)
        }
    }

    #[inline]
    pub(crate) fn try_set(&self, f: impl FnOnce() -> F) -> bool {
        if let Some(mut lock) = self.inner.try_lock() {
            lock.try_set(f);
            true
        } else {
            false
        }
    }

    pub(crate) fn async_set(&self, x: F) -> impl Future<Output = ()> + '_ {
        let mut x = Some(x);
        std::future::poll_fn(move |cx| {
            if let Some(mut cell) = self.inner.try_lock() {
                if !cell.is_set() {
                    cell.set(x.take().unwrap());
                    return Poll::Ready(());
                }
            }

            self.wakelist.register(cx.waker());
            Poll::Pending
        })
    }

    #[inline]
    pub(crate) fn try_with<U>(
        &self,
        cb: impl FnOnce(&mut FutureCell<F>) -> Result<U, Error>,
    ) -> Result<U, Error> {
        if let Some(mut lock) = self.inner.try_lock() {
            Ok((cb)(&mut lock)?)
        } else {
            Err(ErrorKind::TrySendError.into())
        }
    }
}
