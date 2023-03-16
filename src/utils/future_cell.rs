use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

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
    pub fn unset(&mut self) {
        self.inner.set(None)
    }

    #[inline]
    pub fn poll_unpin(&mut self, cx: &mut Context<'_>) -> Poll<F::Output> {
        let Some(fut) = self.inner.as_mut().as_pin_mut() else {
            return Poll::Pending;
        };

        let res = ready!(fut.poll(cx));
        self.unset();

        Poll::Ready(res)
    }

    #[inline]
    pub fn try_poll_unpin(&mut self, cx: &mut Context<'_>) -> Option<Poll<F::Output>> {
        let fut = self.inner.as_mut().as_pin_mut()?;
        let res = match fut.poll(cx) {
            Poll::Ready(res) => res,
            Poll::Pending => return Some(Poll::Pending),
        };

        self.unset();

        Some(Poll::Ready(res))
    }
}
