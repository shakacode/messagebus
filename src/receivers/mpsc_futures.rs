use crate::receiver::SendError;
use core::pin::Pin;
use core::task::{Context, Poll};
use crossbeam::queue::SegQueue;
use futures::{channel::mpsc, Stream};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Waker,
};

pub struct State {
    buffer: usize,
    counter: AtomicUsize,
    send_wakers: SegQueue<Waker>,
}

pub fn channel<T>(buffer: usize) -> (Sender<T>, Receiver<T>) {
    let state = Arc::new(State {
        buffer,
        counter: AtomicUsize::new(0),
        send_wakers: SegQueue::new(),
    });
    let (tx, rx) = mpsc::unbounded();

    (
        Sender {
            inner: tx,
            state: state.clone(),
        },
        Receiver { inner: rx, state },
    )
}

pub struct Sender<T> {
    inner: mpsc::UnboundedSender<T>,
    state: Arc<State>,
}

impl<T> Sender<T> {
    pub fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.state.counter.load(Ordering::SeqCst) >= self.state.buffer {
            self.state.send_wakers.push(cx.waker().clone());
            return Poll::Pending;
        }

        Poll::Ready(())
    }

    pub fn try_send(&self, item: T) -> Result<(), SendError<T>> {
        if self.state.counter.load(Ordering::Relaxed) >= self.state.buffer {
            return Err(SendError::Full(item));
        }

        self.state.counter.fetch_add(1, Ordering::SeqCst);
        match self.inner.unbounded_send(item) {
            Ok(_) => Ok(()),
            Err(err) if err.is_full() => Err(SendError::Full(err.into_inner())),
            Err(err) => Err(SendError::Closed(err.into_inner())),
        }
    }

    #[inline]
    pub fn flush(&self) {}

    #[inline]
    pub fn close(&self) {
        self.inner.close_channel();
    }
}

pub struct Receiver<T> {
    inner: mpsc::UnboundedReceiver<T>,
    state: Arc<State>,
}

impl<T> Stream for Receiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(inner) => {
                let val = this.state.counter.fetch_sub(1, Ordering::SeqCst);

                if val <= this.state.buffer {
                    if let Some(waker) = this.state.send_wakers.pop() {
                        waker.wake();
                    }
                }

                Poll::Ready(inner)
            }
            Poll::Pending => {
                while let Some(waker) = this.state.send_wakers.pop() {
                    waker.wake();
                }

                Poll::Pending
            },
        }
    }
}
