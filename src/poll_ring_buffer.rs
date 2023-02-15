use anyhow::Error;
use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Poll};
use futures::{ready, Future, Stream};
use parking_lot::Mutex;

pub struct PollRingBuffer<const S: usize, F> {
    inner: [UnsafeCell<MaybeUninit<F>>; S],
    wakers: Mutex<Wakelist>,
    head: Mutex<usize>,
    tail: Mutex<usize>,
}

impl<const S: usize, F: Future> PollRingBuffer<S, F> {
    pub fn enqueue(&self, fut: F) -> Result<(), Error> {
        let mut tail = self.tail.lock();
        if *tail + 1 == *self.head.lock() {
            return anyhow::bail!("full");
        }

        let item = unsafe { &mut *self.inner[*tail].get() };
        item.write(fut);

        *tail += 1 % S;
        drop(tail);

        self.wakelist.lock().wake_all();

        Ok(())
    }

    pub fn poll_next(self: Pin<&Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Event, Error>>> {
        let mut head = self.head.lock();

        while *head < *self.tail.lock() {
            let item = unsafe { &mut *self.inner[*head].get() };

            let x = unsafe { ready!(Pin::new_unchecked(item.assume_init_mut()).poll(cx)) };

            unsafe {
                item.assume_init_drop();
            }

            *head = (*head + 1) % S;
        }

        self.wakelist.lock().push(cx.waker().clone());

        Poll::Pending
    }
}
