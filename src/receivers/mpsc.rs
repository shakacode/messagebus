use futures::{Stream, StreamExt};
use core::pin::Pin;
use crossbeam::queue::ArrayQueue;
use crossbeam::atomic::AtomicCell;
use core::task::{Waker, Context, Poll};
use std::sync::{Arc, atomic::*};

use crate::receiver::SendError;



struct ChannelInner<T> {
    queue: ArrayQueue<T>,
    send_waker: AtomicCell<Option<Box<Waker>>>,
    recv_waker: AtomicCell<Option<Box<Waker>>>,
    closed: AtomicBool,
}

pub fn channel<T>(buffer: usize) -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(ChannelInner {
        queue: ArrayQueue::new(buffer),
        send_waker: AtomicCell::new(None),
        recv_waker: AtomicCell::new(None),
        closed: AtomicBool::new(false),
    });
    
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver {
            inner,
        }
    )
}


#[derive(Clone)]
pub struct Sender<T> {
    inner: Arc<ChannelInner<T>>
}

impl <T> Sender<T> {
    pub fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }

        if self.inner.queue.is_full() {
            self.inner.send_waker.store(Some(Box::new(cx.waker().clone())));
        }
        
        let mut counter = 4;
        loop {
            if self.inner.queue.is_full() {
                if counter > 0 {
                    counter -= 1;
                    continue;
                } else {
                    break Poll::Pending;
                }
            } else {
                break Poll::Ready(());
            }
        }
    }
    
    pub fn try_send(&self, mut item: T) -> Result<(), SendError<T>> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(SendError::Closed(item));
        }

        let mut counter = 0;
        loop {
            match self.inner.queue.push(item) {
                Ok(_) => {
                    if let Some(waker) = self.inner.recv_waker.take() {
                        waker.wake();
                    }
                    
                    break Ok(());
                }
                
                Err(inner) => {
                    if counter >= 4 {
                        break Err(SendError::Full(inner));
                    } else {
                        item = inner;
                        counter += 1;
                    }
                }
            }
        }
    }

    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
        if let Some(waker) = self.inner.recv_waker.take() {
            waker.wake();
        }
    }    
}

pub struct Receiver<T> {
    inner: Arc<ChannelInner<T>>
}

impl <T> Stream for Receiver<T> {
    type Item = T;
    
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut counter = 0;
        
        loop {
            match this.inner.queue.pop() {
                Some(inner) => {
                    if let Some(waker) = this.inner.send_waker.take() {
                        waker.wake();
                    }
                    
                    break Poll::Ready(Some(inner));
                },
                None => {
                    if this.inner.closed.load(Ordering::SeqCst) {
                        break Poll::Ready(None);
                    } else {
                        if counter == 0 {
                            this.inner.recv_waker.store(Some(Box::new(cx.waker().clone())));
                        }
                        
                        if counter >= 8 {
                            break Poll::Pending;
                        } else {
                            counter += 1;
                        }
                    }
                }
            }
        }
    }
}
