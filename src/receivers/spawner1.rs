use std::{
    ops::ControlFlow,
    pin::Pin,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use futures::future::poll_fn;
use heapless::mpmc::Q4;
use parking_lot::Mutex;

use crate::{
    cell::{MessageCell, MsgCell, SharedCell},
    error::Error,
    message::Message,
    permit::{Permit, RootPermit},
    receiver::{Event, Receiver, ReceiverEx},
    type_tag::{TypeTag, TypeTagQuery},
    wakelist::WakeList,
};

pub struct SpawnerInner<M: Message, R: Receiver> {
    receiver: R,
    in_queue: AtomicU32,
    ready_counter: AtomicU32,
    queue: Q4<(MsgCell<M>, Option<SharedCell>)>,
    ready_wakers: Mutex<WakeList>,
    poll_wakers: Mutex<WakeList>,
    send_wakers: Mutex<WakeList>,
}

pub struct Spawner<M: Message, R: Receiver> {
    inner: Arc<SpawnerInner<M, R>>,
    permit: RootPermit,
}

impl<M: Message, R: Receiver + Send + Sync + 'static> Spawner<M, R> {
    pub fn new(receiver: R) -> Self {
        let inner = Arc::new(SpawnerInner {
            receiver,
            in_queue: Default::default(),
            ready_counter: Default::default(),
            queue: Default::default(),
            ready_wakers: Mutex::new(WakeList::new()),
            poll_wakers: Mutex::new(WakeList::new()),
            send_wakers: Mutex::new(WakeList::new()),
        });

        let inner_clone = inner.clone();
        tokio::spawn(async move {
            let inner = inner_clone;
            loop {
                while let Some((mut msg, resp)) = inner.queue.dequeue() {
                    inner.in_queue.fetch_sub(1, Ordering::Relaxed);
                    inner.ready_wakers.lock().wake_all();

                    let _ = inner.receiver.send::<M>(&mut msg, resp).await; // TODO handler error

                    inner.ready_counter.fetch_add(1, Ordering::Relaxed);
                    inner.poll_wakers.lock().wake_all();
                }

                poll_fn::<(), _>(|cx| {
                    inner.send_wakers.lock().push(cx.waker().clone());
                    Poll::Pending
                })
                .await;
            }
        });

        Self {
            inner,
            permit: RootPermit::new(),
        }
    }
}

impl<M: Message, R: Receiver> Receiver for Spawner<M, R> {
    fn poll_ready(
        &self,
        msg: &TypeTag,
        resp: Option<&TypeTag>,
        cx: Option<&mut Context<'_>>,
    ) -> Poll<Result<Permit, Error>> {
        loop {
            let count = self.inner.in_queue.load(Ordering::Acquire);

            if count < 4 {
                if let Err(_) = self.inner.in_queue.compare_exchange(
                    count,
                    count + 1,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    continue;
                }

                return Poll::Ready(Ok(self.permit.derive()));
            } else {
                if let Some(cx) = cx {
                    self.inner.ready_wakers.lock().push(cx.waker().clone());
                }

                return Poll::Pending;
            }
        }
    }

    fn handle(
        &self,
        pmt: Permit,
        msg: &mut dyn MessageCell,
        resp: Option<SharedCell>,
    ) -> Result<(), Error> {
        assert!(pmt.belongs_to(&self.permit));

        self.inner
            .queue
            .enqueue((msg.take_cell::<M>()?, resp))
            .map_err(|_msg| Error::SendError)?;

        self.inner.in_queue.fetch_add(1, Ordering::AcqRel);
        self.inner.send_wakers.lock().wake_all();

        Ok(())
    }

    fn poll_event(self: Pin<&Self>, cx: &mut Context<'_>) -> Poll<Result<Event, Error>> {
        loop {
            let ready = self.inner.ready_counter.load(Ordering::Acquire);

            if ready > 0 {
                if self
                    .inner
                    .ready_counter
                    .compare_exchange(ready, 0, Ordering::Release, Ordering::Relaxed)
                    .is_err()
                {
                    continue;
                }

                break Poll::Ready(Ok(Event::Completed(ready as _)));
            } else {
                self.inner.poll_wakers.lock().push(cx.waker().clone());
                break Poll::Pending;
            }
        }
    }

    fn message_types(
        &self,
        cb: &mut dyn for<'a> FnMut(TypeTagQuery<'a>, TypeTagQuery<'a>) -> ControlFlow<()>,
    ) {
        todo!()
    }
}
