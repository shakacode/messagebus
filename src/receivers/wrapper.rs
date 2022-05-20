use std::{
    intrinsics::transmute,
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{ready, Future};
use parking_lot::{Mutex, MutexGuard};

use crate::{
    cell::{MessageCell, SharedCell},
    error::Error,
    handler::Handler,
    message::Message,
    permit::{Permit, RootPermit},
    receiver::{Event, Receiver},
    type_tag::TypeTag,
    wakelist::WakeList,
};

pub struct HandlerWrapper<M: Message, T: Handler<M> + 'static> {
    inner: T,
    handle_fut: Mutex<Option<T::HandleFuture<'static>>>,
    pending_msg: Mutex<Option<M>>,
    ready_list: Mutex<WakeList>,
    poll_list: Mutex<WakeList>,
    permit: RootPermit,
}

impl<M: Message, T: Handler<M>> HandlerWrapper<M, T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            handle_fut: Mutex::new(None),
            pending_msg: Mutex::new(None),
            ready_list: Mutex::new(WakeList::new()),
            poll_list: Mutex::new(WakeList::new()),
            permit: RootPermit::new(),
        }
    }

    fn start_handle(&self, mut guard: MutexGuard<'_, Option<T::HandleFuture<'static>>>, msg: M) {
        let mut fut1: Option<T::HandleFuture<'_>> = Some(self.inner.handle(msg));
        let fut1_mut: &mut Option<T::HandleFuture<'static>> = unsafe { transmute(&mut fut1) };

        std::mem::swap(fut1_mut, &mut guard);
    }
}

impl<M: Message, T: Handler<M>> Receiver for HandlerWrapper<M, T> {
    fn poll_ready(&self, cx: Option<&mut Context<'_>>) -> Poll<Result<Permit, Error>> {
        if self.pending_msg.lock().is_none() {
            return Poll::Ready(Ok(self.permit.derive()));
        }

        if let Some(cx) = cx {
            self.ready_list.lock().push(cx.waker().clone());
        }

        Poll::Pending
    }

    fn handle(
        &self,
        pmt: Permit,
        msg: &mut dyn MessageCell,
        resp: Option<SharedCell>,
    ) -> Result<(), Error> {
        assert!(pmt.belongs_to(&self.permit));

        let msg = msg.take()?;

        if let Some(guard) = self.handle_fut.try_lock() {
            if guard.is_none() {
                self.start_handle(guard, msg);
                self.poll_list.lock().wake_all();

                return Ok(());
            }
        }

        let mut guard = self.pending_msg.lock();
        if guard.is_some() {
            return Err(Error::SendError);
        }

        guard.replace(msg);

        Ok(())
    }

    fn poll_event(self: Pin<&Self>, cx: &mut Context<'_>) -> Poll<Result<Event, Error>> {
        loop {
            let mut lock = self.handle_fut.lock();

            if lock.is_some() {
                let fut = lock.as_mut().unwrap();
                let pinned = unsafe { Pin::new_unchecked(fut) };
                let _res = ready!(pinned.poll(cx));

                // TODO use result

                return Poll::Ready(Ok(Event::Completed(1)));
            }

            if let Some(msg) = self.pending_msg.lock().take() {
                self.start_handle(lock, msg);
                continue;
            } else {
                self.poll_list.lock().push(cx.waker().clone());
            }

            break Poll::Pending;
        }
    }

    fn message_types(&self, cb: &mut dyn FnMut(TypeTag, Option<TypeTag>) -> ControlFlow<()>) {
        todo!()
    }
}
