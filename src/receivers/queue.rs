use std::{
    ops::ControlFlow,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crossbeam::queue::SegQueue;
use tokio::sync::Notify;

use crate::{
    cell::{MessageCell, MsgCell, SharedCell},
    error::Error,
    message::Message,
    permit::{Permit, RootPermit},
    receiver::{Event, Receiver},
    type_tag::TypeTag,
};

pub struct QueueConfig {
    queue_size: usize,
}

pub struct QueueInner<T: Message, R: Receiver> {
    queue: SegQueue<(MsgCell<T>, Option<SharedCell>)>,
    inner: R,
}

pub struct Queue<T: Message, R: Receiver> {
    inner: Arc<QueueInner<T, R>>,
    config: QueueConfig,
    root_permit: RootPermit,
    notify: Arc<Notify>,
}

impl<T: Message, R: Receiver> Queue<T, R> {
    pub fn build() {}
}

impl<T: Message, R: Receiver> Receiver for Queue<T, R> {
    fn poll_ready(&self, cx: Option<&mut Context<'_>>) -> Poll<Result<Permit, Error>> {
        todo!()
    }

    fn handle(
        &self,
        pmt: Permit,
        msg: &mut dyn MessageCell,
        resp: Option<SharedCell>,
    ) -> Result<(), Error> {
        todo!()
    }

    fn poll_event(self: Pin<&Self>, cx: &mut Context<'_>) -> Poll<Result<Event, Error>> {
        todo!()
    }

    fn message_types(&self, cb: &mut dyn FnMut(TypeTag, Option<TypeTag>) -> ControlFlow<()>) {}

    //     fn name(&self) -> &str {
    //         self.inner.name()
    //     }

    //     fn metadata(&self) -> ReceiverMetadata {
    //         self.inner.metadata()
    //     }

    //     fn init(&self, _notify: Arc<Notify>) -> Result<bool, Error> {
    //         let inner = self.inner.clone();

    //         tokio::spawn(async move {
    //             loop {
    //                 loop {
    //                     if inner.queue.is_empty() {
    //                         break;
    //                     }

    //                     if let Ok(Some(pmt)) = inner.inner.permit(false, inner.notify.clone()) {
    //                         if let Some((msg, resp)) = inner.queue.pop() {
    //                             let _ = inner.inner.send(pmt, msg.as_ref_cell(), resp);
    //                             // TODO process error
    //                         }
    //                     }
    //                 }

    //                 inner.notify.notified().await
    //             }
    //         });

    //         Ok(true)
    //     }

    //     fn notify(&self, notify: Arc<Notify>) -> Result<(), Error> {
    //         self.inner.inner.notify(notify)
    //     }

    //     fn subscribe(&self, query: &MessageTypeQuery, notify: Arc<Notify>) -> Result<(), Error> {
    //         self.subscribe(query, notify)
    //     }

    //     fn permit(&self, _force: bool, _notify: Arc<Notify>) -> Result<Option<Permit>, Error> {
    //         Ok(Some(self.root_permit.derive(None)))
    //     }

    //     fn send(&self, pmt: Permit, msg: RefCell<'_>, resp: Option<SharedCell>) -> Result<(), Error> {
    //         if !pmt.is_derived(&self.root_permit) {
    //             return Err(Error::wrong_permit(pmt));
    //         }

    //         if self.queue.is_empty() {
    //             if let Some(pmt) = self.inner.permit(false, self.inner.inner.notify.clone())? {
    //                 return self.send(pmt, msg, resp);
    //             }
    //         }

    //         self.queue.push((msg.into_cell::<T>(), resp));

    //         Ok(())
    //     }
}
