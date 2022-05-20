use std::any::TypeId;
use std::sync::Arc;

use futures::Stream;
use tokio::sync::Notify;

use crate::error::Error;
use crate::receiver_store::ReceiverStore;
use crate::{
    cell::{RefCell, SharedCell},
    message::Message,
    type_tag::TypeTag,
};

pub enum MessageType {
    Send(TypeTag),
    Request(TypeTag, TypeTag),
    RequestWithError(TypeTag, TypeTag, TypeTag),
}

pub struct ReceiverMetadata {
    pub request_types: Vec<MessageType>,
    pub receive_types: Vec<MessageType>,
}

pub enum Event {
    Completed(u32),
}

pub trait ReceiverBuidler {
    type Config;
    fn build() -> Self;
}

pub trait Receiver {
    fn name(&self) -> &str;
    fn init(&self, _notify: Arc<Notify>) -> Result<(), Error> {
        Ok(true)
    }
    fn metadata(&self) -> ReceiverMetadata;
    fn subscribe(&self, query: &MessageTypeQuery, notify: Arc<Notify>) -> Result<(), Error>;
    fn notify(&self, notify: Arc<Notify>) -> Result<(), Error>;
    fn permit(&self, force: bool, notify: Arc<Notify>) -> Result<Option<Permit>, Error>;
    fn send(&self, pmt: Permit, msg: RefCell<'_>, resp: SharedCell) -> Result<(), Error>;
    fn sync(&self, notify: Arc<Notify>) -> Result<(), Error>;
}

// impl<T: Receiver + ?Sized> T {
//     pub async fn send(&self, msg: RefCell<'_>) -> Result<(), Error> {
//         self.permit(false, )

//         Ok(())
//     }
// }

pub struct BusInner {
    receivers: ReceiverStore,
}

impl BusInner {
    pub async fn send<M: Message>(&self, msg: M) -> Result<(), Error> {
        let tid = M::TYPE_TAG();

        let receivers = self.receivers.query(tid, None, None, Group::ANY);
        let mut to_send = Some(msg);

        let first = receivers
            .next()
            .or_else(|| Error::no_such_receiver(tid, None))?;

        let mut to_send_cell = None;
        for receiver in receivers {
            if to_send_cell.is_none() {
                to_send_cell = to_send.clone();
            }

            receiver.send(&mut to_send_cell).await;
        }

        let receiver = self
            .receivers
            .get(tid, Default::default())
            .or_else(|| Error::no_such_receiver(tid, None));

        receiver.send(&mut to_send).await
    }

    pub async fn request<M: Message, R: Message>(&self, req: M) -> Result<R, Error> {
        let tid = M::TYPE_TAG();
        let rid = R::TYPE_TAG();

        let receiver = self
            .receivers
            .get(tid, Some(rid))
            .or_else(|| Error::no_such_receiver(tid, Some(rid)));

        let mut to_send = Some(req);
        let mut resp: Option<R> = None;

        receiver.request(&mut to_send, &mut resp).await?
    }
}
