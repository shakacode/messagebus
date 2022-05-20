use std::collections::HashMap;

use crate::type_tag::TypeTag;
use parking_lot::RwLock;

pub struct MessageQueue {
    type_tag: TypeTag,
    queue: g,
}

pub struct MessageTypeQuery {}
pub struct ReceiverStore {
    store: RwLock<HashMap<MessageTypeQuery, ReceiverImpl>>,
}

// pub enum ReceiverImpl {
//     QueuedReceiverImpl(QueuedReceiverImpl),
//     SimpleReceiverImpl(SimpleReceiverImpl),
// }

// impl ReceiverImpl {
//     pub fn request
// }
impl ReceiverStore {
    pub(crate) fn query(
        &self,
        tid: crate::type_tag::TypeTag,
        none_1: Option<_>,
        none_2: Option<_>,
        any: _,
    ) -> impl Iterator<Item = &Receiver> {
        todo!()
    }

    pub(crate) fn get(
        &self,
        tid: crate::type_tag::TypeTag,
        rid: Option<crate::type_tag::TypeTag>,
    ) -> _ {
        todo!()
    }
}
