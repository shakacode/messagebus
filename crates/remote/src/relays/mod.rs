// #[cfg(feature = "quic")]
mod quic;
// mod redis;
mod tcp;

use futures::Stream;
use messagebus::{error::GenericError, Event, Message, TypeTag};
use std::{collections::HashMap, pin::Pin};

// #[cfg(feature = "quic")]
pub use quic::*;
pub use tcp::*;

pub(crate) type GenericEventStream =
    Pin<Box<dyn Stream<Item = Event<Box<dyn Message>, GenericError>> + Send>>;

#[derive(Debug, Default)]
pub struct MessageTable {
    table: HashMap<TypeTag, Vec<Option<(TypeTag, TypeTag)>>>,
}

impl MessageTable {
    #[inline]
    pub fn add_any(&mut self, req: TypeTag, resp_err: Option<(TypeTag, TypeTag)>) {
        self.table
            .entry(req)
            .or_insert_with(Vec::new)
            .push(resp_err);
    }

    #[inline]
    pub fn add_request(&mut self, req: TypeTag, resp: TypeTag, err: TypeTag) {
        self.add_any(req, Some((resp, err)));
    }

    #[inline]
    pub fn add_message(&mut self, msg: TypeTag) {
        self.add_any(msg, None);
    }

    pub fn iter_keys(&self) -> impl Iterator<Item = &str> + '_ {
        self.table.keys().map(|k| k.as_ref())
    }

    #[inline]
    pub fn accept_any(&self, msg: &TypeTag) -> bool {
        self.table.contains_key(msg)
    }

    #[inline]
    pub fn accept_message(&self, msg: &TypeTag) -> bool {
        self.table
            .get(msg)
            .map_or(false, |v| v.iter().any(Option::is_none))
    }

    pub fn accept_request(
        &self,
        msg: &TypeTag,
        resp: Option<&TypeTag>,
        err: Option<&TypeTag>,
    ) -> bool {
        self.table.get(msg).map_or(false, |v| {
            v.iter().filter_map(Option::as_ref).any(|(r, e)| {
                resp.map_or(true, |resp| resp.as_ref() == r.as_ref())
                    && err.map_or(true, |err| err.as_ref() == e.as_ref())
            })
        })
    }

    pub fn iter_types(
        &self,
    ) -> impl Iterator<Item = (&'_ TypeTag, Option<&'_ (TypeTag, TypeTag)>)> + '_ {
        self.table
            .iter()
            .map(|(k, v)| v.iter().map(move |resp| (k, resp.as_ref())))
            .flatten()
    }
}

impl From<Vec<(TypeTag, Option<(TypeTag, TypeTag)>)>> for MessageTable {
    fn from(table: Vec<(TypeTag, Option<(TypeTag, TypeTag)>)>) -> Self {
        let mut outgoing_table = MessageTable::default();
        for (key, val) in table {
            outgoing_table.add_any(key, val);
        }
        outgoing_table
    }
}
