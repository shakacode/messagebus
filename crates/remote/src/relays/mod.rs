// #[cfg(feature = "quic")]
mod quic;

// mod redis;

use futures::Stream;
use messagebus::{error::GenericError, Event, Message, TypeTag};
use std::{collections::HashMap, pin::Pin};

// #[cfg(feature = "quic")]
pub use quic::*;

pub(crate) type GenericEventStream =
    Pin<Box<dyn Stream<Item = Event<Box<dyn Message>, GenericError>> + Send>>;

#[derive(Debug, Default)]
pub struct MessageTable {
    table: HashMap<TypeTag, Vec<(TypeTag, TypeTag)>>,
}

impl MessageTable {
    pub fn add(&mut self, req: TypeTag, resp: TypeTag, err: TypeTag) {
        self.table
            .entry(req)
            .or_insert_with(Vec::new)
            .push((resp, err));
    }

    pub fn accept(&self, msg: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
        self.table.get(msg).map_or(false, |v| {
            v.iter().any(|(r, e)| {
                resp.map_or(true, |resp| resp.as_ref() == r.as_ref())
                    && err.map_or(true, |err| err.as_ref() == e.as_ref())
            })
        })
    }

    pub fn iter_types(&self) -> impl Iterator<Item = (&'_ TypeTag, &'_ TypeTag, &'_ TypeTag)> + '_ {
        self.table
            .iter()
            .map(|(k, v)| v.iter().map(move |(e, r)| (k, r, e)))
            .flatten()
    }
}

impl From<Vec<(TypeTag, TypeTag, TypeTag)>> for MessageTable {
    fn from(table: Vec<(TypeTag, TypeTag, TypeTag)>) -> Self {
        let mut outgoing_table = MessageTable::default();
        for (x, y, z) in table {
            outgoing_table.add(x, y, z);
        }
        outgoing_table
    }
}
