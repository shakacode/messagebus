use std::sync::Arc;

use futures::Stream;
use tokio::sync::Notify;

use crate::{
    cell::{RefCell, SharedCell},
    error::Error,
    receiver::{Event, Permit, Receiver, ReceiverMetadata},
};

pub struct Limit<R: Receiver> {
    inner: R,
    limit: usize,
    root_permit: Permit,
}

impl<R: Receiver> Limit<R> {}

impl<R: Receiver> Receiver for Limit<R> {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn metadata(&self) -> ReceiverMetadata {
        self.inner.metadata()
    }

    fn subscribe(&self, query: &MessageTypeQuery, notify: Arc<Notify>) -> Result<(), Error> {
        self.inner.subscribe(query, notify)
    }

    fn permit(&self, force: bool, notify: Arc<Notify>) -> Result<Option<Permit>, Error> {
        if force || Arc::strong_count(&self.root_permit) <= self.limit {
            if let Some(pmt) = self.inner.permit(force, notify)? {
                return self.root_permit.derive(Some(pmt));
            }
        } else {
            self.inner.notify(notify)?;
        }

        Ok(None)
    }

    fn send(&self, pmt: Permit, msg: RefCell<'_>, resp: SharedCell) -> Result<(), Error> {
        if !pmt.is_derived(&self.root_permit) {
            return Err(Error::wrong_permit(pmt));
        }

        if let Some(inner) = pmt.into_inner() {
            return self.inner.send(inner, msg, resp);
        }

        Err(Error::wrong_permit(pmt))
    }
}
