//! Common utilities shared across receiver implementations.

use crate::error::StdSyncSendError;
use crate::receiver::{Action, Event};
use crate::receivers::Request;
use crate::{Error, Message};
use futures::Stream;
use parking_lot::Mutex as ParkingLotMutex;
use std::pin::Pin;
use tokio::sync::mpsc;

/// Trait for optional stats tracking on send.
pub trait MaybeSendStats {
    /// Called when a message is successfully sent. Default is no-op.
    fn on_send_success(&self) {}
}

/// Marker type for receivers without stats.
pub struct NoStats;

impl MaybeSendStats for NoStats {}

/// Sends an action through the request channel.
///
/// This is the common implementation for `SendUntypedReceiver::send`.
pub fn send_untyped_action<M: Message>(
    sender: &mpsc::UnboundedSender<Request<M>>,
    msg: Action,
) -> Result<(), Error<Action>> {
    match sender.send(Request::Action(msg)) {
        Ok(_) => Ok(()),
        Err(mpsc::error::SendError(Request::Action(msg))) => Err(Error::send_closed(msg)),
        _ => unimplemented!(),
    }
}

/// Sends a typed message through the request channel.
///
/// This is the common implementation for `SendTypedReceiver::send`.
pub fn send_typed_message<M: Message, S: MaybeSendStats>(
    sender: &mpsc::UnboundedSender<Request<M>>,
    stats: &S,
    mid: u64,
    m: M,
    req: bool,
) -> Result<(), Error<M>> {
    match sender.send(Request::Request(mid, m, req)) {
        Ok(_) => {
            stats.on_send_success();
            Ok(())
        }
        Err(mpsc::error::SendError(Request::Request(_, msg, _))) => Err(Error::send_closed(msg)),
        _ => unimplemented!(),
    }
}

/// Creates an event stream from a locked receiver.
///
/// This is the common implementation for `ReciveTypedReceiver::event_stream`.
pub fn create_event_stream<R, E>(
    srx: &ParkingLotMutex<Option<mpsc::UnboundedReceiver<Event<R, E>>>>,
) -> Pin<Box<dyn Stream<Item = Event<R, E>> + Send>>
where
    R: Message,
    E: StdSyncSendError,
{
    let mut rx = srx.lock().take().expect("event_stream called twice");
    Box::pin(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
}
