//! Common utilities shared across receiver implementations.

use crate::error::StdSyncSendError;
use crate::receiver::{Action, Event};
use crate::receivers::Request;
use crate::{Error, Message};
use futures::Stream;
use parking_lot::Mutex as ParkingLotMutex;
use std::pin::Pin;
use tokio::sync::mpsc;

/// Macro to implement `ReceiverSubscriberBuilder` for receiver types.
///
/// This reduces boilerplate for the 8 nearly-identical implementations across
/// the 4 receiver types (sync and async variants each).
///
/// # Variants
///
/// - Basic: `impl_receiver_subscriber_builder!(Receiver, Handler, Config, Mode)`
/// - Batched: `impl_receiver_subscriber_builder!(Receiver, Handler, Config, Mode, batched)`
/// - Batched with Clone: `impl_receiver_subscriber_builder!(Receiver, Handler, Config, Mode, batched_clone)`
#[macro_export]
macro_rules! impl_receiver_subscriber_builder {
    // Pattern for non-batched handlers (E is explicit generic)
    ($receiver:ty, $handler:ident, $config:ty, $mode:ty) => {
        impl<T, M, R, E> $crate::builder::ReceiverSubscriberBuilder<T, M, R, E> for $receiver
        where
            T: $crate::$handler<M, Response = R, Error = E> + 'static,
            M: $crate::Message,
            R: $crate::Message,
            E: $crate::error::StdSyncSendError,
        {
            type Config = $config;
            fn build(cfg: Self::Config) -> (Self, $crate::receiver::UntypedPollerCallback) {
                build_receiver::<T, M, R, E, $mode>(cfg)
            }
        }
    };

    // Pattern for batched handlers (uses T::Error)
    ($receiver:ty, $handler:ident, $config:ty, $mode:ty, batched) => {
        impl<T, M, R> $crate::builder::ReceiverSubscriberBuilder<T, M, R, T::Error> for $receiver
        where
            T: $crate::$handler<M, Response = R> + 'static,
            T::Error: $crate::error::StdSyncSendError,
            M: $crate::Message,
            R: $crate::Message,
        {
            type Config = $config;
            fn build(cfg: Self::Config) -> (Self, $crate::receiver::UntypedPollerCallback) {
                build_receiver::<T, M, R, T::Error, $mode>(cfg)
            }
        }
    };

    // Pattern for batched handlers with Clone bound on error
    ($receiver:ty, $handler:ident, $config:ty, $mode:ty, batched_clone) => {
        impl<T, M, R> $crate::builder::ReceiverSubscriberBuilder<T, M, R, T::Error> for $receiver
        where
            T: $crate::$handler<M, Response = R> + 'static,
            T::Error: $crate::error::StdSyncSendError + Clone,
            M: $crate::Message,
            R: $crate::Message,
        {
            type Config = $config;
            fn build(cfg: Self::Config) -> (Self, $crate::receiver::UntypedPollerCallback) {
                build_receiver::<T, M, R, T::Error, $mode>(cfg)
            }
        }
    };
}

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

use crate::group::GroupId;

/// Sends a typed message through the request channel.
///
/// This is the common implementation for `SendTypedReceiver::send`.
pub fn send_typed_message<M: Message>(
    sender: &mpsc::UnboundedSender<Request<M>>,
    mid: u64,
    m: M,
    req: bool,
    group_id: Option<GroupId>,
) -> Result<(), Error<M>> {
    match sender.send(Request::Request(mid, m, req, group_id)) {
        Ok(_) => Ok(()),
        Err(mpsc::error::SendError(Request::Request(_, msg, _, _))) => Err(Error::send_closed(msg)),
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

/// Buffer for messages belonging to a single group.
///
/// Used by batched receivers to collect messages per group_id,
/// ensuring each batch contains only messages from the same group.
pub struct GroupBuffer<M> {
    /// The buffered message payloads.
    msgs: Vec<M>,
    /// Message metadata: pairs of (message_id, requires_response).
    /// - `message_id`: Unique identifier for tracking the message through the bus.
    /// - `requires_response`: Whether the sender is waiting for a response.
    mids: Vec<(u64, bool)>,
}

impl<M> GroupBuffer<M> {
    /// Creates a new buffer with the specified capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            msgs: Vec::with_capacity(capacity),
            mids: Vec::with_capacity(capacity),
        }
    }

    /// Adds a message to the buffer.
    pub fn push(&mut self, mid: u64, msg: M, req: bool) {
        self.mids.push((mid, req));
        self.msgs.push(msg);
    }

    /// Returns the number of messages in the buffer.
    pub fn len(&self) -> usize {
        self.msgs.len()
    }

    /// Returns true if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.msgs.is_empty()
    }

    /// Takes all messages from the buffer, leaving it empty.
    pub fn take(&mut self) -> (Vec<M>, Vec<(u64, bool)>) {
        (
            std::mem::take(&mut self.msgs),
            std::mem::take(&mut self.mids),
        )
    }
}
