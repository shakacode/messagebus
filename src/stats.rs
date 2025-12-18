//! Statistics for message bus receivers.

use std::borrow::Cow;

/// Statistics about a receiver's current state.
///
/// Returned by [`Bus::stats()`](crate::Bus::stats) to provide insight into
/// receiver queue depths, batch sizes, and parallelism levels.
///
/// # Example
///
/// ```rust,ignore
/// let stats = bus.stats();
/// for stat in stats {
///     println!("Message type: {}", stat.msg_type_tag);
///     if stat.has_queue {
///         println!("  Queue: {}/{}", stat.queue_size, stat.queue_capacity);
///     }
/// }
/// ```
#[derive(Default, Debug, Clone)]
pub struct Stats {
    /// The type tag of messages this receiver handles.
    pub msg_type_tag: Cow<'static, str>,

    /// The type tag of response messages, if the handler produces responses.
    pub resp_type_tag: Option<Cow<'static, str>>,

    /// The type tag of errors this handler can produce.
    pub err_type_tag: Option<Cow<'static, str>>,

    /// Whether this receiver has a message queue.
    pub has_queue: bool,

    /// Maximum number of messages the queue can hold.
    pub queue_capacity: i64,

    /// Current number of messages in the queue.
    pub queue_size: i64,

    /// Whether this receiver supports parallel message processing.
    pub has_parallel: bool,

    /// Maximum number of messages that can be processed in parallel.
    pub parallel_capacity: i64,

    /// Current number of messages being processed in parallel.
    pub parallel_size: i64,

    /// Whether this receiver batches messages before processing.
    pub has_batch: bool,

    /// Maximum number of messages per batch.
    pub batch_capacity: i64,

    /// Current number of messages in the batch buffer.
    pub batch_size: i64,
}
