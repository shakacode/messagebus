use std::borrow::Cow;

#[derive(Default, Debug, Clone)]
pub struct Stats {
    pub msg_type_tag: Cow<'static, str>,

    pub resp_type_tag: Option<Cow<'static, str>>,
    pub err_type_tag: Option<Cow<'static, str>>,

    pub has_queue: bool,
    pub queue_capacity: i64,
    pub queue_size: i64,

    pub has_parallel: bool,
    pub parallel_capacity: i64,
    pub parallel_size: i64,

    pub has_batch: bool,
    pub batch_capacity: i64,
    pub batch_size: i64,
}
