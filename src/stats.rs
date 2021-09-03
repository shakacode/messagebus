use std::borrow::Cow;

#[derive(Default, Debug, Clone)]
pub struct Stats {
    pub msg_type_tag: Cow<'static, str>,
    
    pub resp_type_tag: Option<Cow<'static, str>>,
    pub err_type_tag: Option<Cow<'static, str>>,

    pub has_queue: bool,
    pub queue_capacity: u64,
    pub queue_size: u64,

    pub has_parallel: bool,
    pub parallel_capacity: u64,
    pub parallel_size: u64,

    pub has_batch: bool,
    pub batch_capacity: u64,
    pub batch_size: u64,
}