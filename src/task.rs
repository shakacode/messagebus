use std::{any::Any, fmt, sync::Arc};

lazy_static::lazy_static! {
    static ref VOID_DATA: Arc<dyn Any + Send + Sync> = Arc::new(());
}

fn noop_drop(_: Arc<dyn Any + Send + Sync>, _: u64) {}

pub struct TaskHandler {
    data: Arc<dyn Any + Send + Sync>,
    index: u64,
    drop: fn(Arc<dyn Any + Send + Sync>, u64),
}

impl PartialEq for TaskHandler {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.data, &other.data) && self.index == other.index
    }
}

impl fmt::Debug for TaskHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Task #{} in {:?}", self.index, self.data)
    }
}

unsafe impl Send for TaskHandler {}
unsafe impl Sync for TaskHandler {}

impl TaskHandler {
    #[inline]
    pub fn index(&self) -> u64 {
        self.index
    }

    #[inline]
    pub fn data(&self) -> &Arc<dyn Any + Send + Sync> {
        &self.data
    }

    #[inline]
    pub fn data_ptr(&self) -> *const () {
        Arc::as_ptr(&self.data) as *const ()
    }

    #[inline]
    pub fn new(
        data: Arc<dyn Any + Send + Sync>,
        index: u64,
        drop: fn(Arc<dyn Any + Send + Sync>, u64),
    ) -> Self {
        Self { data, index, drop }
    }

    #[inline]
    pub(crate) fn finish(&mut self) {
        (self.drop)(
            std::mem::replace(&mut self.data, VOID_DATA.clone()),
            self.index,
        );

        self.drop = noop_drop;
    }

    #[inline]
    pub(crate) fn is_finished(&self) -> bool {
        Arc::ptr_eq(&self.data, &VOID_DATA)
    }

    pub(crate) fn noop() -> TaskHandler {
        Self {
            data: VOID_DATA.clone(),
            index: 0,
            drop: noop_drop,
        }
    }
}

impl Drop for TaskHandler {
    fn drop(&mut self) {
        // println!("dropping task {}", self.index);

        (self.drop)(
            std::mem::replace(&mut self.data, VOID_DATA.clone()),
            self.index,
        );
    }
}
