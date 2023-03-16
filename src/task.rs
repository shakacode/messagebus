use std::{any::Any, fmt, sync::Arc};

pub struct TaskHandlerVTable {
    pub drop: fn(Arc<dyn Any + Send + Sync>, u64),
}

impl TaskHandlerVTable {
    pub const EMPTY: &TaskHandlerVTable = &TaskHandlerVTable { drop: |_, _| {} };
}

pub struct TaskHandler {
    data: Arc<dyn Any + Send + Sync>,
    index: u64,
    vtable: &'static TaskHandlerVTable,
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
        vtable: &'static TaskHandlerVTable,
        data: Arc<dyn Any + Send + Sync>,
        index: u64,
    ) -> Self {
        Self {
            data,
            index,
            vtable,
        }
    }

    #[inline]
    pub(crate) fn finish(&mut self) {
        (self.vtable.drop)(self.data.clone(), self.index);
        self.vtable = TaskHandlerVTable::EMPTY;
    }

    #[inline]
    pub(crate) fn is_finished(&self) -> bool {
        std::ptr::eq(self.vtable, TaskHandlerVTable::EMPTY)
    }
}

impl Drop for TaskHandler {
    fn drop(&mut self) {
        // TODO optimize redundant clone
        (self.vtable.drop)(self.data.clone(), self.index);
    }
}
