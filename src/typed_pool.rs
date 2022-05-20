use std::{any::Any, collections::HashMap};

use parking_lot::{Mutex, RwLock};

use crate::type_tag::TypeTag;

#[derive(Default)]
pub struct TypedPool {
    pool: RwLock<HashMap<TypeTag, Mutex<Vec<Box<dyn Any>>>>>,
}

impl TypedPool {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn get<T: Any>(&self) -> Box<T> {}
    pub fn get_arc<T: Any>(&self) -> Arc<T> {}

    pub fn put<T: Any>(&self, val: Box<T>) {}
}
