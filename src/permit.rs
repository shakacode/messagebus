use std::sync::Arc;

pub struct Permit(Arc<()>);

impl Permit {
    fn new(inner: Arc<()>) -> Self {
        Permit(inner)
    }

    pub fn belongs_to(&self, other: &RootPermit) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

pub struct RootPermit(Arc<()>);
impl RootPermit {
    pub fn new() -> Self {
        Self(Arc::new(()))
    }

    pub fn derive(&self) -> Permit {
        Permit::new(self.0.clone())
    }

    pub fn granted(&self) -> usize {
        Arc::strong_count(&self.0) - 1
    }
}
