use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Error(pub Arc<anyhow::Error>);

impl<T: Into<anyhow::Error>> From<T> for Error {
    fn from(e: T) -> Self {
        Self(Arc::new(e.into()))
    }
}
