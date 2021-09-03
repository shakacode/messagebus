use messagebus::{
    derive::{Error as MbError, Message},
    error, Message, TypeTagged,
};
use thiserror::Error;

#[derive(Debug, Error, MbError)]
enum Error {
    #[error("Error({0})")]
    Error(anyhow::Error),
}

impl<M: Message> From<error::Error<M>> for Error {
    fn from(err: error::Error<M>) -> Self {
        Self::Error(err.into())
    }
}

#[derive(Debug, Clone, Message)]
#[namespace("api")]
pub struct Msg<F>(pub F);

#[derive(Debug, Clone, Message)]
#[type_tag("api::Query")]
pub struct Qqq<F, G, H>(
    pub F,
    pub G,
    pub H,
);

fn main() {
    assert_eq!(
        Qqq::<Msg<i32>, Msg<()>, u64>::type_tag_(),
        "api::Query<api::Msg<i32>,api::Msg<()>,u64>"
    );
}
