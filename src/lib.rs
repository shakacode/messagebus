#![feature(type_alias_impl_trait)]

pub mod bus;
pub mod cell;
pub mod error;
pub mod handler;
pub mod message;
pub mod message_impls;
pub mod polling_pool;
pub mod receiver;
pub mod receivers;
pub mod task;
pub mod type_tag;
mod utils;

pub use bus::Bus;
pub use handler::*;
pub use message::*;
pub use message_impls::*;
pub use task::*;
pub use type_tag::{TypeTag, TypeTagInfo};

pub mod derive {
    pub use messagebus_derive::*;
}

pub mod __derive_private {
    pub use lazy_static;
}
