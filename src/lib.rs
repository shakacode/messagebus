#![feature(type_alias_impl_trait)]
#![feature(generic_associated_types)]

pub mod cell;
pub mod error;
pub mod handler;
pub mod message;
pub mod permit;
pub mod receiver;
pub mod receivers;
pub mod type_tag;

mod wakelist;
