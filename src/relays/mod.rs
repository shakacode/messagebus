#[cfg(feature = "quic")]
mod quic;

pub enum AuthKind {
    Token(String),
}

#[cfg(feature = "quic")]
pub use quic::*;
