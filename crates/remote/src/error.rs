use quinn::{ConnectError, ConnectionError, EndpointError, ParseError, ReadToEndError, WriteError};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),

    // #[error("ReadToEnd: {0}")]
    // ReadToEnd(#[from] ReadToEndError),
    #[error("ConnectionError: {0}")]
    ConnectionError(#[from] ConnectionError),

    #[error("ConnectError: {0}")]
    ConnectError(#[from] ConnectError),

    #[error("EndpointError: {0}")]
    EndpointError(#[from] EndpointError),

    #[error("WriteError: {0}")]
    WriteError(#[from] WriteError),

    #[error("ReadToEndError: {0}")]
    ReadToEndError(#[from] ReadToEndError),

    #[error("QuinnConnectError: {0}")]
    QuinnParseError(#[from] ParseError),
}
