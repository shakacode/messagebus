use crate::error::Error;
use futures::{Future, Stream};
use quinn::Connecting;
use std::{
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use super::WaitIdle;

pub struct QuicServerEndpoint {
    endpoint: quinn::Endpoint,
    incoming: quinn::Incoming,
}

impl QuicServerEndpoint {
    pub fn new(key_path: &str, cert_path: &str, addr: &SocketAddr) -> Result<Self, Error> {
        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_concurrent_uni_streams(0)?;

        let mut server_config = quinn::ServerConfig::default();
        server_config.transport = Arc::new(transport_config);

        let mut server_config = quinn::ServerConfigBuilder::new(server_config);

        server_config.protocols(super::ALPN_QUIC_HTTP);
        server_config.enable_keylog();

        let key = std::fs::read(key_path)?;
        let cert_der = std::fs::read(cert_path)?;

        let key = quinn::PrivateKey::from_der(&key)?;
        let cert_chain = quinn::Certificate::from_der(&cert_der)?;

        let cert = quinn::CertificateChain::from_certs([cert_chain]);

        server_config.certificate(cert, key)?;

        let mut endpoint = quinn::Endpoint::builder();
        endpoint.listen(server_config.build());

        let (endpoint, incoming) = endpoint.bind(addr)?;

        Ok(Self { endpoint, incoming })
    }
}

impl Stream for QuicServerEndpoint {
    type Item = Connecting;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        unsafe { Pin::new_unchecked(&mut this.incoming) }.poll_next(cx)
    }
}

impl<'a> WaitIdle<'a> for QuicServerEndpoint {
    type Fut = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    fn wait_idle(&'a self) -> Self::Fut {
        Box::pin(self.endpoint.wait_idle())
    }
}
