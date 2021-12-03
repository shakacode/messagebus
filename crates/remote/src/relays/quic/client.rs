use crate::error::Error;
use futures::{Future, Stream};
use quinn::Connecting;
use std::{
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use super::WaitIdle;

pub struct QuicClientEndpoint {
    addr: SocketAddr,
    host: String,
    endpoint: quinn::Endpoint,
}

impl QuicClientEndpoint {
    pub fn new(cert: &str, addr: SocketAddr, host: String) -> Result<Self, Error> {
        let mut client_config = quinn::ClientConfigBuilder::default();
        client_config.protocols(super::ALPN_QUIC_HTTP);
        client_config.enable_keylog();

        let cert_der = std::fs::read(cert)?;
        let cert = quinn::Certificate::from_der(&cert_der)?;

        client_config.add_certificate_authority(cert).unwrap();

        let mut endpoint = quinn::Endpoint::builder();
        endpoint.default_client_config(client_config.build());

        let (endpoint, _) = endpoint.bind(&"0.0.0.0:0".parse().unwrap())?;

        Ok(Self {
            addr,
            host,
            endpoint,
        })
    }
}

impl Stream for QuicClientEndpoint {
    type Item = Connecting;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Poll::Ready(this.endpoint.connect(&this.addr, &this.host).ok())
    }
}

impl<'a> WaitIdle<'a> for QuicClientEndpoint {
    type Fut = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    fn wait_idle(&'a self) -> Self::Fut {
        Box::pin(self.endpoint.wait_idle())
    }
}
