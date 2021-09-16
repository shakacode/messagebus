use std::net::SocketAddr;
use crate::error::Error;

pub const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];

pub struct QuicClientRelay {
    endpoint: quinn::Endpoint,
}

impl QuicClientRelay {
    pub fn new(cert: &str) -> Result<Self, Error> {
        let mut client_config = quinn::ClientConfigBuilder::default();
        client_config.protocols(ALPN_QUIC_HTTP);
        client_config.enable_keylog();
    
        let cert_der = std::fs::read(cert)?;
        let cert = quinn::Certificate::from_der(&cert_der)?;
    
        client_config.add_certificate_authority(cert).unwrap();
    
        let mut endpoint = quinn::Endpoint::builder();
        endpoint.default_client_config(client_config.build());
    
        let (endpoint, _) = endpoint.bind(&"0.0.0.0:0".parse().unwrap())?;

        Ok(Self { endpoint })
    }

    pub async fn connect(&self, addr: SocketAddr, host: &str) -> Result<(), Error> {
        let new_conn = self.endpoint
            .connect(&addr, host)?
            .await?;
    
        let quinn::NewConnection { connection: conn, .. } = new_conn;
    
        let (mut send, recv) = conn.open_bi().await?;
    
        send.write_all("hi".as_bytes()).await?;
        send.finish().await?;
    
        let _resp = recv
            .read_to_end(usize::max_value())
            .await?;

        conn.close(0u32.into(), b"done");

        self.endpoint.wait_idle().await;

        Ok(())
    }
}
