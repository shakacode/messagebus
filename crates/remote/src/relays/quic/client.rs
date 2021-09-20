use crate::error::Error;
use futures::Stream;
use messagebus::{
    error::{GenericError, SendError},
    Action, Bus, Event, Message, ReciveUntypedReceiver, SendUntypedReceiver, TypeTag,
    TypeTagAccept,
};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    pin::Pin,
};

pub const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];

pub struct QuicClientRelayEndpoint {
    endpoint: quinn::Endpoint,
}

impl QuicClientRelayEndpoint {
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
    pub async fn connect(
        &self,
        addr: SocketAddr,
        host: &str,
    ) -> Result<QuicClientConnection, Error> {
        let quinn::NewConnection { connection, .. } = self.endpoint.connect(&addr, host)?.await?;
        let (send, recv) = connection.open_bi().await?;

        Ok(QuicClientConnection {
            connection,
            send,
            recv,
        })
    }

    #[inline]
    pub async fn wait_idle(&self) {
        self.endpoint.wait_idle().await;
    }
}

pub struct QuicClientConnection {
    connection: quinn::Connection,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}

impl QuicClientConnection {
    #[inline]
    pub fn send(&self, req: Request) -> Result<(), Error> {
        Ok(())
    }
}

pub struct TypeTable {
    type_tags: HashSet<TypeTag>,
}

pub struct QuicClientRelay {
    connection: QuicClientConnection,
    incoming_table: HashMap<TypeTag, Vec<(TypeTag, TypeTag)>>,
    outgoing_table: HashMap<TypeTag, Vec<(TypeTag, TypeTag)>>,
}

impl TypeTagAccept for QuicClientRelay {
    fn accept(&self, msg: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
        self.outgoing_table.get(msg).map_or(false, |v| {
            v.into_iter().any(|(r, e)| {
                resp.map_or(true, |resp| resp.as_ref() == r.as_ref())
                    && err.map_or(true, |err| err.as_ref() == e.as_ref())
            })
        })
    }

    fn iter_types(&self, cb: &mut dyn FnMut(&TypeTag, &TypeTag, &TypeTag) -> bool) {
        let iter = self
            .outgoing_table
            .iter()
            .map(|(k, v)| v.iter().map(move |(e, r)| (k, r, e)))
            .flatten();

        for (m, r, e) in iter {
            if cb(m, r, e) {
                return;
            }
        }
    }
}

#[derive(Debug)]
pub enum Request {
    Flush,
    Sync,
    Close,
    Stats,
    Send(u64, Box<dyn Message>),
    Request(u64, Box<dyn Message>),
}

impl SendUntypedReceiver for QuicClientRelay {
    fn send(&self, msg: Action, _bus: &Bus) -> Result<(), SendError<Action>> {
        self.connection
            .send(match msg {
                Action::Init => return Ok(()),
                Action::Flush => Request::Flush,
                Action::Sync => Request::Sync,
                Action::Close => Request::Close,
                Action::Stats => Request::Stats,
                _ => unimplemented!(),
            })
            .unwrap();

        Ok(())
    }

    fn send_msg(
        &self,
        mid: u64,
        msg: Box<dyn Message>,
        req: bool,
        _bus: &Bus,
    ) -> Result<(), SendError<Box<dyn Message>>> {
        self.connection
            .send(if req {
                Request::Request(mid, msg)
            } else {
                Request::Send(mid, msg)
            })
            .unwrap();

        Ok(())
    }
}
impl ReciveUntypedReceiver for QuicClientRelay {
    type Stream = Pin<Box<dyn Stream<Item = Event<Box<dyn Message>, GenericError>> + Send>>;

    fn event_stream(&self) -> Self::Stream {
        // let mut rx = self.srx.lock().take().unwrap();

        // Box::pin(futures::stream::poll_fn(move |cx|rx.poll_recv(cx)))
        Box::pin(futures::stream::poll_fn(move |_cx| {
            std::task::Poll::Pending
        }))
    }
}
