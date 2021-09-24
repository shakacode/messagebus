use crate::{error::Error, proto::{BodyType, ProtocolItem, ProtocolPacket}, relays::{GenericEventStream, MessageTable}};
use futures::{Future, FutureExt, StreamExt};
use messagebus::{Action, Bus, Event, Message, ReciveUntypedReceiver, SendUntypedReceiver, TypeTag, TypeTagAccept};
use parking_lot::Mutex;
use std::{net::SocketAddr, sync::{Arc, atomic::AtomicBool}};
use tokio::{sync::{mpsc::{self, UnboundedSender, UnboundedReceiver}, oneshot}};
use bytes::{Buf, BufMut};

pub const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];

pub struct QuicServerRelayEndpoint {
    endpoint: quinn::Endpoint,
    incoming: Mutex<Option<quinn::Incoming>>,
}

impl QuicServerRelayEndpoint {
    pub fn new(key_path: &str, cert_path: &str, addr: &SocketAddr) -> Result<Self, Error> {
        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_concurrent_uni_streams(0)?;

        let mut server_config = quinn::ServerConfig::default();
        server_config.transport = Arc::new(transport_config);
        
        let mut server_config = quinn::ServerConfigBuilder::new(server_config);
        
        server_config.protocols(ALPN_QUIC_HTTP);
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

        Ok(Self { endpoint, incoming: Mutex::new(Some(incoming)) })
    }

    pub fn incoming(&self) -> impl Future<Output = Result<QuicServerConnection, Error>> {
        let mut conn = self.incoming.lock().take().unwrap();

        async move {
            let conn = conn.next().await.unwrap();
            let quinn::NewConnection { connection, .. } = conn.await?;
            
            Ok(QuicServerConnection {
                connection,
            })
        }
    }

    pub fn connect(
        &self,
        addr: &SocketAddr,
        host: &str,
    ) -> impl Future<Output = Result<QuicServerConnection, Error>> {
        let conn = self.endpoint.connect(addr, host);
        
        async move {
            let quinn::NewConnection { connection, .. } = conn?.await?;

            Ok(QuicServerConnection {
                connection,
            })
        }
    }

    #[inline]
    pub async fn wait_idle(&self) {
        self.endpoint.wait_idle().await;
    }
}

pub struct QuicServerConnection {
    connection: quinn::Connection,
}

pub struct QuicServerRelay {
    ready_flag: AtomicBool,
    endpoint: QuicServerRelayEndpoint,
    outgoing_table: MessageTable,
    sender: UnboundedSender<ProtocolItem>,
    receiver_send: Mutex<Option<(UnboundedSender<quinn::RecvStream>, UnboundedReceiver<ProtocolItem>)>>,
    receiver_recv: Mutex<Option<UnboundedReceiver<quinn::RecvStream>>>,
}

impl QuicServerRelay {
    pub fn new(key_path: &str, cert_path: &str, addr: SocketAddr, table: Vec<(TypeTag, TypeTag, TypeTag)>) -> Result<Self, Error> {
        let endpoint = QuicServerRelayEndpoint::new(key_path, cert_path, &addr)?;
        let (sender, receiver) = mpsc::unbounded_channel();
        let (recv_send, recv_recv) = mpsc::unbounded_channel();

        Ok(Self {
            ready_flag: AtomicBool::new(false),
            endpoint,
            outgoing_table: MessageTable::from(table),
            sender,
            receiver_send: Mutex::new(Some((recv_send, receiver))),
            receiver_recv: Mutex::new(Some(recv_recv)),
        })
    }
}

impl TypeTagAccept for QuicServerRelay {
    fn accept(&self, msg: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
        self.outgoing_table.accept(msg, resp, err)
    }

    fn iter_types(&self, cb: &mut dyn FnMut(&TypeTag, &TypeTag, &TypeTag) -> bool) {
        let iter = self.outgoing_table.iter_types();

        for (m, r, e) in iter {
            if cb(m, r, e) {
                return;
            }
        }
    }
}

impl SendUntypedReceiver for QuicServerRelay {
    fn send(&self, msg: Action, _bus: &Bus) -> Result<(), messagebus::error::Error<Action>> {
        match msg {
            Action::Init => {
                let (sender, mut rx) = self.receiver_send.lock().take().unwrap();
                let conn = self.endpoint.incoming();

                tokio::spawn(async move {
                    let conn = conn.await.unwrap();

                    let mut body_buff = Vec::new();
                    let mut header_buff = Vec::new();

                    while let Some(r) = rx.recv().await {
                        let (mut send, recv) = conn.connection.open_bi().await.unwrap();

                        sender.send(recv).unwrap();

                        body_buff.clear();
                        header_buff.clear();
                        
                        let pkt = r.serialize(BodyType::Cbor, &mut body_buff).unwrap();
                        serde_cbor::to_writer(&mut header_buff, &pkt).unwrap();

                        let mut buf = [0u8; 16];
                        let mut writer = &mut buf[..];

                        writer.put(&b"MBUS"[..]);
                        writer.put_u16(1);
                        writer.put_u16(0);
                        writer.put_u64(header_buff.len() as _);

                        send.write_all(&buf).await.unwrap();
                        send.write_all(&header_buff).await.unwrap();
                    }
                });
            }

            other => self.sender.send(other.into()).unwrap(),
        }

        Ok(())
    }

    fn send_msg(
        &self,
        mid: u64,
        msg: Box<dyn Message>,
        req: bool,
        _bus: &Bus,
    ) -> Result<(), messagebus::error::Error<Box<dyn Message>>> {
        match msg.as_shared_boxed() {
            Ok(msg) => {
                if let Err(err) = self.sender.send((mid, msg, req).into()) {
                    Err(messagebus::error::Error::TryAgain(err.0.unwrap_send().unwrap().1.upcast_box()))
                } else {
                    Ok(())
                }
            }
            
            Err(msg) => Err(messagebus::error::Error::TryAgain(msg)),
        }
    }
}

impl ReciveUntypedReceiver for QuicServerRelay {
    type Stream = GenericEventStream;

    fn event_stream(&self, bus: Bus) -> Self::Stream {
        let mut recv = self.receiver_recv.lock().take().unwrap();

        Box::pin(futures::stream::poll_fn(move |cx|recv.poll_recv(cx))
            .map(move |recv| {
                let buff = Vec::with_capacity(16);
                let bus = bus.clone();

                futures::stream::unfold(
                    (true, recv, bus, buff),
                    |(first, mut recv, bus, mut buff)| async move {
                        if first {
                            return Some((Event::Ready, (false, recv, bus, buff)));
                        }

                        println!("1");

                        unsafe { buff.set_len(16) };
                        recv.read_exact(&mut buff).await.unwrap();

                        println!("{:?}", buff);

                        let mut reader = &buff[..];
                        let mut sign = [0u8; 4];
                        reader.copy_to_slice(&mut sign);
                        assert!(&sign != b"MBUS");

                        let version = reader.get_u16();
                        assert!(version == 1);

                        let content_type = reader.get_u16();

                        let body_size = reader.get_u64();
                        let diff = buff.capacity() as i64 - body_size as i64;
                        if diff < 0 {
                            buff.reserve(-diff as usize);
                        }

                        unsafe { buff.set_len(body_size as usize); }
                        recv.read_exact(&mut buff).await.unwrap();

                        println!("{:?}", buff);

                        let event = match content_type {
                            0 => { // CBOR
                                let proto: ProtocolPacket = serde_cbor::from_slice(&buff).unwrap();
                                
                                match proto.deserialize(&bus).unwrap() {
                                    ProtocolItem::Event(ev) => ev.map_msg(|msg|msg.upcast_box()),
                                    _ => unimplemented!()
                                }
                            },
                            _ => unimplemented!()
                        };
        
                        Some((event, (false, recv, bus, buff)))
                    },
                )
            })
            .flatten()
        )
    }
}