use crate::{error::Error, proto::{BodyType, ProtocolItem, ProtocolPacket}, relays::{GenericEventStream, MessageTable}};
use futures::{Future, FutureExt};
use messagebus::{Action, Bus, Event, Message, ReciveUntypedReceiver, SendUntypedReceiver, TypeTag, TypeTagAccept};
use parking_lot::Mutex;
use std::{net::SocketAddr, sync::atomic::AtomicBool};
use tokio::{sync::{mpsc::{self, UnboundedSender, UnboundedReceiver}, oneshot}};
use bytes::{Buf, BufMut};

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

    pub fn connect(
        &self,
        addr: &SocketAddr,
        host: &str,
    ) -> impl Future<Output = Result<QuicClientConnection, Error>> {
        let conn = self.endpoint.connect(addr, host);
        
        async move {
            let quinn::NewConnection { connection, .. } = conn?.await?;
            let (send, recv) = connection.open_bi().await?;

            Ok(QuicClientConnection {
                connection,
                send,
                recv,
            })
        }
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

pub struct QuicClientRelay {
    ready_flag: AtomicBool,
    addr: SocketAddr,
    host: String,
    endpoint: QuicClientRelayEndpoint,
    outgoing_table: MessageTable,
    sender: UnboundedSender<ProtocolItem>,
    receiver_send: Mutex<Option<(oneshot::Sender<(quinn::RecvStream, quinn::Connection)>, UnboundedReceiver<ProtocolItem>)>>,
    receiver_recv: Mutex<Option<oneshot::Receiver<(quinn::RecvStream, quinn::Connection)>>>,
}

impl QuicClientRelay {
    pub fn new(cert: &str, addr: SocketAddr, host: String, table: Vec<(TypeTag, TypeTag, TypeTag)>) -> Result<Self, Error> {
        let endpoint = QuicClientRelayEndpoint::new(cert)?;
        let (sender, receiver) = mpsc::unbounded_channel();
        let (recv_send, recv_recv) = oneshot::channel();

        Ok(Self {
            ready_flag: AtomicBool::new(false),
            addr,
            host,
            endpoint,
            outgoing_table: MessageTable::from(table),
            sender,
            receiver_send: Mutex::new(Some((recv_send, receiver))),
            receiver_recv: Mutex::new(Some(recv_recv)),
        })
    }
}

impl TypeTagAccept for QuicClientRelay {
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

impl SendUntypedReceiver for QuicClientRelay {
    fn send(&self, msg: Action, _bus: &Bus) -> Result<(), messagebus::error::Error<Action>> {
        match msg {
            Action::Init => {
                let (sender, mut rx) = self.receiver_send.lock().take().unwrap();
                let conn = self.endpoint.connect(&self.addr, &self.host);

                tokio::spawn(async move {
                    println!("spawn");
                    let mut conn = conn.await.unwrap();
                    sender.send((conn.recv, conn.connection)).unwrap();
                    let mut body_buff = Vec::new();
                    let mut header_buff = Vec::new();

                    while let Some(r) = rx.recv().await {
                        body_buff.clear();
                        header_buff.clear();
                        
                        let pkt = r.serialize(BodyType::Cbor, &mut body_buff).unwrap();
                        serde_cbor::to_writer(&mut header_buff, &pkt).unwrap();

                        println!("msg {:?}", pkt);

                        let mut buf = [0u8; 16];
                        let mut writer = &mut buf[..];

                        writer.put(&b"MBUS"[..]);
                        writer.put_u16(1);
                        writer.put_u16(0);
                        writer.put_u64(header_buff.len() as _);

                        conn.send.write_all(&buf).await.unwrap();
                        println!("header sent");
                        conn.send.write_all(&header_buff).await.unwrap();
                        println!("body sent");
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

impl ReciveUntypedReceiver for QuicClientRelay {
    type Stream = GenericEventStream;

    fn event_stream(&self, bus: Bus) -> Self::Stream {
        let recv = self.receiver_recv.lock().take().unwrap();

        Box::pin(async move {
            let buff = Vec::with_capacity(1024);
            let (recv, conn) = recv.await.unwrap();
            futures::stream::unfold(
                (true, recv, conn, bus, buff),
                |(first, mut recv, conn, bus, mut buff)| async move {
                    if first {
                        return Some((Event::Ready, (false, recv, conn, bus, buff)));
                    }
                    
                    unsafe { buff.set_len(16) };
                    recv.read_exact(&mut buff).await.unwrap();

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
    
                    Some((event, (false, recv, conn, bus, buff)))
                },
            )
        }.flatten_stream())
    }
}
