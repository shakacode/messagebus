use crate::{error::Error, proto::{BodyType, ProtocolItem, ProtocolPacket}, relays::{GenericEventStream, MessageTable}};
use futures::StreamExt;
use messagebus::{Action, Bus, Event, Message, ReciveUntypedReceiver, SendOptions, SendUntypedReceiver, TypeTag, TypeTagAccept};
use parking_lot::Mutex;
use quinn::IncomingBiStreams;
use std::{net::SocketAddr, sync::{Arc, atomic::{AtomicU64, Ordering}}};
use tokio::sync::mpsc::{self, UnboundedSender, UnboundedReceiver};
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
}

pub struct QuicClientRelay {
    // ready_flag: AtomicBool,
    self_id: Arc<AtomicU64>,
    addr: SocketAddr,
    host: String,
    endpoint: QuicClientRelayEndpoint,
    outgoing_table: MessageTable,
    sender: UnboundedSender<ProtocolItem>,
    receiver: Mutex<Option<(UnboundedReceiver<ProtocolItem>, UnboundedSender<IncomingBiStreams>)>>,
    st_receiver: Mutex<Option<UnboundedReceiver<IncomingBiStreams>>>,
}

impl QuicClientRelay {
    pub fn new(cert: &str, addr: SocketAddr, host: String, table: Vec<(TypeTag, TypeTag, TypeTag)>) -> Result<Self, Error> {
        let endpoint = QuicClientRelayEndpoint::new(cert)?;
        let (sender, receiver) = mpsc::unbounded_channel();
        let (st_sender, st_receiver) = mpsc::unbounded_channel();

        Ok(Self {
            // ready_flag: AtomicBool::new(false),
            self_id: Arc::new(AtomicU64::new(0)),
            addr,
            host,
            endpoint,
            outgoing_table: MessageTable::from(table),
            sender,
            receiver: Mutex::new(Some((receiver, st_sender))),
            st_receiver: Mutex::new(Some(st_receiver)),
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
            Action::Init(self_id) => {
                let (mut rx, recv_stream) = self.receiver.lock().take().unwrap();
                let conn = self.endpoint.endpoint.connect(&self.addr, &self.host).unwrap();
                self.self_id.store(self_id, Ordering::SeqCst);
                
                tokio::spawn(async move {
                    let mut body_buff = Vec::new();
                    let mut header_buff = Vec::new();
                    let conn = conn.await.unwrap();

                    recv_stream.send(conn.bi_streams).unwrap();
                    
                    while let Some(r) = rx.recv().await {
                        header_buff.clear();
                        body_buff.clear();

                        let (mut send, _) = conn.connection.open_bi().await.unwrap();
                        let pkt = r.serialize(BodyType::Cbor, &mut body_buff).unwrap();

                        header_buff.put(&b"MBUS"[..]);
                        header_buff.put_u16(1);
                        header_buff.put_u16(0);
                        header_buff.put_u64(header_buff.len() as _);
                        serde_cbor::to_writer(&mut header_buff, &pkt).unwrap();

                        send.write_all(&header_buff).await.unwrap();
                        send.finish().await.unwrap();

                        println!("sent");
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
        let self_id = self.self_id.clone();
        let sender = self.sender.clone();
        let mut recv = self.st_receiver.lock().take().unwrap();

        Box::pin(
            futures::stream::poll_fn(move |cx|recv.poll_recv(cx))
                .map(move |uni_streams| {
                    let self_id = self_id.clone();
                    let bus = bus.clone();
                    let sender = sender.clone();
                    // let buff = Bytes::new();

                    futures::stream::unfold((true, uni_streams, bus, sender, self_id), |(first, mut uni_streams, bus, sender, self_id)| async move {
                        loop {
                            if first {
                                return Some((Event::Ready, (false, uni_streams, bus, sender, self_id)));
                            }

                            let (_, recv) = match uni_streams.next().await? {
                                Ok(recv) => recv,
                                Err(err) => {
                                    println!("error: {}", err);
                                    return None;
                                }
                            };

                            let buff = recv
                                .read_to_end(usize::max_value())
                                .await
                                .unwrap();

                            // assert_eq!(&buff[0..4], b"MBUS");

                            let mut reader = &buff[4..];

                            let version = reader.get_u16();
                            let content_type = reader.get_u16();
                            let body_size = reader.get_u64();

                            println!("inbound packet {}: v: {}; ct: {}; bs: {}", String::from_utf8_lossy(&buff[0..4]), version, content_type, body_size);

                            let event = match content_type {
                                0 => { // CBOR
                                    let proto: ProtocolPacket = serde_cbor::from_slice(&buff[16..]).unwrap();
                                    
                                    match proto.deserialize(&bus).unwrap() {
                                        ProtocolItem::Event(ev) => ev.map_msg(|msg|msg.upcast_box()),
                                        ProtocolItem::Action(action) => {
                                            match action {
                                                Action::Close => {
                                                    println!("warning: Close recevied - ignoring!");
                                                    sender.send(ProtocolItem::Event(Event::Exited)).unwrap();
                                                },
                                                Action::Flush => {
                                                    bus.flush().await;
                                                    sender.send(ProtocolItem::Event(Event::Flushed)).unwrap();
                                                },
                                                Action::Sync => {
                                                    bus.sync().await;
                                                    sender.send(ProtocolItem::Event(Event::Synchronized(Ok(())))).unwrap();
                                                },
                                                Action::Init(..) => (),
                                                Action::Stats => (),        
                                                _ => (),                                        
                                            }
                                            continue;
                                        }
                                        ProtocolItem::Send(mid, msg, req) => {
                                            if req {
                                                let res = bus.request_boxed(
                                                    msg.upcast_box(), 
                                                SendOptions::Except(self_id.load(Ordering::SeqCst))
                                                )
                                                    .await
                                                    .map(|x|x.as_shared_boxed().unwrap())
                                                    .map_err(|x|x.map_msg(|_|()));
                                                
                                                sender.send(ProtocolItem::Event(Event::Response(mid, res))).unwrap();
                                            } else {
                                                let _ = bus.send_boxed(msg.upcast_box(), Default::default())
                                                    .await;
                                            }

                                            continue;
                                        }
                                        _ => unimplemented!()
                                    }
                                },
                                _ => unimplemented!()
                            };
                            
                            return Some((event, (false, uni_streams, bus, sender, self_id)));
                        }
                    })
                })
                .flatten()
        )
    }
}
