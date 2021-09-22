use crate::{
    error::Error,
    relays::{GenericEventStream, MessageTable},
};
use futures::{Future, FutureExt, Stream, pin_mut};
use messagebus::{Action, Bus, Event, Message, ReciveUntypedReceiver, SendUntypedReceiver, SharedMessage, TypeTag, TypeTagAccept, TypeTagged, error::{GenericError, SendError}};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_derive::{Deserialize, Serialize};
use core::slice::SlicePattern;
use std::{collections::{HashMap, HashSet}, net::SocketAddr, pin::Pin, sync::atomic::AtomicBool, task::Poll};
use tokio::{io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf}, sync::{mpsc::{self, UnboundedSender, UnboundedReceiver}, oneshot}};
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
    sender: UnboundedSender<Request>,
    receiver_send: Mutex<Option<(oneshot::Sender<(quinn::RecvStream, quinn::Connection)>, UnboundedReceiver<Request>)>>,
    receiver_recv: Mutex<Option<oneshot::Receiver<(quinn::RecvStream, quinn::Connection)>>>,
}

impl QuicClientRelay {
    pub fn new(cert: &str, addr: SocketAddr, host: String, table: Vec<(TypeTag, TypeTag, TypeTag)>) -> Result<Self, Error> {
        let endpoint = QuicClientRelayEndpoint::new(cert)?;
        let mut outgoing_table = MessageTable::from(table);
        let (sender, receiver) = mpsc::unbounded_channel();
        let (recv_send, recv_recv) = oneshot::channel();

        Ok(Self {
            ready_flag: AtomicBool::new(false),
            addr,
            host,
            endpoint,
            outgoing_table,
            sender,
            receiver_send: Mutex::new(Some((recv_send, receiver))),
            receiver_recv: Mutex::new(Some(recv_recv)),
        })
    }
}

#[derive(Deserialize, Serialize)]
#[repr(u16)]
pub enum ProtocolHeaderActionKind {
    Nop,
    Send,
    Response,
    Flush,
    Flushed,
    Synchronize,
    Synchronized,
    BatchComplete,
    Close,
    Exited,
    Initialize,
    Ready,
    Pause,
    Paused,
    Error,
}

#[derive(Deserialize, Serialize)]
pub struct ProtocolHeader<'a> {
    kind: ProtocolHeaderActionKind,
    type_tag: Option<&'a [u8]>,
    failed: bool,
    body_encoding: u32,
    argument: u64,
}

impl<'a> ProtocolHeader<'a> {
    pub fn send(mid: u64, tt: &'a TypeTag) -> ProtocolHeader<'a> {
        ProtocolHeader {
            kind: ProtocolHeaderActionKind::Send,
            type_tag: Some(tt.as_bytes()),
            failed: false,
            body_encoding: 0,
            argument: mid,
        }
    }

    pub fn flush() -> Self {
        ProtocolHeader {
            kind: ProtocolHeaderActionKind::Flush,
            type_tag: None,
            failed: false,
            body_encoding: 0,
            argument: 0,
        }
    }

    pub fn close() -> Self {
        ProtocolHeader {
            kind: ProtocolHeaderActionKind::Close,
            type_tag: None,
            failed: false,
            body_encoding: 0,
            argument: 0,
        }
    }

    pub fn sync() -> Self {
        ProtocolHeader {
            kind: ProtocolHeaderActionKind::Synchronize,
            type_tag: None,
            failed: false,
            body_encoding: 0,
            argument: 0,
        }
    }

    pub fn init() -> Self {
        ProtocolHeader {
            kind: ProtocolHeaderActionKind::Initialize,
            type_tag: None,
            failed: false,
            body_encoding: 0,
            argument: 0,
        }
    }

    pub fn pause() -> Self {
        ProtocolHeader {
            kind: ProtocolHeaderActionKind::Pause,
            type_tag: None,
            failed: false,
            body_encoding: 0,
            argument: 0,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct ProtocolPacket<'a> {
    header: ProtocolHeader<'a>,
    body: &'a [u8]
}

#[derive(Debug)]
pub enum Request {
    Init,
    Flush,
    Sync,
    Close,
    Stats,
    Send(u64, Box<dyn SharedMessage>, bool),
}

impl Request {
    pub async fn serialize<W: AsyncWrite + Unpin>(&self, mut header_buff: &mut Vec<u8>, body_buff: &mut Vec<u8>, conn: &mut W) -> Result<(), Error> {
        body_buff.clear();
        header_buff.clear();

        let mut tt = TypeTag::Borrowed("");
        let header = match self {
            Request::Init => unimplemented!(),
            Request::Flush => ProtocolHeader::flush(),
            Request::Sync => ProtocolHeader::sync(),
            Request::Close => ProtocolHeader::close(),
            Request::Stats => unimplemented!(),
            Request::Send(mid, msg, req) => {
                tt = msg.type_tag();
                
                let mut cbor_se = serde_cbor::Serializer::new(&mut *body_buff);
                let mut se = <dyn erased_serde::Serializer>::erase(&mut cbor_se);

                msg.erased_serialize(&mut se);

                ProtocolHeader::send(if *req {*mid} else {0}, &tt)
            }
        };

        serde_cbor::to_writer(&mut header_buff, &ProtocolPacket {
            header,
            body: body_buff.as_slice(),
        }).unwrap();

        let mut buf = [0u8; 16];
        let mut writer = &mut buf[..];

        writer.put(&b"MBUS"[..]);
        writer.put_u16(1);
        writer.put_u16(0);
        writer.put_u64(header_buff.len() as _);

        conn.write_all(&buf).await?;
        conn.write_all(&header_buff).await?;

        Ok(())
        // buff.
        
       
        // let x = match self {
        //     Request::Init => unimplemented!(),
        //     Request::Flush => ProtocolHeader::Flush,
        //     Request::Sync => ProtocolHeader::Synchronize,
        //     Request::Close => ProtocolHeader::Close,
        //     Request::Stats => unimplemented!(),
        //     Request::Send(_mid, msg) => {
        //         let ser = serde_cbor::Serializer::new(buff);
                
        //         ProtocolHeader::Send(msg.type_tag())
        //     }

        //     Request::Request(mid, msg) => {
        //         ProtocolHeader::Request(*mid, msg.type_tag())
        //     }
        // };
    }
}

impl From<Action> for Request {
    fn from(action: Action) -> Self {
        match action {
            Action::Init => Request::Init,
            Action::Flush => Request::Flush,
            Action::Sync => Request::Sync,
            Action::Close => Request::Close,
            Action::Stats => Request::Stats,
            _ => unimplemented!(),
        }
    }
}

impl From<(bool, u64, Box<dyn SharedMessage>)> for Request {
    fn from((req, mid, msg): (bool, u64, Box<dyn SharedMessage>)) -> Self {
        Request::Send(mid, msg, req)
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
                    let mut conn = conn.await.unwrap();
                    sender.send((conn.recv, conn.connection)).unwrap();
                    let mut buf1 = Vec::new();
                    let mut buf2 = Vec::new();

                    while let Some(r) = rx.recv().await {
                        r.serialize(&mut buf1, &mut buf2, &mut conn.send);
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
        if let Ok(val) = msg.as_shared_boxed() {
            self.sender.send((req, mid, msg).into()).unwrap();
            Ok(())
        } else {
            Err(SendError:)
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
                            
                            use ProtocolHeaderActionKind::*;
                            match proto.header.kind {
                                Nop => unimplemented!(),
                                Response => {
                                    let tt = String::from_utf8_lossy(proto.header.type_tag.unwrap()).to_string();
                                    
                                    let res = if proto.header.failed {
                                        Err(
                                            messagebus::error::Error::Other(GenericError {
                                                type_tag: tt.into(),
                                                description: "unknown".into()
                                            }
                                        ))
                                    } else {
                                        let mut cbor_de = serde_cbor::Deserializer::from_slice(proto.body);
                                        let mut de = <dyn erased_serde::Deserializer>::erase(&mut cbor_de);

                                        bus.deserialize_message(tt.into(), &mut de)
                                            .map_err(|x| x.map_msg(|_| ()))
                                    };

                                    Event::Response(proto.header.argument, res)
                                }
                                Ready => {
                                    if proto.header.failed {
                                        let tt = String::from_utf8_lossy(proto.header.type_tag.unwrap()).to_string();
                                        Event::InitFailed(messagebus::error::Error::Other(GenericError {
                                            type_tag: tt.into(),
                                            description: "unknown".into()
                                        }))
                                    } else {
                                        Event::Ready
                                    }
                                }
                                Exited => Event::Exited,
                                Pause => Event::Pause,
                                BatchComplete => Event::Finished(proto.header.argument),
                                Flushed => Event::Flushed,
                                Error => {
                                    let tt = String::from_utf8_lossy(proto.header.type_tag.unwrap()).to_string();
                                    Event::Error(GenericError {
                                        type_tag: tt.into(),
                                        description: "unknown".into()
                                    })
                                }
                                Synchronized => {
                                    if proto.header.failed {
                                        let tt = String::from_utf8_lossy(proto.header.type_tag.unwrap()).to_string();
                                        Event::Synchronized( Err(messagebus::error::Error::Other(GenericError {
                                            type_tag: tt.into(),
                                            description: "unknown".into()
                                        })))
                                    } else {
                                        Event::Synchronized(Ok(()))
                                    }
                                },

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
