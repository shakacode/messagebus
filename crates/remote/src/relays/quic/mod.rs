mod client;
mod server;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub use client::QuicClientEndpoint;
use messagebus::{Action, Bus, Event, Message, ReciveUntypedReceiver, SendOptions, SendUntypedReceiver, TypeTag, TypeTagAccept};
use parking_lot::Mutex;
use quinn::{Connecting, IncomingBiStreams};
pub use server::QuicServerEndpoint;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::{Future, Stream, StreamExt, pin_mut};
use bytes::{Buf, BufMut};

pub const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];

use crate::proto::{BodyType, ProtocolItem, ProtocolPacket};

pub type QuicClientRelay = QuicRelay<QuicClientEndpoint>;
pub type QuicServerRelay = QuicRelay<QuicServerEndpoint>;

use super::{GenericEventStream, MessageTable};


pub trait WaitIdle<'a>: Sync {
    type Fut: Future + Send + 'a;
    fn wait_idle(&'a self) -> Self::Fut;
}

#[derive(Debug)]
enum RecvDo {
    Pause,
    Ready,
    Closed,
    Incoming(IncomingBiStreams),
}

pub struct QuicRelay<B> {
    base: Mutex<Option<B>>,
    self_id: Arc<AtomicU64>,
    outgoing_table: MessageTable,

    item_sender: UnboundedSender<Option<ProtocolItem>>,
    item_receiver: Mutex<Option<UnboundedReceiver<Option<ProtocolItem>>>>,
    event_sender: UnboundedSender<RecvDo>,
    event_receiver: Mutex<Option<UnboundedReceiver<RecvDo>>>,
}

impl QuicRelay<QuicClientEndpoint> {
    pub fn new(cert: &str, addr: SocketAddr, host: String, table: Vec<(TypeTag, TypeTag, TypeTag)>) -> Result<Self, crate::error::Error> {
        let (item_sender, item_receiver) = mpsc::unbounded_channel();
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        Ok(QuicRelay {
            base: Mutex::new(Some(QuicClientEndpoint::new(cert, addr, host)?)),
            self_id: Arc::new(AtomicU64::new(0)),
            outgoing_table: MessageTable::from(table),
            item_sender,
            item_receiver: Mutex::new(Some(item_receiver)),
            event_sender,
            event_receiver: Mutex::new(Some(event_receiver)),
        })
    }
}

impl QuicRelay<QuicServerEndpoint> {
    pub fn new(key_path: &str, cert_path: &str, addr: SocketAddr, table: Vec<(TypeTag, TypeTag, TypeTag)>) -> Result<Self, crate::error::Error> {
        let (item_sender, item_receiver) = mpsc::unbounded_channel();
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        Ok(QuicRelay {
            base: Mutex::new(Some(QuicServerEndpoint::new(key_path, cert_path, &addr )?)),
            self_id: Arc::new(AtomicU64::new(0)),
            outgoing_table: MessageTable::from(table),
            item_sender,
            item_receiver: Mutex::new(Some(item_receiver)),
            event_sender,
            event_receiver: Mutex::new(Some(event_receiver)),
        })
    }
}

impl<B> TypeTagAccept for QuicRelay<B> 
where B: Stream<Item = Connecting> + Send + 'static
{
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

impl<B> SendUntypedReceiver for QuicRelay<B> 
    where B: for<'a> WaitIdle<'a> + Stream<Item = Connecting> + Send + 'static
{
    fn send(&self, msg: Action, _bus: &Bus) -> Result<(), messagebus::error::Error<Action>> {
        match msg {
            Action::Init(self_id) => {
                let event_sender = self.event_sender.clone();
                let mut rx = self.item_receiver.lock().take().unwrap();

                let incoming = self.base.lock().take().unwrap();
                self.self_id.store(self_id, Ordering::SeqCst);

                tokio::spawn(async move {
                    pin_mut!(incoming);

                    let mut body_buff = Vec::new();
                    let mut header_buff = Vec::new();
                    let mut item = None;
                    
                    loop {
                        println!("begin");
                        let conn = match incoming.next().await {
                            Some(x) => x,
                            None => {
                                log::warn!("No more connections. Message {:?} has been lost!", item);
                                break;
                            }
                        };

                        let conn = match conn.await {
                            Ok(conn) => conn,
                            Err(err) => {
                                log::warn!("connection dropped with err {}. waiting next connection", err);
                                continue;
                            }
                        };

                        event_sender.send(RecvDo::Ready).unwrap();
                        event_sender.send(RecvDo::Incoming(conn.bi_streams)).unwrap();

                        loop {
                            let r = if let Some(r) = item.take() {
                                r
                            } else {
                                match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
                                    Ok(Some(Some(r))) => r,
                                    Ok(None) | Ok(Some(None)) => {
                                        conn.connection.close(0u32.into(), b"done");
                                        incoming.wait_idle().await;
                                        break;
                                    },
                                    Err(_) => {
                                        println!("PING");
                                        let (mut send, _) = match conn.connection.open_bi().await {
                                            Ok(x) => x,
                                            Err(err) => {
                                                println!("err {}", err);
                                                break;
                                            }
                                        };
                                        let _ = send.finish().await;
                                        continue;
                                    }
                                }
                            };

                            header_buff.clear();
                            body_buff.clear();
    
                            let (mut send, _) = match conn.connection.open_bi().await {
                                Ok(x) => x,
                                Err(err) => {
                                    println!("err {}", err);
                                    break;
                                }
                            };

                            let pkt = r.serialize(BodyType::Cbor, &mut body_buff).unwrap();
    
                            header_buff.put(&b"MBUS"[..]);
                            header_buff.put_u16(1);
                            header_buff.put_u16(0);
                            header_buff.put_u64(header_buff.len() as _);
                            serde_cbor::to_writer(&mut header_buff, &pkt).unwrap();

                            let result = send.write_all(&header_buff).await;
                            let result = if result.is_ok() {
                                send.finish().await
                            } else {
                                result
                            };
                            
                            if let Err(err) = result {
                                item = Some(r);
                                log::warn!("broken connection err {}. try with next connection", err);
                                break;
                            }

                            println!("1");
                        }

                        println!("2");

                        event_sender.send(RecvDo::Pause).unwrap();
                    }

                    println!("exit main loop");
                });
            }

            Action::Close => {
                self.item_sender.send(None).unwrap();
                self.event_sender.send(RecvDo::Closed).unwrap();
            }

            other => self.item_sender.send(Some(other.into())).unwrap(),
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
                if let Err(err) = self.item_sender.send(Some((mid, msg, req).into())) {
                    Err(messagebus::error::Error::TryAgain(err.0.unwrap().unwrap_send().unwrap().1.upcast_box()))
                } else {
                    Ok(())
                }
            }
            
            Err(msg) => Err(messagebus::error::Error::TryAgain(msg)),
        }
    }
}

impl<B> ReciveUntypedReceiver for QuicRelay<B> 
    where B: Send
{
    type Stream = GenericEventStream;

    fn event_stream(&self, bus: Bus) -> Self::Stream {
        let self_id = self.self_id.clone();
        let sender = self.item_sender.clone();
        let mut recv = self.event_receiver.lock().take().unwrap();

        Box::pin(
            futures::stream::poll_fn(move |cx|recv.poll_recv(cx))
                .map(move |recv_do| {
                    let self_id = self_id.clone();
                    let bus = bus.clone();
                    let sender = sender.clone();

                    match recv_do {
                        RecvDo::Incoming(incoming) => {
                            futures::stream::unfold((incoming, bus, sender, self_id), |(mut incoming, bus, sender, self_id)| async move {
                                loop {
                                    let (_, recv) = match incoming.next().await? {
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

                                    if buff.is_empty() {
                                        println!("PONG");
                                        continue;
                                    }
        
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
                                                            sender.send(Some(ProtocolItem::Event(Event::Exited))).unwrap();
                                                        },
                                                        Action::Flush => {
                                                            bus.flush().await;
                                                            sender.send(Some(ProtocolItem::Event(Event::Flushed))).unwrap();
                                                        },
                                                        Action::Sync => {
                                                            bus.sync().await;
                                                            sender.send(Some(ProtocolItem::Event(Event::Synchronized(Ok(()))))).unwrap();
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
                                                        
                                                        sender.send(Some(ProtocolItem::Event(Event::Response(mid, res)))).unwrap();
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

                                    return Some((event, (incoming, bus, sender, self_id)));
                                }
                            }).right_stream()
                        }

                        other => futures::stream::once(async move { 
                            match other {
                                RecvDo::Pause => Event::Pause,
                                RecvDo::Ready => Event::Ready,
                                RecvDo::Closed => Event::Exited,
                                _ => unreachable!()
                            } 
                        }).left_stream()
                    }

                    
                })
                .flatten()
        )
    }
}