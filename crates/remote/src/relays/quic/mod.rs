mod client;
mod server;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{Buf, BufMut};
pub use client::QuicClientEndpoint;
use futures::{pin_mut, Future, Stream, StreamExt};
use messagebus::error::GenericError;
use messagebus::{
    Action, Bus, Event, EventBoxed, Message, ReciveUntypedReceiver, SendOptions,
    SendUntypedReceiver, TypeTag, TypeTagAccept,
};
use parking_lot::Mutex;
use quinn::{Connecting, IncomingBiStreams};
pub use server::QuicServerEndpoint;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

pub const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];

use crate::proto::{BodyType, ProtocolItem, ProtocolPacket};

pub type QuicClientRelay = QuicRelay<QuicClientEndpoint>;
pub type QuicServerRelay = QuicRelay<QuicServerEndpoint>;

use super::{GenericEventStream, MessageTable};
pub type MessageList = Vec<(TypeTag, Option<(TypeTag, TypeTag)>)>;

pub trait WaitIdle<'a>: Sync {
    type Fut: Future + Send + 'a;
    fn wait_idle(&'a self) -> Self::Fut;
}

pub struct QuicRelay<B> {
    base: Mutex<Option<B>>,
    self_id: Arc<AtomicU64>,
    in_table: MessageTable,
    _out_table: MessageTable,

    item_sender: UnboundedSender<Option<ProtocolItem>>,
    item_receiver: Mutex<Option<UnboundedReceiver<Option<ProtocolItem>>>>,

    event_sender: UnboundedSender<EventBoxed<GenericError>>,
    event_receiver: Mutex<Option<UnboundedReceiver<EventBoxed<GenericError>>>>,

    stream_sender: UnboundedSender<IncomingBiStreams>,
    stream_receiver: Mutex<Option<UnboundedReceiver<IncomingBiStreams>>>,
}

impl QuicRelay<QuicClientEndpoint> {
    pub fn new(
        cert: &str,
        addr: SocketAddr,
        host: String,
        table: (MessageList, MessageList),
    ) -> Result<Self, crate::error::Error> {
        let (item_sender, item_receiver) = mpsc::unbounded_channel();
        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        let (stream_sender, stream_receiver) = mpsc::unbounded_channel();

        Ok(QuicRelay {
            base: Mutex::new(Some(QuicClientEndpoint::new(cert, addr, host)?)),
            self_id: Arc::new(AtomicU64::new(0)),
            in_table: MessageTable::from(table.0),
            _out_table: MessageTable::from(table.1),
            item_sender,
            item_receiver: Mutex::new(Some(item_receiver)),
            event_sender,
            event_receiver: Mutex::new(Some(event_receiver)),
            stream_sender,
            stream_receiver: Mutex::new(Some(stream_receiver)),
        })
    }
}

impl QuicRelay<QuicServerEndpoint> {
    pub fn new(
        key_path: &str,
        cert_path: &str,
        addr: SocketAddr,
        table: (MessageList, MessageList),
    ) -> Result<Self, crate::error::Error> {
        let (item_sender, item_receiver) = mpsc::unbounded_channel();
        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        let (stream_sender, stream_receiver) = mpsc::unbounded_channel();

        Ok(QuicRelay {
            base: Mutex::new(Some(QuicServerEndpoint::new(key_path, cert_path, &addr)?)),
            self_id: Arc::new(AtomicU64::new(0)),
            in_table: MessageTable::from(table.0),
            _out_table: MessageTable::from(table.1),
            item_sender,
            item_receiver: Mutex::new(Some(item_receiver)),
            event_sender,
            event_receiver: Mutex::new(Some(event_receiver)),
            stream_sender,
            stream_receiver: Mutex::new(Some(stream_receiver)),
        })
    }
}

impl<B> TypeTagAccept for QuicRelay<B>
where
    B: Stream<Item = Connecting> + Send + 'static,
{
    fn iter_types(&self) -> Box<dyn Iterator<Item = (TypeTag, Option<(TypeTag, TypeTag)>)> + '_> {
        let iter = self.in_table.iter_types();
        Box::new(iter.map(|(x, y)| (x.clone(), y.cloned())))
    }

    fn accept_msg(&self, msg: &TypeTag) -> bool {
        self.in_table.accept_message(msg)
    }

    fn accept_req(&self, req: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
        self.in_table.accept_request(req, resp, err)
    }
}

impl<B> SendUntypedReceiver for QuicRelay<B>
where
    B: for<'a> WaitIdle<'a> + Stream<Item = Connecting> + Send + 'static,
{
    fn send(&self, msg: Action, _bus: &Bus) -> Result<(), messagebus::error::Error<Action>> {
        match msg {
            Action::Init(self_id) => {
                let mut rx = match self.item_receiver.lock().take() {
                    Some(x) => x,
                    None => return Ok(()),
                };

                let stream_sender = self.stream_sender.clone();
                let event_sender = self.event_sender.clone();

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
                                println!("No more connections. Message {:?} has been lost!", item);
                                break;
                            }
                        };

                        let conn = match conn.await {
                            Ok(conn) => conn,
                            Err(err) => {
                                println!(
                                    "connection dropped with err {}. waiting next connection",
                                    err
                                );
                                continue;
                            }
                        };

                        stream_sender.send(conn.bi_streams).unwrap();
                        event_sender.send(Event::Ready).unwrap();

                        loop {
                            let r = if let Some(r) = item.take() {
                                r
                            } else {
                                match rx.recv().await {
                                    Some(Some(r)) => r,
                                    None | Some(None) => {
                                        println!("closing");
                                        conn.connection.close(0u32.into(), b"done");
                                        incoming.wait_idle().await;
                                        break;
                                    }
                                }

                                // match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
                                //     Ok(Some(Some(r))) => r,
                                //     Ok(None) | Ok(Some(None)) => {
                                //         conn.connection.close(0u32.into(), b"done");
                                //         incoming.wait_idle().await;
                                //         break;
                                //     },
                                //     Err(_) => {
                                //         // println!("{:?}", err);
                                //         // let (mut send, _) = match conn.connection.open_bi().await {
                                //         //     Ok(x) => x,
                                //         //     Err(err) => {
                                //         //         println!("err {}", err);
                                //         //         break;
                                //         //     }
                                //         // };
                                //         // println!("<< PING");
                                //         // let _ = send.write_all(b"PING").await.unwrap();
                                //         // let _ = send.finish().await;
                                //         continue;
                                //     }
                                // }
                            };

                            let (mut send, _) = match conn.connection.open_bi().await {
                                Ok(x) => x,
                                Err(err) => {
                                    println!("err {}", err);
                                    break;
                                }
                            };

                            body_buff.clear();
                            let pkt = r.serialize(BodyType::Cbor, &mut body_buff).unwrap();

                            header_buff.resize(16, 0);
                            serde_cbor::to_writer(&mut header_buff, &pkt).unwrap();
                            let body_size = header_buff.len() - 16;

                            let mut head = &mut header_buff[0..16];

                            head.put(&b"MBUS"[..]);
                            head.put_u16(1);
                            head.put_u16(0);
                            head.put_u64(body_size as _);

                            if let Err(err) = send.write_all(&header_buff).await {
                                item = Some(r);
                                println!(
                                    "write broken connection err {}. try with next connection",
                                    err
                                );
                                break;
                            }

                            if let Err(err) = send.finish().await {
                                item = Some(r);
                                println!(
                                    "finish broken connection err {}. try with next connection",
                                    err
                                );
                                break;
                            }
                        }

                        event_sender.send(Event::Pause).unwrap();
                    }

                    println!("exit main loop");
                });
            }

            Action::Close => {
                self.item_sender.send(None).unwrap();
                self.event_sender.send(Event::Exited).unwrap();
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
                    Err(messagebus::error::Error::TryAgain(
                        err.0.unwrap().unwrap_send().unwrap().1.upcast_box(),
                    ))
                } else {
                    Ok(())
                }
            }

            Err(msg) => Err(messagebus::error::Error::TryAgain(msg)),
        }
    }
}

impl<B> ReciveUntypedReceiver for QuicRelay<B>
where
    B: Send,
{
    type Stream = GenericEventStream;

    fn event_stream(&self, bus: Bus) -> Self::Stream {
        let self_id = self.self_id.clone();

        let mut recv_stream = self.stream_receiver.lock().take().unwrap();
        let mut recv_events = self.event_receiver.lock().take().unwrap();
        let sender = self.item_sender.clone();

        let stream1 = futures::stream::poll_fn(move |cx| recv_stream.poll_recv(cx))
            .map(move |incoming| {
                let buff: Vec<u8> = Vec::with_capacity(1024);
                let bus = bus.clone();
                let self_id = self_id.clone();
                let sender = sender.clone();

                futures::stream::unfold(
                    (incoming, bus, sender, self_id, buff),
                    |(mut incoming, bus, sender, self_id, mut buff)| async move {
                        loop {
                            let (_, mut recv) = match incoming.next().await? {
                                Ok(recv) => recv,
                                Err(err) => {
                                    println!("error: {}", err);
                                    return None;
                                }
                            };

                            buff.resize(4, 0);
                            if let Err(err) = recv.read_exact(&mut buff[..]).await {
                                println!("recv err: {}", err);
                                continue;
                            }

                            let verb = match std::str::from_utf8(&buff[0..4]) {
                                Ok(m) => m,
                                Err(err) => {
                                    println!("recv err parse: {}", err);
                                    continue;
                                }
                            };

                            if verb == "PING" {
                                println!(">> PING");
                                continue;
                            }

                            if verb != "MBUS" {
                                println!("Not MBUS packet!");
                                continue;
                            }

                            buff.resize(12, 0);
                            if let Err(err) = recv.read_exact(&mut buff[..]).await {
                                println!("recv err: {}", err);
                                continue;
                            }

                            let mut reader = &buff[..];
                            let version = reader.get_u16();
                            let content_type = reader.get_u16();
                            let body_size = reader.get_u64();

                            buff.resize(body_size as _, 0);
                            if let Err(err) = recv.read_exact(&mut buff[..]).await {
                                println!("recv err: {}", err);
                                continue;
                            }

                            drop(recv);

                            // println!("inbound packet MBUS v: {}; ct: {}; bs: {}",
                            //     version, content_type, body_size);

                            let event = match content_type {
                                0 => {
                                    // CBOR
                                    let proto: ProtocolPacket =
                                        match serde_cbor::from_slice(&buff[..]) {
                                            Ok(val) => val,
                                            Err(err) => {
                                                println!("pkt parse err: {}", err);
                                                continue;
                                            }
                                        };

                                    let item: ProtocolItem = match proto.deserialize(&bus) {
                                        Ok(val) => val,
                                        Err(err) => {
                                            println!("item parse err: {}", err);
                                            continue;
                                        }
                                    };

                                    match item {
                                        ProtocolItem::Event(ev) => {
                                            ev.map_msg(|msg| msg.upcast_box())
                                        }
                                        ProtocolItem::Action(action) => {
                                            match action {
                                                Action::Close => {
                                                    println!("warning: Close recevied - ignoring!");
                                                    sender
                                                        .send(Some(ProtocolItem::Event(
                                                            Event::Exited,
                                                        )))
                                                        .unwrap();
                                                }
                                                Action::Flush => {
                                                    println!("flush");
                                                    bus.flush_all().await;
                                                    sender
                                                        .send(Some(ProtocolItem::Event(
                                                            Event::Flushed,
                                                        )))
                                                        .unwrap();
                                                }
                                                Action::Sync => {
                                                    println!("flush");
                                                    bus.sync_all().await;
                                                    sender
                                                        .send(Some(ProtocolItem::Event(
                                                            Event::Synchronized(Ok(())),
                                                        )))
                                                        .unwrap();
                                                }
                                                Action::Init(..) => (),
                                                Action::Stats => (),
                                                _ => (),
                                            }
                                            continue;
                                        }
                                        ProtocolItem::Send(mid, msg, req) => {
                                            let self_id = self_id.clone();
                                            let sender = sender.clone();
                                            let bus = bus.clone();

                                            let _ = tokio::spawn(async move {
                                                if req {
                                                    let res = bus
                                                        .request_boxed(
                                                            msg.upcast_box(),
                                                            SendOptions::Except(
                                                                self_id.load(Ordering::SeqCst),
                                                            ),
                                                        )
                                                        .await
                                                        .map(|x| x.as_shared_boxed().unwrap())
                                                        .map_err(|x| x.map_msg(|_| ()));

                                                    sender
                                                        .send(Some(ProtocolItem::Event(
                                                            Event::Response(mid, res),
                                                        )))
                                                        .unwrap();
                                                } else {
                                                    let tt = msg.type_tag();
                                                    let _ = bus
                                                        .send_boxed(
                                                            msg.upcast_box(),
                                                            Default::default(),
                                                        )
                                                        .await;

                                                    sender
                                                        .send(Some(ProtocolItem::Event(
                                                            Event::BatchComplete(tt, 1),
                                                        )))
                                                        .unwrap();
                                                }
                                            });

                                            continue;
                                        }
                                        _ => unimplemented!(),
                                    }
                                }
                                _ => unimplemented!(),
                            };

                            return Some((event, (incoming, bus, sender, self_id, buff)));
                        }
                    },
                )
            })
            .flatten();

        let stream2 = futures::stream::poll_fn(move |cx| recv_events.poll_recv(cx));
        Box::pin(
            futures::stream::select(stream1, stream2)
                .take_while(|x| futures::future::ready(!matches!(x, Event::Exited))),
        )
    }
}

