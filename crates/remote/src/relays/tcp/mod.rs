use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{Buf, BufMut};
use futures::stream::unfold;
use futures::{pin_mut, Stream, StreamExt};
use messagebus::error::GenericError;
use messagebus::{
    Action, Bus, Event, EventBoxed, Message, ReciveUntypedReceiver, SendOptions,
    SendUntypedReceiver, TypeTag, TypeTagAccept, TypeTagAcceptItem,
};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::proto::{BodyType, ProtocolItem, ProtocolPacket};

use super::{GenericEventStream, MessageList, MessageTable};

pub struct TcpRelay {
    server_mode: bool,
    addr: SocketAddr,

    self_id: Arc<AtomicU64>,
    in_table: MessageTable,
    // _out_table: MessageTable,
    item_sender: UnboundedSender<Option<ProtocolItem>>,
    item_receiver: Mutex<Option<UnboundedReceiver<Option<ProtocolItem>>>>,

    event_sender: UnboundedSender<EventBoxed<GenericError>>,
    event_receiver: Mutex<Option<UnboundedReceiver<EventBoxed<GenericError>>>>,

    stream_sender: UnboundedSender<OwnedReadHalf>,
    stream_receiver: Mutex<Option<UnboundedReceiver<OwnedReadHalf>>>,
}

impl TcpRelay {
    pub fn new(server_mode: bool, addr: SocketAddr, table: (MessageList, MessageList)) -> Self {
        let (item_sender, item_receiver) = mpsc::unbounded_channel();
        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        let (stream_sender, stream_receiver) = mpsc::unbounded_channel();

        Self {
            self_id: Arc::new(AtomicU64::new(0)),
            server_mode,
            addr,
            item_sender,
            in_table: MessageTable::from(table.0),
            item_receiver: Mutex::new(Some(item_receiver)),
            event_sender,
            event_receiver: Mutex::new(Some(event_receiver)),
            stream_sender,
            stream_receiver: Mutex::new(Some(stream_receiver)),
        }
    }

    fn connections(&self) -> impl Stream<Item = TcpRelayConnection> {
        unfold(
            (self.server_mode, self.addr),
            move |(sm, addr)| async move {
                let stream = if sm {
                    let bind_res = TcpListener::bind(addr).await;
                    let listener = match bind_res {
                        Err(err) => {
                            println!("bind error: {}", err);
                            return None;
                        }

                        Ok(listener) => listener,
                    };

                    unfold((listener,), move |(listener,)| async move {
                        let (stream, _addr) = match listener.accept().await {
                            Err(err) => {
                                println!("accept error: {}", err);
                                return None;
                            }

                            Ok(listener) => listener,
                        };

                        Some((TcpRelayConnection::from(stream), (listener,)))
                    })
                    .left_stream()
                } else {
                    unfold((addr,), move |(addr,)| async move {
                        let stream = match TcpStream::connect(addr).await {
                            Err(err) => {
                                println!("connect error: {}", err);
                                return None;
                            }

                            Ok(listener) => listener,
                        };

                        Some((TcpRelayConnection::from(stream), (addr,)))
                    })
                    .right_stream()
                };

                Some((stream, (sm, addr)))
            },
        )
        .flatten()
    }
}

struct TcpRelayConnection {
    recv: OwnedReadHalf,
    send: OwnedWriteHalf,
}

impl From<TcpStream> for TcpRelayConnection {
    fn from(stream: TcpStream) -> Self {
        let (recv, send) = stream.into_split();
        TcpRelayConnection { recv, send }
    }
}

impl TypeTagAccept for TcpRelay {
    fn iter_types(&self) -> Box<dyn Iterator<Item = TypeTagAcceptItem> + '_> {
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

impl SendUntypedReceiver for TcpRelay {
    fn send(&self, msg: Action, _bus: &Bus) -> Result<(), messagebus::error::Error<Action>> {
        match msg {
            Action::Init(self_id) => {
                let mut rx = match self.item_receiver.lock().take() {
                    Some(x) => x,
                    None => return Ok(()),
                };

                let stream_sender = self.stream_sender.clone();
                let event_sender = self.event_sender.clone();

                let incoming = self.connections();
                self.self_id.store(self_id, Ordering::SeqCst);

                tokio::spawn(async move {
                    pin_mut!(incoming);

                    let mut body_buff = Vec::new();
                    let mut header_buff = Vec::new();
                    let mut item = None;

                    loop {
                        println!("begin");

                        let mut conn = match incoming.next().await {
                            Some(x) => x,
                            None => {
                                println!("No more connections. Message {:?} has been lost!", item);
                                break;
                            }
                        };

                        stream_sender.send(conn.recv).unwrap();
                        event_sender.send(Event::Ready).unwrap();

                        loop {
                            let r = if let Some(r) = item.take() {
                                r
                            } else {
                                match rx.recv().await {
                                    Some(Some(r)) => r,
                                    None | Some(None) => {
                                        println!("closing");
                                        drop(conn.send);
                                        break;
                                    }
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

                            if let Err(err) = conn.send.write_all(&header_buff).await {
                                item = Some(r);
                                println!(
                                    "write broken connection err {}. try with next connection",
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

impl ReciveUntypedReceiver for TcpRelay {
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
                    |(mut recv, bus, sender, self_id, mut buff)| async move {
                        loop {
                            buff.resize(4, 0);
                            if let Err(err) = recv.read_exact(&mut buff[..]).await {
                                println!("recv err: {}", err);
                                break None;
                            }

                            if &buff == b"PING" {
                                println!(">> PING");
                                continue;
                            }

                            if &buff != b"MBUS" {
                                println!("Not MBUS packet!");
                                continue;
                            }

                            buff.resize(12, 0);
                            if let Err(err) = recv.read_exact(&mut buff[..]).await {
                                println!("recv err: {}", err);
                                continue;
                            }

                            let mut reader = &buff[..];
                            let _version = reader.get_u16();
                            let content_type = reader.get_u16();
                            let body_size = reader.get_u64();

                            buff.resize(body_size as _, 0);
                            if let Err(err) = recv.read_exact(&mut buff[..]).await {
                                println!("recv err: {}", err);
                                continue;
                            }

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
                                                    println!("sync");
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

                            return Some((event, (recv, bus, sender, self_id, buff)));
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
