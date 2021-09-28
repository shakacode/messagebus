
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use messagebus::{Action, Bus, Event, Message, ReciveUntypedReceiver, SendUntypedReceiver, TypeTag, TypeTagAccept};
use parking_lot::Mutex;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::StreamExt;

use crate::proto::{BodyType, ProtocolItem};
use super::{GenericEventStream, MessageTable};
use redis::AsyncCommands;


#[derive(Debug)]
enum RecvDo {
    Pause,
    Ready,
    Closed,
}


pub struct RedisRelay {
    client: Arc<redis::Client>,
    self_id: Arc<AtomicU64>,
    table: MessageTable,
    item_sender: UnboundedSender<Option<ProtocolItem>>,
    item_receiver: Mutex<Option<UnboundedReceiver<Option<ProtocolItem>>>>,
    event_sender: UnboundedSender<RecvDo>,
    event_receiver: Mutex<Option<UnboundedReceiver<RecvDo>>>,
}

impl RedisRelay {
    pub fn new(path: &str, table: Vec<(TypeTag, TypeTag, TypeTag)>) -> Result<Self, crate::error::Error> {
        let client = redis::Client::open(path)?;

        let (item_sender, item_receiver) = mpsc::unbounded_channel();
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        Ok(RedisRelay {
            client: Arc::new(client),
            self_id: Arc::new(AtomicU64::new(0)),
            table: MessageTable::from(table),
            item_sender,
            item_receiver: Mutex::new(Some(item_receiver)),
            event_sender,
            event_receiver: Mutex::new(Some(event_receiver)),
        })
    }
}

impl TypeTagAccept for RedisRelay {
    fn accept(&self, msg: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
        self.table.accept(msg, resp, err)
    }

    fn iter_types(&self, cb: &mut dyn FnMut(&TypeTag, &TypeTag, &TypeTag) -> bool) {
        let iter = self.table.iter_types();

        for (m, r, e) in iter {
            if cb(m, r, e) {
                return;
            }
        }
    }
}

impl SendUntypedReceiver for RedisRelay {
    fn send(&self, msg: Action, _bus: &Bus) -> Result<(), messagebus::error::Error<Action>> {
        match msg {
            Action::Init(self_id) => {
                let event_sender = self.event_sender.clone();
                let mut rx = self.item_receiver.lock().take().unwrap();

                let client = self.client.clone();
                self.self_id.store(self_id, Ordering::SeqCst);

                tokio::spawn(async move {
                    let mut connection = client.get_tokio_connection().await.unwrap();
                    let mut body_buff = Vec::new();
                    let mut header_buff = Vec::new();
                    // let mut item = None;

                    event_sender.send(RecvDo::Ready).unwrap();
                    
                    while let Some(Some(item)) = rx.recv().await {
                        header_buff.clear();
                        body_buff.clear();

                        let pkt = item.serialize(BodyType::Cbor, &mut body_buff).unwrap();

                        serde_cbor::to_writer(&mut header_buff, &pkt).unwrap();

                        let channel = match &item {
                            ProtocolItem::Action(_) => "mbus_action".into(),
                            ProtocolItem::Send(_, msg, _) => format!("mbus_request::{}", msg.type_tag()),
                            ProtocolItem::Event(ev) => "mbus_response::".into(),
                            _ => unreachable!()
                        };

                        let () = connection.publish(channel, &header_buff).await.unwrap();
                    }

                });
            }

            Action::Close => {
                self.item_sender.send(None).unwrap();
                // self.event_sender.send(RecvDo::Closed).unwrap();
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

impl ReciveUntypedReceiver for RedisRelay {
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
                    //     RecvDo::Incoming(incoming) => {
                    //         futures::stream::unfold((incoming, bus, sender, self_id), |(mut incoming, bus, sender, self_id)| async move {
                    //             loop {
                    //                 let (_, recv) = match incoming.next().await? {
                    //                     Ok(recv) => recv,
                    //                     Err(err) => {
                    //                         println!("error: {}", err);
                    //                         return None;
                    //                     }
                    //                 };
        
                    //                 let buff = recv
                    //                     .read_to_end(usize::max_value())
                    //                     .await
                    //                     .unwrap();
        
                    //                 // assert_eq!(&buff[0..4], b"MBUS");

                    //                 if buff.is_empty() {
                    //                     println!("PONG");
                    //                     continue;
                    //                 }
        
                    //                 let mut reader = &buff[4..];
        
                    //                 let version = reader.get_u16();
                    //                 let content_type = reader.get_u16();
                    //                 let body_size = reader.get_u64();
        
                    //                 println!("inbound packet {}: v: {}; ct: {}; bs: {}", String::from_utf8_lossy(&buff[0..4]), version, content_type, body_size);
        
                    //                 let event = match content_type {
                    //                     0 => { // CBOR
                    //                         let proto: ProtocolPacket = serde_cbor::from_slice(&buff[16..]).unwrap();
                    //                         match proto.deserialize(&bus).unwrap() {
                    //                             ProtocolItem::Event(ev) => ev.map_msg(|msg|msg.upcast_box()),
                    //                             ProtocolItem::Action(action) => {
                    //                                 match action {
                    //                                     Action::Close => {
                    //                                         println!("warning: Close recevied - ignoring!");
                    //                                         sender.send(Some(ProtocolItem::Event(Event::Exited))).unwrap();
                    //                                     },
                    //                                     Action::Flush => {
                    //                                         bus.flush().await;
                    //                                         sender.send(Some(ProtocolItem::Event(Event::Flushed))).unwrap();
                    //                                     },
                    //                                     Action::Sync => {
                    //                                         bus.sync().await;
                    //                                         sender.send(Some(ProtocolItem::Event(Event::Synchronized(Ok(()))))).unwrap();
                    //                                     },
                    //                                     Action::Init(..) => (),
                    //                                     Action::Stats => (),        
                    //                                     _ => (),                                        
                    //                                 }
                    //                                 continue;
                    //                             }
                    //                             ProtocolItem::Send(mid, msg, req) => {
                    //                                 if req {
                    //                                     let res = bus.request_boxed(
                    //                                         msg.upcast_box(), 
                    //                                     SendOptions::Except(self_id.load(Ordering::SeqCst))
                    //                                     )
                    //                                         .await
                    //                                         .map(|x|x.as_shared_boxed().unwrap())
                    //                                         .map_err(|x|x.map_msg(|_|()));
                                                        
                    //                                     sender.send(Some(ProtocolItem::Event(Event::Response(mid, res)))).unwrap();
                    //                                 } else {
                    //                                     let _ = bus.send_boxed(msg.upcast_box(), Default::default())
                    //                                         .await;
                    //                                 }
        
                    //                                 continue;
                    //                             }
                    //                             _ => unimplemented!()
                    //                         }
                    //                     },
                    //                     _ => unimplemented!()
                    //                 };

                    //                 return Some((event, (incoming, bus, sender, self_id)));
                    //             }
                    //         }).right_stream()
                    //     }

                        other => futures::stream::once(async move { 
                            match other {
                                RecvDo::Pause => Event::Pause,
                                RecvDo::Ready => Event::Ready,
                                RecvDo::Closed => Event::Exited,
                                _ => unreachable!()
                            } 
                        })
                        // .left_stream()
                    }

                    
                })
                .flatten()
        )
    }
}