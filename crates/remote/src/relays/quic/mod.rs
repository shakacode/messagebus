mod client;
mod server;

pub use client::QuicClientRelay;
use messagebus::{Bus, ReciveUntypedReceiver};
use parking_lot::Mutex;
use quinn::IncomingUniStreams;
pub use server::QuicServerRelay;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::StreamExt;
use bytes::{Buf};

use crate::proto::{ProtocolItem, ProtocolPacket};

use super::GenericEventStream;

pub struct QuicRelay {
    sender: UnboundedSender<ProtocolItem>,
    receiver: Mutex<Option<(UnboundedReceiver<ProtocolItem>, UnboundedSender<IncomingUniStreams>)>>,
    st_receiver: Mutex<Option<UnboundedReceiver<IncomingUniStreams>>>,
}

pub const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];

impl ReciveUntypedReceiver for QuicRelay {
    type Stream = GenericEventStream;

    fn event_stream(&self, bus: Bus) -> Self::Stream {
        let mut recv = self.st_receiver.lock().take().unwrap();

        Box::pin(
            futures::stream::poll_fn(move |cx|recv.poll_recv(cx))
                .map(move |uni_streams| {
                    let bus = bus.clone();
                    uni_streams.filter_map(move |recv| {
                        
                        let bus = bus.clone();
                        let mut buff = Vec::new();

                        async move {
                            let mut recv = recv.ok()?;

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
                            Some(event)
                        }
                    })
                })
                .flatten()
        )
    }
}