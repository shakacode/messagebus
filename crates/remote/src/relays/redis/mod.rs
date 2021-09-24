use messagebus::{Action, Bus, Event, Message, ReciveUntypedReceiver, SendUntypedReceiver, TypeTag, TypeTagAccept};

use crate::error::Error;

use super::{GenericEventStream, MessageTable};
use futures::StreamExt;

pub struct RedisRelay {
    in_table: MessageTable,
    out_table: MessageTable,
}

impl RedisRelay {
    fn new(uri: &str) -> Result<Self, Error> {
        let client = redis::Client::open("redis://127.0.0.1/")?;
        unimplemented!()



        // let mut publish_conn = client.get_async_connection().await?;
        // let mut pubsub_conn = client.get_async_connection().await?.into_pubsub();

        // pubsub_conn.subscribe("wavephone").await?;
        // let mut pubsub_stream = pubsub_conn.on_message();

        // publish_conn.publish("wavephone", "banana").await?;

        // let pubsub_msg: String = pubsub_stream.next().await.unwrap().get_payload()?;
        // assert_eq!(&pubsub_msg, "banana");

        // Ok(())
    }
} 

impl TypeTagAccept for RedisRelay {
    fn accept(&self, msg: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
        self.out_table.accept(msg, resp, err)
    }

    fn iter_types(&self, cb: &mut dyn FnMut(&TypeTag, &TypeTag, &TypeTag) -> bool) {
        let iter = self.out_table.iter_types();

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
            Action::Init => {
                // let (sender, mut rx) = self.receiver_send.lock().take().unwrap();
                // let conn = self.endpoint.incoming();

                // tokio::spawn(async move {
                //     let mut conn = conn.await.unwrap();
                //     sender.send((conn.recv, conn.connection)).unwrap();
                //     let mut buf1 = Vec::new();
                //     let mut buf2 = Vec::new();

                //     while let Some(r) = rx.recv().await {
                //         r.serialize(&mut buf1, &mut buf2, &mut conn.send)
                //             .await
                //             .unwrap();

                //     }
                // });
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
                if let Err(err) = self.sender.send((req, mid, msg).into()) {
                    Err(messagebus::error::Error::TryAgain(err.0.unwrap_msg().unwrap()))
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
        let recevier = self.recevier.lock().take().unwrap();

        Box::pin(
            futures::stream::unfold((recevier, bus), |(recv, bus)| async move {
                if let Ok(r) = recv.next().await? {
                    let stream = futures::stream::unfold((true, r, bus.clone()), |(first, r, bus)| async move {
                        if first {
                            return Some((Event::Ready, (false, r, bus)));
                        }
                        
                        Some((Event::Pause, (false, r, bus)))
                    });

                    Some((stream.left_stream(), (recv, bus)))
                } else {
                    Some((futures::stream::once(async move { Event::Pause }).right_stream(), (recv, bus)))
                }
            }).flatten()
        )
    }
}
