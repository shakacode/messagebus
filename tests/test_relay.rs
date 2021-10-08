use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use messagebus::{Action, AsyncHandler, Bus, Event, Message, MessageBounds, ReciveUntypedReceiver, SendUntypedReceiver, TypeTag, TypeTagAccept, TypeTagged, derive::{Error as MbError, Message}, error::{self, GenericError}, receivers};
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error, MbError)]
enum Error {
    #[error("Error({0})")]
    Error(anyhow::Error),
}

impl<M: Message> From<error::Error<M>> for Error {
    fn from(err: error::Error<M>) -> Self {
        Self::Error(err.into())
    }
}

#[derive(Debug, Clone, Message)]
pub struct Msg<F: MessageBounds + Clone>(pub F);

struct TmpReceiver;

#[async_trait]
impl AsyncHandler<Msg<i32>> for TmpReceiver {
    type Error = Error;
    type Response = ();

    async fn handle(&self, msg: Msg<i32>, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        println!("TmpReceiver::handle {:?}", msg);
        Ok(())
    }

    async fn sync(&self, _bus: &Bus) -> Result<(), Self::Error> {
        println!("TmpReceiver::sync");
        Ok(())
    }
}

pub type TestRelayRxChannelCell =
    Mutex<Option<mpsc::UnboundedReceiver<Event<Box<dyn Message>, GenericError>>>>;
pub type TestRelayRxStream =
    Pin<Box<dyn Stream<Item = Event<Box<dyn Message>, error::GenericError>> + Send>>;

pub struct TestRelay {
    stx: mpsc::UnboundedSender<Event<Box<dyn Message>, GenericError>>,
    srx: TestRelayRxChannelCell,
}

impl TypeTagAccept for TestRelay {
    fn accept_req(
        &self,
        msg: &messagebus::TypeTag,
        resp: Option<&messagebus::TypeTag>,
        _err: Option<&messagebus::TypeTag>,
    ) -> bool {
        if msg.as_ref() == Msg::<i16>::type_tag_().as_ref() {
            if let Some(resp) = resp {
                if resp.as_ref() == Msg::<u8>::type_tag_().as_ref() {
                    return true
                }
            } else {
                return true
            }            
        }

        if msg.as_ref() == Msg::<i32>::type_tag_().as_ref() {
            if let Some(resp) = resp {
                if resp.as_ref() == Msg::<u64>::type_tag_().as_ref() {
                    return true
                }
            } else {
                return true
            }            
        }

        false
    }

    fn accept_msg(
        &self,
        msg: &messagebus::TypeTag,
    ) -> bool {
        if msg.as_ref() == Msg::<i32>::type_tag_().as_ref() {
            return true
        }

        false
    }

    fn iter_types(&self) -> Box<dyn Iterator<Item = (TypeTag, Option<(TypeTag, TypeTag)>)>> {
        Box::new(
            std::iter::once((Msg::<i32>::type_tag_(), None))
                .chain(std::iter::once((Msg::<i32>::type_tag_(), Some((Msg::<u64>::type_tag_(), GenericError::type_tag_())))))
                .chain(std::iter::once((Msg::<i16>::type_tag_(), Some((Msg::<u8>::type_tag_(), GenericError::type_tag_())))))
        )
    }
}

impl SendUntypedReceiver for TestRelay {
    fn send(&self, msg: Action, _bus: &Bus) -> Result<(), error::Error<Action>> {
        match msg {
            Action::Init(..) => {
                self.stx.send(Event::Ready).unwrap();
            }
            Action::Close => {
                self.stx.send(Event::Exited).unwrap();
            }
            Action::Flush => {
                self.stx.send(Event::Flushed).unwrap();
            }
            Action::Sync => {
                self.stx.send(Event::Synchronized(Ok(()))).unwrap();
            }
            _ => unimplemented!(),
        }

        println!("TestRelay::send {:?}", msg);
        Ok(())
    }

    fn send_msg(
        &self,
        mid: u64,
        msg: Box<dyn Message>,
        req: bool,
        _bus: &Bus,
    ) -> Result<(), error::Error<Box<dyn Message>>> {
        println!("TestRelay::send_msg [{}] {:?}", mid, msg);
        if msg.type_tag().as_ref() == Msg::<i16>::type_tag_().as_ref() {
            self.stx
                .send(Event::Response(mid, Ok(Box::new(Msg(9u8)))))
                .unwrap();
        } else if msg.type_tag().as_ref() == Msg::<i32>::type_tag_().as_ref() {
            if req {
                self.stx
                    .send(Event::Response(mid, Ok(Box::new(Msg(22u64)))))
                    .unwrap();
            } else {
                self.stx
                    .send(Event::Response(mid, Ok(Box::new(()))))
                    .unwrap();
            }
        } else {
            panic!("unsupported message type {}", msg.type_tag());
        }

        Ok(())
    }
}

impl ReciveUntypedReceiver for TestRelay {
    type Stream = TestRelayRxStream;

    fn event_stream(&self, _: Bus) -> Self::Stream {
        let mut rx = self.srx.lock().take().unwrap();

        Box::pin(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
    }
}

#[tokio::test]
async fn test_relay() {
    let (stx, srx) = mpsc::unbounded_channel();
    let relay = TestRelay {
        stx,
        srx: Mutex::new(Some(srx)),
    };

    let (b, poller) = Bus::build()
        .register_relay(relay)
        .register(TmpReceiver)
        .subscribe_async::<Msg<i32>>(
            1,
            receivers::BufferUnorderedConfig {
                buffer_size: 1,
                max_parallel: 1,
            },
        )
        .done()
        .build();

    b.send(Msg(32i32)).await.unwrap();
    let res1: Msg<u8> = b.request(Msg(12i16), Default::default()).await.unwrap();
    let res2: Msg<u64> = b.request(Msg(12i32), Default::default()).await.unwrap();

    assert_eq!(res1.0, 9u8);
    assert_eq!(res2.0, 22u64);

    b.flush().await;
    b.close().await;
    poller.await;
}
