use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use messagebus::{
    derive::{Error as MbError, Message},
    error::{self, GenericError},
    receivers, Action, AsyncHandler, Bus, Event, Message, MessageBounds, ReciveUntypedReceiver,
    SendUntypedReceiver, TypeTagAccept, TypeTagged,
};
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

pub struct TestRelay {
    stx: mpsc::UnboundedSender<Event<Box<dyn Message>, GenericError>>,
    srx: Mutex<Option<mpsc::UnboundedReceiver<Event<Box<dyn Message>, GenericError>>>>,
}

impl TypeTagAccept for TestRelay {
    fn accept(
        &self,
        msg: &messagebus::TypeTag,
        resp: Option<&messagebus::TypeTag>,
        _err: Option<&messagebus::TypeTag>,
    ) -> bool {
        if msg.as_ref() == Msg::<i16>::type_tag_().as_ref() {
            if let Some(resp) = resp {
                if resp.as_ref() != Msg::<u8>::type_tag_().as_ref() {
                    return false;
                }
            }
        }

        if msg.as_ref() == Msg::<i32>::type_tag_().as_ref() {
            if let Some(resp) = resp {
                if resp.as_ref() != Msg::<i64>::type_tag_().as_ref()
                    && resp.as_ref() != Msg::<()>::type_tag_().as_ref()
                {
                    return false;
                }
            }
        }

        true
    }

    fn iter_types(
        &self,
        cb: &mut dyn FnMut(
            &messagebus::TypeTag,
            &messagebus::TypeTag,
            &messagebus::TypeTag,
        ) -> bool,
    ) {
        if !cb(
            &Msg::<i16>::type_tag_(),
            &Msg::<u8>::type_tag_(),
            &Error::type_tag_(),
        ) {
            return;
        }
        if !cb(
            &Msg::<i32>::type_tag_(),
            &Msg::<()>::type_tag_(),
            &Error::type_tag_(),
        ) {
            return;
        }
        if !cb(
            &Msg::<i32>::type_tag_(),
            &Msg::<i64>::type_tag_(),
            &Error::type_tag_(),
        ) {
            return;
        }
    }
}

impl SendUntypedReceiver for TestRelay {
    fn send(&self, msg: Action, _bus: &Bus) -> Result<(), error::SendError<Action>> {
        match msg {
            Action::Init => {
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
        _req: bool,
        _bus: &Bus,
    ) -> Result<(), error::SendError<Box<dyn Message>>> {
        println!("TestRelay::send_msg [{}] {:?}", mid, msg);
        if msg.type_tag().as_ref() == Msg::<i16>::type_tag_().as_ref() {
            self.stx
                .send(Event::Response(mid, Ok(Box::new(Msg(9u8)))))
                .unwrap();
        } else {
            self.stx
                .send(Event::Response(mid, Ok(Box::new(()))))
                .unwrap();
        }

        Ok(())
    }
}

impl ReciveUntypedReceiver for TestRelay {
    type Stream = Pin<Box<dyn Stream<Item = Event<Box<dyn Message>, error::GenericError>> + Send>>;

    fn event_stream(&self) -> Self::Stream {
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
    let res: Msg<u8> = b.request(Msg(12i16), Default::default()).await.unwrap();

    assert_eq!(res.0, 9u8);

    b.flush().await;
    b.close().await;
    poller.await;
}
