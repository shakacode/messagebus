use std::task::{Context, Poll};

use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error::{self, GenericError},
    receivers, Action, AsyncHandler, Bus, Event, Message, MessageBounds, ReciveUnypedReceiver,
    SendUntypedReceiver, TypeTagAccept, TypeTagged,
};
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::mpsc;

// macro_rules! type_map {
//     (inbound : $($tt:tt)*) => {{
//         let mut type_map = TypeMap::new();
//         type_map!(@inbound type_map, $($tt)*);
//         type_map
//     }};

//     (outbound : $($tt:tt)*) => {
//         let mut type_map = TypeMap::new();
//         type_map!(@outbound type_map, $($tt)*);
//         type_map
//     };

//     (@inbound $tm: ident, $msg:ty => $resp: ty, $($tt:tt)*) => {
//         $tm.add_inbound(<$msg as TypeTagged>::type_tag_(), <$resp as TypeTagged>::type_tag_(), None);
//         type_map!(@inbound $tm, $($tt)*)
//     };

//     (@inbound $tm: ident, $msg:ty => ($resp:ty) throws $err: ty, $($tt:tt)*) => {
//         $tm.add_inbound(<$msg as messagebus::TypeTagged>::type_tag_(), <$resp as messagebus:TypeTagged>::type_tag_(), Some(<$err as messagebus::TypeTagged>::type_tag_()));
//         type_map!(@inbound $tm, $($tt)*)
//     };

//     (@inbound $tm: ident, $msg:ty => $resp: ty) => {
//         $tm.add_inbound(<$msg as TypeTagged>::type_tag_(), <$resp as TypeTagged>::type_tag_(), None);
//     };

//     (@inbound $tm: ident, outbound : $($tt:tt)*) => {
//         type_map!(@outbound $tm, $($tt)*)
//     };

//     (@inbound $tm: ident,) => {};

//     (@outbound $tm: ident, $msg:ty => ($resp:ty) throws $err: ty, $($tt:tt)*) => {
//         $tm.add_outbound(<$msg as TypeTagged>::type_tag_(), <$resp as TypeTagged>::type_tag_(), Some(<$err as TypeTagged>::type_tag_()));
//         type_map!(@outbound $tm, $($tt)*)
//     };

//     (@outbound $tm: ident, $msg:ty => $resp: ty, $($tt:tt)*) => {
//         $tm.add_outbound(<$msg as TypeTagged>::type_tag_(), <$resp as TypeTagged>::type_tag_(), None);
//         type_map!(@outbound $tm, $($tt)*)
//     };

//     (@outbound $tm: ident, $msg:ty => $resp: ty) => {
//         $tm.add_outbound(<$msg as TypeTagged>::type_tag_(), <$resp as TypeTagged>::type_tag_(), None);
//     };

//     (@outbound $tm: ident, inbound : $($tt:tt)*) => {
//         type_map!(@inbound $tm, $($tt)*)
//     };

//     (@outbound $tm: ident,) => {};
// }

// #[derive(Debug, Clone)]
// pub struct TypeMap {
//     inbound: HashMap<TypeTag, Vec<(TypeTag, Option<TypeTag>)>>,
//     outbound: HashMap<TypeTag, Vec<(TypeTag, Option<TypeTag>)>>,
// }

// impl TypeMap {
//     pub fn new() -> Self {
//         Self {
//             inbound: Default::default(),
//             outbound: Default::default(),
//         }
//     }

//     pub fn add_inbound(&mut self, msg: TypeTag, resp: TypeTag, err: Option<TypeTag>) -> &mut Self {
//         let vec = self.inbound.entry(msg)
//             .or_insert_with(Vec::new);

//         vec.push((resp, err));
//         self
//     }

//     pub fn add_outbound(&mut self, msg: TypeTag, resp: TypeTag, err: Option<TypeTag>) -> &mut Self {
//         let vec = self.outbound.entry(msg)
//             .or_insert_with(Vec::new);

//         vec.push((resp, err));
//         self
//     }

//     pub fn accept_inbound(&self, msg: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
//         if let Some(vec) = self.inbound.get(msg) {
//             if let Some(rr) = resp {
//                 vec.iter().find(|(r, e)| {
//                     let ee = if let Some(e) = e {
//                         if let Some(te) = err {
//                             te.as_ref() == e.as_ref()
//                         } else {
//                             true
//                         }
//                     } else {
//                         err.is_none()
//                     };

//                     r.as_ref() == rr.as_ref() && ee
//                 }).is_some()
//             } else {
//                 true
//             }
//         } else {
//             false
//         }
//     }

//     pub fn accept_outbound(&self, msg: &TypeTag, resp: Option<&TypeTag>, err: Option<&TypeTag>) -> bool {
//         if let Some(vec) = self.outbound.get(msg) {
//             if let Some(rr) = resp {
//                 vec.iter().find(|(r, e)| {
//                     let ee = if let Some(e) = e {
//                         if let Some(te) = err {
//                             te.as_ref() == e.as_ref()
//                         } else {
//                             true
//                         }
//                     } else {
//                         err.is_none()
//                     };

//                     r.as_ref() == rr.as_ref() && ee
//                 }).is_some()
//             } else {
//                 true
//             }
//         } else {
//             false
//         }
//     }

//     #[inline]
//     pub fn inbound_iter(&self) -> impl Iterator<Item = (&TypeTag, &TypeTag, Option<&TypeTag>)> {
//         self.inbound.iter().map(|(k, v)| v.into_iter().map(move |(r, e)|(k, r, e.as_ref()))).flatten()
//     }
// }

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
    srx: Mutex<mpsc::UnboundedReceiver<Event<Box<dyn Message>, GenericError>>>,
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
    fn send(&self, msg: Action) -> Result<(), error::SendError<Action>> {
        match msg {
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

impl ReciveUnypedReceiver for TestRelay {
    fn poll_events(
        &self,
        ctx: &mut Context<'_>,
    ) -> Poll<Event<Box<dyn Message>, error::GenericError>> {
        let poll = self.srx.lock().poll_recv(ctx);
        match poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(event)) => Poll::Ready(event),
            Poll::Ready(None) => Poll::Ready(Event::Exited),
        }
    }
}

#[tokio::test]
async fn test_relay() {
    let (stx, srx) = mpsc::unbounded_channel();
    let relay = TestRelay {
        stx,
        srx: Mutex::new(srx),
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
