use messagebus::{Action, Bus, Event, SharedMessage, TypeTag};
use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug, Deserialize, Serialize)]
#[repr(u16)]
pub enum ProtocolHeaderActionKind {
    Nop,
    Error,

    Send,
    Response,
    BatchComplete,

    Flush,
    Flushed,

    Synchronize,
    Synchronized,

    Close,
    Exited,

    Initialize,
    Ready,

    Pause,
}

#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
#[non_exhaustive]
#[repr(u32)]
pub enum BodyType {
    None,
    Utf8,
    Cbor,
    MessagePack,
    Json,
    Bson,
}

bitflags::bitflags! {
    #[derive(Deserialize, Serialize)]
    pub struct ProtocolHeaderFlags: u32 {
        const TYPE_TAG = 0b00000001;
        const BODY     = 0b00000010;
        const ERROR    = 0b00000100;
        const ARGUMENT = 0b00001000;
        const TT_AND_ERROR = Self::TYPE_TAG.bits | Self::ERROR.bits;
        const TT_AND_BODY =  Self::TYPE_TAG.bits | Self::BODY.bits;
        const TT_ERROR_AND_ARGUMENT = Self::TYPE_TAG.bits | Self::ERROR.bits | Self::ARGUMENT.bits;
        const TT_BODY_AND_ARGUMENT =  Self::TYPE_TAG.bits | Self::BODY.bits | Self::ARGUMENT.bits;
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProtocolHeader<'a> {
    pub kind: ProtocolHeaderActionKind,
    pub type_tag: Option<Cow<'a, [u8]>>,
    pub flags: ProtocolHeaderFlags,
    pub body_type: BodyType,
    pub argument: u64,
}

impl<'a> ProtocolHeader<'a> {
    pub fn send(mid: u64, tt: &'a TypeTag, body_type: BodyType) -> ProtocolHeader<'a> {
        ProtocolHeader {
            kind: ProtocolHeaderActionKind::Send,
            type_tag: Some(tt.as_bytes().into()),
            flags: ProtocolHeaderFlags::TT_BODY_AND_ARGUMENT,
            body_type,
            argument: mid,
        }
    }

    pub fn flush() -> Self {
        ProtocolHeader {
            kind: ProtocolHeaderActionKind::Flush,
            type_tag: None,
            flags: ProtocolHeaderFlags::empty(),
            body_type: BodyType::None,
            argument: 0,
        }
    }

    pub fn close() -> Self {
        ProtocolHeader {
            kind: ProtocolHeaderActionKind::Close,
            type_tag: None,
            flags: ProtocolHeaderFlags::empty(),
            body_type: BodyType::None,
            argument: 0,
        }
    }

    pub fn sync() -> Self {
        ProtocolHeader {
            kind: ProtocolHeaderActionKind::Synchronize,
            type_tag: None,
            flags: ProtocolHeaderFlags::empty(),
            body_type: BodyType::None,
            argument: 0,
        }
    }

    // pub fn init() -> Self {
    //     ProtocolHeader {
    //         kind: ProtocolHeaderActionKind::Initialize,
    //         type_tag: None,
    //         failed: false,
    //         body_encoding: 0,
    //         argument: 0,
    //     }
    // }

    // pub fn pause() -> Self {
    //     ProtocolHeader {
    //         kind: ProtocolHeaderActionKind::Pause,
    //         type_tag: None,
    //         failed: false,
    //         body_encoding: 0,
    //         argument: 0,
    //     }
    // }
}

#[derive(Debug)]
pub enum ProtocolItem {
    Nop,
    Event(Event<Box<dyn SharedMessage>, messagebus::error::GenericError>),
    Action(Action),
    Send(u64, Box<dyn SharedMessage>, bool)
}

impl From<Action> for ProtocolItem {
    fn from(action: Action) -> Self {
        ProtocolItem::Action(action)
    }
}

impl From<Event<Box<dyn SharedMessage>, messagebus::error::GenericError>> for ProtocolItem {
    fn from(ev: Event<Box<dyn SharedMessage>, messagebus::error::GenericError>) -> Self {
        ProtocolItem::Event(ev)
    }
}

impl From<(u64, Box<dyn SharedMessage>, bool)> for ProtocolItem {
    fn from(msg: (u64, Box<dyn SharedMessage>, bool)) -> Self {
        ProtocolItem::Send(msg.0, msg.1, msg.2)
    }
}

impl ProtocolItem {
    pub fn unwrap_send(self) -> Result<(u64, Box<dyn SharedMessage>, bool), ProtocolItem> {
        match self {
            ProtocolItem::Send(a, b, c) => Ok((a, b, c)),
            other => Err(other)
        }
    }

    pub fn serialize(self, mut body_type: BodyType, body_buff: &mut Vec<u8>) -> Result<ProtocolPacket<'_>, crate::error::Error> {
        let mut argument = 0;
        let mut type_tag = None;
        let mut body = None;
        let mut flags = ProtocolHeaderFlags::empty();
        
        let kind = match self {
            ProtocolItem::Nop => ProtocolHeaderActionKind::Nop,
            ProtocolItem::Action(action) => match action {
                Action::Close => ProtocolHeaderActionKind::Close,
                Action::Flush => ProtocolHeaderActionKind::Flush,
                Action::Init => ProtocolHeaderActionKind::Initialize,
                Action::Sync => ProtocolHeaderActionKind::Synchronize,
                _ => unimplemented!(),
            }
            ProtocolItem::Send(mid, msg, req) => {
                let msg = msg.as_shared_boxed()
                    .map_err(|_| crate::error::Error::UnknownCodec)?;

                argument = mid;
                flags.set(ProtocolHeaderFlags::ARGUMENT, req);
                flags.set(ProtocolHeaderFlags::BODY, true);
                flags.set(ProtocolHeaderFlags::TYPE_TAG, true);
                type_tag = Some(msg.type_tag());
                body = Some(generic_serialize(body_type, &*msg, body_buff)?);

                ProtocolHeaderActionKind::Send
            },
            ProtocolItem::Event(ev) => match ev {
                Event::Response(mid, res) => {
                    argument = mid;
                    flags.set(ProtocolHeaderFlags::ARGUMENT, true);
                    flags.set(ProtocolHeaderFlags::BODY, true);
                    flags.set(ProtocolHeaderFlags::TYPE_TAG, true);

                    match res {
                        Ok(msg) => {
                            let msg = msg.as_shared_boxed()
                                .map_err(|_| crate::error::Error::UnknownCodec)?;
    
                            type_tag = Some(msg.type_tag());
                            body = Some(generic_serialize(body_type, &*msg, body_buff)?);
                        }

                        Err(err) => {
                            flags.set(ProtocolHeaderFlags::ERROR, true);
                            
                            type_tag = Some("GenericError".into());
                            body_type = BodyType::Utf8;
                            body = Some(format!("{}", err).into_bytes().into());
                        }
                    }

                    ProtocolHeaderActionKind::Response
                },
                Event::Error(err) => {
                    flags.set(ProtocolHeaderFlags::ERROR, true);
                    flags.set(ProtocolHeaderFlags::BODY, true);
                    flags.set(ProtocolHeaderFlags::TYPE_TAG, true);

                    type_tag = Some("GenericError".into());
                    body_type = BodyType::Utf8;
                    body = Some(format!("{}", err).into_bytes().into());

                    ProtocolHeaderActionKind::Error
                }
                Event::Finished(n) => {
                    argument = n;
                    flags.set(ProtocolHeaderFlags::ARGUMENT, true);
                    ProtocolHeaderActionKind::BatchComplete
                },
                Event::Synchronized(res) => {
                    match res {
                        Ok(_) => {}
                        Err(err) => {
                            flags.set(ProtocolHeaderFlags::BODY, true);
                            flags.set(ProtocolHeaderFlags::ERROR, true);
                            flags.set(ProtocolHeaderFlags::TYPE_TAG, true);
                            
                            type_tag = Some("GenericError".into());
                            body_type = BodyType::Utf8;
                            body = Some(format!("{}", err).into_bytes().into());
                        }
                    }
                    ProtocolHeaderActionKind::Synchronized 
                }
                Event::InitFailed(err) => {
                    flags.set(ProtocolHeaderFlags::BODY, true);
                    flags.set(ProtocolHeaderFlags::ERROR, true);
                    flags.set(ProtocolHeaderFlags::TYPE_TAG, true);
                    
                    type_tag = Some("GenericError".into());
                    body_type = BodyType::Utf8;
                    body = Some(format!("{}", err).into_bytes().into());

                    ProtocolHeaderActionKind::Ready
                },
                Event::Ready => ProtocolHeaderActionKind::Ready,
                Event::Pause => ProtocolHeaderActionKind::Pause,
                Event::Exited => ProtocolHeaderActionKind::Exited,
                Event::Flushed => ProtocolHeaderActionKind::Flushed,
                _ => unimplemented!()
            }
        };

        Ok(ProtocolPacket {
            header: ProtocolHeader {
                kind,
                type_tag: type_tag.map(|x| x.to_string().into_bytes().into()),
                flags,
                body_type,
                argument,
            },
            body
        })
    }
}


#[derive(Debug, Deserialize, Serialize)]
pub struct ProtocolPacket<'a> {
    pub header: ProtocolHeader<'a>,
    pub body: Option<Cow<'a, [u8]>>,
}

impl<'a> ProtocolPacket<'a> {
    pub fn deserialize(
        self,
        bus: &Bus,
    ) -> Result<ProtocolItem, crate::error::Error>
    {
        let type_tag: Option<TypeTag> = if self.header.flags.contains(ProtocolHeaderFlags::TYPE_TAG) {
            self.header
                .type_tag
                .map(|x| String::from_utf8_lossy(x.as_ref()).to_string().into())
        } else {
            None
        };

        let (body, error) = if self.header.flags.contains(ProtocolHeaderFlags::ERROR) {
            let error = messagebus::error::GenericError {
                type_tag: type_tag.unwrap(),
                description: self.body.map(|x|String::from_utf8_lossy(x.as_ref()).to_string()).unwrap_or_default(),
            };

            (None, Some(messagebus::error::Error::Other(error)))
        } else if self.header.flags.contains(ProtocolHeaderFlags::TT_AND_BODY) {
            let body = self.body.ok_or(crate::error::Error::ProtocolParseError)?;
            let res = generic_deserialize(self.header.body_type, body.as_ref(), |de| {
                bus.deserialize_message(type_tag.unwrap(), de)
                    .map_err(|x| x.map_msg(|_| ()))
            })?;

            match res {
                Ok(body) => (Some(body), None),
                Err(err) => (None, Some(err)),
            }
        } else {
            (None, None)
        };

        let argument = if self.header.flags.contains(ProtocolHeaderFlags::ARGUMENT) {
            Some(self.header.argument)
        } else {
            None
        };

        Ok(ProtocolItem::Event(match self.header.kind {
            ProtocolHeaderActionKind::Response => Event::Response(
                argument
                    .ok_or(crate::error::Error::ProtocolParseError)?,
                error
                    .map(Err)
                    .or_else(|| body.map(Ok))
                    .ok_or(crate::error::Error::ProtocolParseError)?,
            ),
            ProtocolHeaderActionKind::Synchronized => {
                Event::Synchronized(error.map(Err).unwrap_or(Ok(())))
            }
            ProtocolHeaderActionKind::Error => {
                Event::Error(error.ok_or(crate::error::Error::ProtocolParseError)?)
            }
            ProtocolHeaderActionKind::BatchComplete => Event::Finished(self.header.argument),
            ProtocolHeaderActionKind::Flushed => Event::Flushed,
            ProtocolHeaderActionKind::Exited => Event::Exited,
            ProtocolHeaderActionKind::Ready => Event::Ready,
            ProtocolHeaderActionKind::Pause => Event::Pause,

            other => return Ok(ProtocolItem::Action(match other {
                ProtocolHeaderActionKind::Initialize => Action::Init,
                ProtocolHeaderActionKind::Close => Action::Close,
                ProtocolHeaderActionKind::Flush => Action::Flush,
                ProtocolHeaderActionKind::Synchronize => Action::Sync,
                ProtocolHeaderActionKind::Send => {
                    let req = argument.is_some();
                    let mid = self.header.argument;
                    let body = body.ok_or(crate::error::Error::ProtocolParseError)?;

                    return Ok(ProtocolItem::Send(mid, body, req));
                },
                ProtocolHeaderActionKind::Nop => return Ok(ProtocolItem::Nop),

                _ => unreachable!()
            })),
        }))
    }
}

fn generic_deserialize<F, T>(k: BodyType, data: &[u8], f: F) -> Result<T, crate::error::Error>
where
    F: FnOnce(&mut dyn erased_serde::Deserializer<'_>) -> T,
{
    match k {
        BodyType::Cbor => {
            let mut cbor_de = serde_cbor::Deserializer::from_slice(data);
            let mut de = <dyn erased_serde::Deserializer>::erase(&mut cbor_de);

            Ok(f(&mut de))
        }

        BodyType::Json => {
            let mut json_de = serde_json::Deserializer::from_slice(data);
            let mut de = <dyn erased_serde::Deserializer>::erase(&mut json_de);

            Ok(f(&mut de))
        }

        _ => Err(crate::error::Error::UnknownCodec),
    }
}

fn generic_serialize<'a>(kind: BodyType, msg: &dyn SharedMessage, buffer: &'a mut Vec<u8>) -> Result<Cow<'a, [u8]>, crate::error::Error> {
    match kind {
        BodyType::Cbor => {
            let mut cbor_se = serde_cbor::Serializer::new(&mut *buffer);
            let mut se = <dyn erased_serde::Serializer>::erase(&mut cbor_se);
            msg.erased_serialize(&mut se)?;
        }

        BodyType::Json => {
            let mut json_se = serde_json::Serializer::new(&mut *buffer);
            let mut se = <dyn erased_serde::Serializer>::erase(&mut json_se);
            msg.erased_serialize(&mut se)?;
        }

        _ => return Err(crate::error::Error::UnknownCodec),
    }

    Ok(buffer.as_slice().into())
}

#[cfg(test)]
mod tests {
    use crate::proto::ProtocolItem;

    use super::{
        BodyType, ProtocolHeader, ProtocolHeaderActionKind, ProtocolHeaderFlags, ProtocolPacket,
    };
    use messagebus::Event;
    use messagebus::{derive::Message, Bus, Message, TypeTagged};
    use serde_derive::{Deserialize, Serialize};
    use std::borrow::Cow;

    #[derive(Serialize, Deserialize, Debug, Clone, Message)]
    #[namespace("test")]
    #[message(shared, clone)]
    struct TestSharedMessage {
        test: String,
        value: i32,
    }

    #[test]
    fn test_proto_pack_event() {
        let (bus, _) = Bus::build()
            .register_shared_message::<TestSharedMessage>()
            .build();

        let pkt = ProtocolPacket {
            header: ProtocolHeader {
                kind: ProtocolHeaderActionKind::Response,
                type_tag: Some(TestSharedMessage::type_tag_().as_bytes().to_vec().into()),
                flags: ProtocolHeaderFlags::TT_BODY_AND_ARGUMENT,
                body_type: BodyType::Json,
                argument: 222,
            },
            body: Some(Cow::Borrowed(br#"{"test":"my test","value":12}"#)),
        };

        let event = match pkt.deserialize(&bus).unwrap() {
            ProtocolItem::Event(ev) => ev,
            _ => unreachable!()
        };
        
        assert!(matches!(event, Event::Response(..)));

        match event {
            Event::Response(mid, msg) => {
                assert_eq!(mid, 222);
                // assert!(msg.is_ok());
                let msg = msg.unwrap();

                assert_eq!(msg.type_tag(), TestSharedMessage::type_tag_());
                let m: Box<TestSharedMessage> = msg.as_any_boxed().downcast().unwrap();

                assert_eq!(m.value, 12);
                assert_eq!(&m.test, "my test");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_proto_pack_event_error() {
        let (bus, _) = Bus::build()
            .register_shared_message::<TestSharedMessage>()
            .build();

        let pkt = ProtocolPacket {
            header: ProtocolHeader {
                kind: ProtocolHeaderActionKind::Response,
                type_tag: Some(TestSharedMessage::type_tag_().as_bytes().to_vec().into()),
                flags: ProtocolHeaderFlags::TT_ERROR_AND_ARGUMENT,
                body_type: BodyType::Utf8,
                argument: 222,
            },
            body: Some(Cow::Borrowed(br#"error description"#)),
        };

        let event = match pkt.deserialize(&bus).unwrap() {
            ProtocolItem::Event(ev) => ev,
            _ => unreachable!()
        };
        
        assert!(matches!(event, Event::Response(..)));

        #[allow(clippy::unit_cmp)]
        match event {
            Event::Response(mid, msg) => {
                assert_eq!(mid, 222);
                let msg = msg.unwrap_err();
                
                assert!(matches!(msg, messagebus::error::Error::Other(val) if (
                    assert_eq!(val.type_tag, TestSharedMessage::type_tag_()) == () &&
                    assert_eq!(val.description, "error description") == ()
                )));
            }
            _ => unreachable!(),
        }
    }
}
