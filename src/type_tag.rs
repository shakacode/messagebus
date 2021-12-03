use parking_lot::RwLock;
use std::collections::HashMap;

use crate::envelop::IntoSharedMessage;
use crate::error::Error;
use crate::{Message, SharedMessage, TypeTag};

static TYPE_REGISTRY: TypeRegistry = TypeRegistry::new();

type MessageDeserializerCallback = Box<
    dyn Fn(&mut dyn erased_serde::Deserializer<'_>) -> Result<Box<dyn SharedMessage>, Error>
        + Send
        + Sync,
>;

pub struct MessageTypeDescriptor {
    de: MessageDeserializerCallback,
}

impl MessageTypeDescriptor {
    #[inline]
    pub fn deserialize_boxed(
        &self,
        de: &mut dyn erased_serde::Deserializer<'_>,
    ) -> Result<Box<dyn SharedMessage>, Error> {
        (self.de)(de)
    }
}

#[derive(Default)]
pub struct TypeRegistry {
    message_types: RwLock<Option<HashMap<TypeTag, MessageTypeDescriptor>>>,
}

impl TypeRegistry {
    pub const fn new() -> Self {
        Self {
            message_types: parking_lot::const_rwlock(None),
        }
    }

    pub fn deserialize(
        &self,
        tt: TypeTag,
        de: &mut dyn erased_serde::Deserializer<'_>,
    ) -> Result<Box<dyn SharedMessage>, Error<Box<dyn Message>>> {
        let guard = self.message_types.read();
        let md = guard
            .as_ref()
            .ok_or_else(|| Error::TypeTagNotRegistered(tt.clone()))?
            .get(&tt)
            .ok_or(Error::TypeTagNotRegistered(tt))?;

        md.deserialize_boxed(de)
            .map_err(|err| err.specify::<Box<dyn Message>>())
    }

    pub fn register<M: Message + serde::Serialize + serde::de::DeserializeOwned>(&self) {
        println!("insert {}", M::type_tag_());

        self.message_types
            .write()
            .get_or_insert_with(HashMap::new)
            .insert(
                M::type_tag_(),
                MessageTypeDescriptor {
                    de: Box::new(move |de| Ok(M::deserialize(de)?.into_shared())),
                },
            );
    }
}

#[inline]
pub fn deserialize_shared_message(
    tt: TypeTag,
    de: &mut dyn erased_serde::Deserializer<'_>,
) -> Result<Box<dyn SharedMessage>, Error<Box<dyn Message>>> {
    TYPE_REGISTRY.deserialize(tt, de)
}

#[inline]
pub fn register_shared_message<M: Message + serde::Serialize + serde::de::DeserializeOwned>() {
    TYPE_REGISTRY.register::<M>();
}
