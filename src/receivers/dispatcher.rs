use std::{
    iter,
    ops::ControlFlow,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use dashmap::{mapref::one::Ref, DashMap};
use parking_lot::RwLock;

use crate::{
    cell::{MessageCell, SharedCell},
    error::Error,
    permit::Permit,
    receiver::{Event, Receiver},
    type_tag::{TypeTag, TypeTagQuery},
};

#[derive(Debug, Clone, Hash)]
pub struct TypeMatch {
    request: TypeTagQuery<'static>,
    response: TypeTagQuery<'static>,
}

pub struct DispatcherRouteConfig {}

impl DispatcherRouteConfig {
    pub fn new() -> Self {
        Self {}
    }
    // fn get(&self, name: &str) -> Option<impl Iterator<Item = &TypeMatch>> {
    //     None
    // }
}

struct MessageTypeMatch {
    request: TypeTag,
    response: Option<TypeTag>,
}

impl MessageTypeMatch {
    // pub fn new() -> Self {
    //     Self {}
    // }
}

struct ReceiverEntry {
    receiver: Arc<dyn Receiver>,
    mtype: MessageTypeMatch,
}

struct Receivers {
    receivers: RwLock<Vec<ReceiverEntry>>,
}

impl Receivers {
    pub fn new() -> Self {
        Self {
            receivers: RwLock::new(Vec::with_capacity(4)),
        }
    }

    pub fn add_receiver(&self, req: TypeTag, resp: Option<TypeTag>, receiver: Arc<dyn Receiver>) {
        self.receivers.write().push(ReceiverEntry {
            receiver,
            mtype: MessageTypeMatch {
                request: req,
                response: resp,
            },
        });
    }
}

pub struct Dispatcher {
    routes: DispatcherRouteConfig,
    receivers: DashMap<u64, Receivers>,
    message_types: RwLock<Vec<MessageTypeMatch>>,
}

impl Dispatcher {
    pub fn new() -> Self {
        Self {
            receivers: DashMap::new(),
            routes: DispatcherRouteConfig::new(),
            message_types: RwLock::new(Vec::new()),
        }
    }

    pub fn add_receiver(&self, receiver: Arc<dyn Receiver>) {
        receiver.message_types(&mut |req: TypeTag, resp: Option<TypeTag>| {
            let query = TypeTagQuery::from_type_tag_info(req.info());

            for hash in query.hashes() {
                self.receivers
                    .entry(hash)
                    .or_insert_with(Receivers::new)
                    .add_receiver(req.clone(), resp.clone(), receiver.clone());
            }

            ControlFlow::Continue(())
        });
    }

    fn query_receivers<'s, 'm: 's, 'r: 's>(
        &'s self,
        mut msg: TypeTagQuery<'m>,
        resp: Option<TypeTagQuery<'r>>,
    ) -> impl Iterator<Item = &Arc<dyn Receiver>> + 's {
        msg.make_hashable();
        let hash = msg.default_hash();

        if let Some(receivers) = self.receivers.get(&hash) {
            let vec = receivers.receivers.read();
            let mut iter = vec.iter();

            iter::from_fn(move || {
                let _ = vec;
                let item = &iter.next()?;
                let key = msg.default_hash();
                let resp = resp.clone();

                None
            })
        } else {
            panic!()
        }
    }
}

pub struct ReceiverIterator<'a> {
    vec: Ref<'a, u64, Receivers>,
}

impl Receiver for Dispatcher {
    fn poll_ready(&self, _cx: Option<&mut Context<'_>>) -> Poll<Result<Permit, Error>> {
        todo!()
    }

    fn handle(
        &self,
        pmt: Permit,
        msg: &mut dyn MessageCell,
        resp: Option<SharedCell>,
    ) -> Result<(), Error> {
        todo!()
    }

    fn poll_event(self: Pin<&Self>, cx: &mut Context<'_>) -> Poll<Result<Event, Error>> {
        todo!()
    }

    fn message_types(&self, cb: &mut dyn FnMut(TypeTag, Option<TypeTag>) -> ControlFlow<()>) {
        let lock = self.message_types.read();

        for mt in lock.iter() {
            match cb(mt.request.clone(), mt.response.clone()) {
                ControlFlow::Continue(_) => continue,
                ControlFlow::Break(_) => break,
            }
        }
    }
}
