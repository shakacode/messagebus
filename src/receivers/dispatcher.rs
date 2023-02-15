use std::{
    ops::ControlFlow,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use dashmap::DashMap;
use parking_lot::RwLock;
use smallvec::SmallVec;

use crate::{
    cell::{MessageCell, SharedCell},
    error::Error,
    permit::Permit,
    receiver::{Event, Receiver},
    type_tag::{TypeTag, TypeTagInfo, TypeTagQuery},
};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TypeMatch {
    request: TypeTagQuery<'static>,
    response: TypeTagQuery<'static>,
}

impl TypeMatch {
    fn test(&self, msg: &TypeTagInfo, resp: Option<&TypeTagInfo>) -> bool {
        self.request.test(&msg) && resp.map(|resp| self.response.test(resp)).unwrap_or(true)
    }
}

pub struct DispatcherRouteConfig {}

impl DispatcherRouteConfig {
    pub fn new() -> Self {
        Self {}
    }
}

struct ReceiverEntry {
    receiver: Arc<dyn Receiver>,
    mtype: TypeMatch,
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

    pub fn add_receiver<'a, 'b>(
        &self,
        req: TypeTagQuery<'a>,
        resp: TypeTagQuery<'b>,
        receiver: Arc<dyn Receiver>,
    ) {
        self.receivers.write().push(ReceiverEntry {
            receiver,
            mtype: TypeMatch {
                request: req.make_owned(),
                response: resp.make_owned(),
            },
        });
    }
}

pub struct Dispatcher {
    routes: DispatcherRouteConfig,
    receivers: DashMap<u64, Receivers>,
    message_types: RwLock<Vec<TypeMatch>>,
    permits: DashMap<TypeMatch, Permit>,
}

impl Dispatcher {
    pub fn new() -> Self {
        Self {
            receivers: DashMap::new(),
            routes: DispatcherRouteConfig::new(),
            message_types: RwLock::new(Vec::new()),
            permits: DashMap::new(),
        }
    }

    pub fn add_receiver(&self, receiver: Arc<dyn Receiver>) {
        receiver.message_types(&mut |req_query: TypeTagQuery, resp_query: TypeTagQuery| {
            for hash in req_query.hashes() {
                self.receivers
                    .entry(hash)
                    .or_insert_with(Receivers::new)
                    .add_receiver(req_query.clone(), resp_query.clone(), receiver.clone());
            }

            ControlFlow::Continue(())
        });
    }

    fn query_receivers<'s, 'm: 's, 'r: 's>(
        &'s self,
        msg: &TypeTagInfo<'m>,
        resp: Option<&TypeTagInfo<'r>>,
    ) -> SmallVec<[Arc<dyn Receiver>; 1]> {
        let mut query = TypeTagQuery::from(msg);
        query.make_hashable();
        let hash = query.default_hash();

        if let Some(receivers) = self.receivers.get(&hash) {
            receivers
                .receivers
                .read()
                .iter()
                .filter(|e| e.mtype.test(&msg, resp))
                .map(|x| x.receiver.clone())
                .collect()
        } else {
            panic!()
        }
    }
}

impl Receiver for Dispatcher {
    fn poll_ready(
        &self,
        msg: &TypeTag,
        resp: Option<&TypeTag>,
        _cx: Option<&mut Context<'_>>,
    ) -> Poll<Result<Permit, Error>> {
        todo!()
    }

    fn handle(
        &self,
        pmt: Permit,
        msg: &mut dyn MessageCell,
        resp: Option<SharedCell>,
    ) -> Result<(), Error> {
        let resp_tq = resp.as_ref().map(|r| r.type_tag().info());
        let receivers = self.query_receivers(msg.type_tag().info(), resp_tq);

        if let Some(resp) = resp {
        } else {
            for r in receivers {
                r.handle(msg, resp);
            }
        }

        todo!()
    }

    fn poll_event(self: Pin<&Self>, cx: &mut Context<'_>) -> Poll<Result<Event, Error>> {
        todo!()
    }

    fn message_types(
        &self,
        cb: &mut dyn for<'a> FnMut(TypeTagQuery<'a>, TypeTagQuery<'a>) -> ControlFlow<()>,
    ) {
        let lock = self.message_types.read();

        for mt in lock.iter() {
            match cb(mt.request.clone(), mt.response.clone()) {
                ControlFlow::Continue(_) => continue,
                ControlFlow::Break(_) => break,
            }
        }
    }
}
