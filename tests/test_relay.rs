use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use messagebus::{
    derive::{Error as MbError, Message},
    error::{self, GenericError},
    receivers, Action, AsyncHandler, Bus, Event, Message, MessageBounds, ReciveUntypedReceiver,
    SendUntypedReceiver, TypeTagAccept, TypeTagAcceptItem, TypeTagged,
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
                    return true;
                }
            } else {
                return true;
            }
        }

        if msg.as_ref() == Msg::<i32>::type_tag_().as_ref() {
            if let Some(resp) = resp {
                if resp.as_ref() == Msg::<u64>::type_tag_().as_ref() {
                    return true;
                }
            } else {
                return true;
            }
        }

        false
    }

    fn accept_msg(&self, msg: &messagebus::TypeTag) -> bool {
        if msg.as_ref() == Msg::<i32>::type_tag_().as_ref() {
            return true;
        }

        false
    }

    fn iter_types(&self) -> Box<dyn Iterator<Item = TypeTagAcceptItem>> {
        Box::new(
            std::iter::once((Msg::<i32>::type_tag_(), None))
                .chain(std::iter::once((
                    Msg::<i32>::type_tag_(),
                    Some((Msg::<u64>::type_tag_(), GenericError::type_tag_())),
                )))
                .chain(std::iter::once((
                    Msg::<i16>::type_tag_(),
                    Some((Msg::<u8>::type_tag_(), GenericError::type_tag_())),
                ))),
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

    b.flush_all().await;
    b.close().await;
    poller.await;
}

/// Test concurrent access to relay's try_reserve and reserve_notify.
/// This validates the fix for the TOCTOU race condition in relay.rs where
/// the check-then-insert-then-get pattern was replaced with atomic entry().
///
/// The test spawns concurrent tasks that simultaneously make requests through
/// the relay, exercising the DashMap entry() API. If the old TOCTOU pattern
/// were still present and entries could be removed, this would panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_relay_concurrent_access() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let (stx, srx) = mpsc::unbounded_channel();
    let relay = TestRelay {
        stx,
        srx: Mutex::new(Some(srx)),
    };

    let (b, poller) = Bus::build().register_relay(relay).build();

    let bus = Arc::new(b);
    let success_count = Arc::new(AtomicUsize::new(0));

    // Spawn concurrent tasks that all make requests simultaneously.
    // This exercises the concurrent entry() access in try_reserve/reserve_notify.
    // We use a small number of requests to keep the test fast.
    let num_tasks = 16;
    let mut handles = Vec::with_capacity(num_tasks);

    for task_id in 0..num_tasks {
        let bus = Arc::clone(&bus);
        let success_count = Arc::clone(&success_count);

        let handle = tokio::spawn(async move {
            // Use two different message types to exercise concurrent access
            // to different DashMap entries
            let success = if task_id % 2 == 0 {
                bus.request::<Msg<i32>, Msg<u64>>(Msg(task_id as i32), Default::default())
                    .await
                    .is_ok()
            } else {
                bus.request::<Msg<i16>, Msg<u8>>(Msg(task_id as i16), Default::default())
                    .await
                    .is_ok()
            };

            if success {
                success_count.fetch_add(1, Ordering::Relaxed);
            }
        });

        handles.push(handle);
    }

    // Wait for all tasks - any panic here would indicate a race condition
    for handle in handles {
        handle
            .await
            .expect("Task panicked - possible race condition in relay!");
    }

    let total_success = success_count.load(Ordering::Relaxed);
    assert_eq!(
        total_success, num_tasks,
        "Expected all {} requests to succeed, got {}",
        num_tasks, total_success
    );

    bus.flush_all().await;
    bus.close().await;
    poller.await;
}
