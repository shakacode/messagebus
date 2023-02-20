use crossbeam::queue::SegQueue;
use futures::task::AtomicWaker;
use parking_lot::Mutex;
use sharded_slab::Slab;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use crate::{
    bus::{Bus, TaskHandler},
    receiver::AbstractReceiver,
};

pub static WAKER_QUEUE: SegQueue<usize> = SegQueue::new();
pub static CURRENT_WAKER: AtomicWaker = AtomicWaker::new();

struct WakerHelper;

impl WakerHelper {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::clone_waker,
        Self::wake,
        Self::wake_by_ref,
        Self::drop_waker,
    );

    unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
        RawWaker::new(ptr, &Self::VTABLE)
    }

    unsafe fn wake(ptr: *const ()) {
        WAKER_QUEUE.push(ptr as usize);
        CURRENT_WAKER.wake();
    }

    unsafe fn wake_by_ref(ptr: *const ()) {
        WAKER_QUEUE.push(ptr as usize);
        CURRENT_WAKER.wake();
    }

    unsafe fn drop_waker(_ptr: *const ()) {}

    #[inline]
    fn waker(idx: usize) -> Waker {
        let raw = RawWaker::new(idx as _, &Self::VTABLE);
        unsafe { Waker::from_raw(raw) }
    }
}

struct PollEntry {
    task: Mutex<TaskHandler>,
    receiver: Arc<dyn AbstractReceiver>,
    multiple: bool,
}

pub struct PollingPool {
    pool: Slab<PollEntry>,
    in_flight: AtomicUsize,
    initialized: AtomicBool,
    closed: AtomicBool,
}

impl PollingPool {
    pub fn new() -> Self {
        PollingPool {
            pool: Slab::new(),
            in_flight: AtomicUsize::new(0),
            initialized: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn push(&self, task: TaskHandler, receiver: Arc<dyn AbstractReceiver>, multiple: bool) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        WAKER_QUEUE.push(
            self.pool
                .insert(PollEntry {
                    task: Mutex::new(task),
                    receiver,
                    multiple,
                })
                .unwrap(),
        );

        self.initialized.store(true, Ordering::Release);
        CURRENT_WAKER.wake();
    }

    #[inline]
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        CURRENT_WAKER.wake();
    }

    pub fn poll(&self, cx: &mut Context<'_>, bus: &Bus) -> Poll<()> {
        CURRENT_WAKER.register(cx.waker());

        if !self.initialized.load(Ordering::Acquire) {
            return Poll::Pending;
        }

        while let Some(idx) = WAKER_QUEUE.pop() {
            let Some(entry) = self.pool.get(idx) else { continue };

            let waker = WakerHelper::waker(idx);
            let mut cx = Context::from_waker(&waker);
            let mut lock = entry.task.lock();
            match entry.receiver.poll_result(&mut *lock, None, &mut cx, bus) {
                Poll::Ready(res) => {
                    if !entry.multiple || (entry.multiple && res.is_err()) {
                        self.pool.remove(idx);
                        self.in_flight.fetch_sub(1, Ordering::Release);
                    }

                    if entry.multiple && res.is_ok() {
                        WAKER_QUEUE.push(idx);
                    }

                    if let Err(err) = res {
                        println!("{}", err);
                    }
                }

                Poll::Pending => {
                    continue;
                }
            }
        }

        if self.in_flight.load(Ordering::Acquire) == 0 && self.closed.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
