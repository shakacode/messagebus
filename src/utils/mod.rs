use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use self::wakelist::WakeList;

pub mod future_cell;
pub mod trait_object;
pub mod wakelist;

lazy_static::lazy_static! {
    static ref WAKER: Waker = unsafe {
        Waker::from_raw(RawWaker::new(std::ptr::null(), NullContext::VTABLE))
    };
}

pub struct NullContext;
impl NullContext {
    const VTABLE: &RawWakerVTable =
        &RawWakerVTable::new(Self::clone_stub, Self::stub, Self::stub, Self::stub);

    pub fn clone_stub(data: *const ()) -> RawWaker {
        RawWaker::new(data, Self::VTABLE)
    }

    pub fn stub(_: *const ()) {}
    pub fn new() -> Context<'static> {
        Context::from_waker(&WAKER)
    }
}

pub struct MutexGuard<'a, T> {
    _inner: parking_lot::MutexGuard<'a, T>,
    mutex: &'a Mutex<T>,
}

impl<'a, T> std::ops::Deref for MutexGuard<'a, T> {
    type Target = parking_lot::MutexGuard<'a, T>;

    fn deref(&self) -> &Self::Target {
        &self._inner
    }
}

impl<'a, T> std::ops::DerefMut for MutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self._inner
    }
}

impl<'a, T> MutexGuard<'a, T> {
    pub fn new(_inner: parking_lot::MutexGuard<'a, T>, mutex: &'a Mutex<T>) -> Self {
        Self { _inner, mutex }
    }
}

impl<'a, T> Drop for MutexGuard<'a, T> {
    fn drop(&mut self) {
        self.mutex.wakelist.wake_all();
    }
}

pub struct Mutex<T> {
    _inner: parking_lot::Mutex<T>,
    wakelist: WakeList,
}

impl<T> Mutex<T> {
    pub fn new(val: T) -> Self {
        Self {
            _inner: parking_lot::Mutex::new(val),
            wakelist: WakeList::new(),
        }
    }

    #[inline]
    pub fn poll_lock(&self, cx: &mut Context<'_>) -> Poll<MutexGuard<'_, T>> {
        if let Some(inner) = self._inner.try_lock() {
            Poll::Ready(MutexGuard::new(inner, self))
        } else {
            self.wakelist.register(cx.waker());
            Poll::Pending
        }
    }

    #[inline]
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        Some(MutexGuard::new(self._inner.try_lock()?, self))
    }

    #[inline]
    pub fn blocking_lock(&self) -> MutexGuard<'_, T> {
        MutexGuard::new(self._inner.lock(), self)
    }
}
