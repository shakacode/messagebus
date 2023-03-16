use core::mem::MaybeUninit;
use core::ptr;
use std::task::Waker;

use parking_lot::Mutex;

const NUM_WAKERS: usize = 32;

pub struct WakeList {
    inner: Mutex<WakeListInner>,
}

impl WakeList {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(WakeListInner::new()),
        }
    }

    #[inline]
    pub fn register(&self, waker: &Waker) {
        self.inner.lock().push(waker.clone());
    }

    #[inline]
    pub fn wake_all(&self) {
        self.inner.lock().wake_all();
    }
}

pub(crate) struct WakeListInner {
    inner: [MaybeUninit<Waker>; NUM_WAKERS],
    curr: usize,
}

impl WakeListInner {
    pub(crate) fn new() -> Self {
        Self {
            inner: unsafe {
                // safety: Create an uninitialized array of `MaybeUninit`. The
                // `assume_init` is safe because the type we are claiming to
                // have initialized here is a bunch of `MaybeUninit`s, which do
                // not require initialization.
                MaybeUninit::uninit().assume_init()
            },
            curr: 0,
        }
    }

    #[inline]
    pub(crate) fn can_push(&self) -> bool {
        self.curr < NUM_WAKERS
    }

    #[inline]
    pub(crate) fn push(&mut self, val: Waker) {
        debug_assert!(self.can_push());

        self.inner[self.curr] = MaybeUninit::new(val);
        self.curr += 1;
    }

    pub(crate) fn wake_all(&mut self) {
        assert!(self.curr <= NUM_WAKERS);
        while self.curr > 0 {
            self.curr -= 1;
            let waker = unsafe { ptr::read(self.inner[self.curr].as_mut_ptr()) };
            waker.wake();
        }
    }
}

impl Drop for WakeListInner {
    fn drop(&mut self) {
        let slice = ptr::slice_from_raw_parts_mut(self.inner.as_mut_ptr() as *mut Waker, self.curr);
        unsafe { ptr::drop_in_place(slice) };
    }
}
