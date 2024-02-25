use crate::page::Page;
use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, Ordering},
};

pub const NUM_PAGES: usize = 1 << 16;

pub struct BufferFrame {
    pub latch: AtomicBool,
    pub page: UnsafeCell<Page>,
}

impl Default for BufferFrame {
    fn default() -> Self {
        BufferFrame {
            latch: AtomicBool::new(false),
            page: UnsafeCell::new(Page::new()),
        }
    }
}

unsafe impl Sync for BufferFrame {}

pub struct BufferPool {
    pub pages: Vec<BufferFrame>,
}

impl BufferPool {
    pub fn new() -> Self {
        let pages = (0..NUM_PAGES).map(|_| BufferFrame::default()).collect();
        BufferPool { pages }
    }

    pub fn lock(&self, page_id: usize) -> Guard {
        let page = &self.pages[page_id];
        while page.latch.swap(true, Ordering::Acquire) {
            // spin
            std::hint::spin_loop();
        }
        Guard { buffer_frame: page }
    }
}

pub struct Guard<'a> {
    pub buffer_frame: &'a BufferFrame,
}

impl<'a> Drop for Guard<'a> {
    fn drop(&mut self) {
        self.buffer_frame.latch.store(false, Ordering::Release);
    }
}

impl Deref for Guard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.buffer_frame.page.get() }
    }
}

impl DerefMut for Guard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.buffer_frame.page.get() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_lock() {
        let buffer_pool = BufferPool::new();
        // multiple threads using frame as a counter
        let num_threads = 10;
        let num_iterations = 10;
        // scoped threads
        thread::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    for _ in 0..num_iterations {
                        let mut guard = buffer_pool.lock(0);
                        guard[0] += 1;
                    }
                });
            }
        });

        // check if the counter is correct
        let guard = buffer_pool.lock(0);
        assert_eq!(guard[0], num_threads * num_iterations);
    }
}
