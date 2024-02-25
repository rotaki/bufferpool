use crate::page::Page;
use std::{
    cell::UnsafeCell,
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, Ordering},
};

pub const NUM_PAGES: usize = 1 << 16;

pub struct FrameHeader {
    pub latch: AtomicBool,
}

impl Default for FrameHeader {
    fn default() -> Self {
        FrameHeader {
            latch: AtomicBool::new(false),
        }
    }
}

impl FrameHeader {
    pub fn latch(&self) {
        while self.latch.swap(true, Ordering::Acquire) {
            // spin
            std::hint::spin_loop();
        }
    }

    pub fn unlatch(&self) {
        self.latch.store(false, Ordering::Release);
    }
}

pub struct BufferFrame {
    pub header: FrameHeader,
    pub page: UnsafeCell<Page>,
}

impl Default for BufferFrame {
    fn default() -> Self {
        BufferFrame {
            header: FrameHeader::default(),
            page: UnsafeCell::new(Page::new()),
        }
    }
}

unsafe impl Sync for BufferFrame {}

pub struct FrameGuard<'a> {
    pub buffer_frame: &'a BufferFrame,
}

impl<'a> Drop for FrameGuard<'a> {
    fn drop(&mut self) {
        self.buffer_frame.header.unlatch();
    }
}

impl Deref for FrameGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.buffer_frame.page.get() }
    }
}

impl DerefMut for FrameGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.buffer_frame.page.get() }
    }
}
struct BufferPoolInner {
    pub id_to_index: HashMap<usize, usize>,
    pub pages: Vec<BufferFrame>,
}

impl BufferPoolInner {
    pub fn new() -> Self {
        let pages = (0..NUM_PAGES).map(|_| BufferFrame::default()).collect();
        BufferPoolInner {
            id_to_index: HashMap::new(),
            pages,
        }
    }

    pub fn latch(&self, index: usize) -> FrameGuard {
        let frame = &self.pages[index];
        frame.header.latch();
        FrameGuard {
            buffer_frame: frame,
        }
    }
}

pub struct BufferPool {
    latch: AtomicBool,
    inner: UnsafeCell<BufferPoolInner>,
}

impl BufferPool {
    pub fn new() -> Self {
        BufferPool {
            latch: AtomicBool::new(false),
            inner: UnsafeCell::new(BufferPoolInner::new()),
        }
    }

    fn latch(&self) {
        while self.latch.swap(true, Ordering::Acquire) {
            // spin
            std::hint::spin_loop();
        }
    }

    fn unlatch(&self) {
        self.latch.store(false, Ordering::Release);
    }

    pub fn get_page(&self, id: usize) -> FrameGuard {
        self.latch();
        let inner = unsafe { &mut *self.inner.get() };
        // Check if the page already exists
        if let Some(index) = inner.id_to_index.get(&id).copied() {
            let guard = inner.latch(index);
            self.unlatch();
            return guard;
        }
        let index = 0; // TODO: implement a replacement policy
        let page = Page::new(); // TODO: read from disk
        inner.id_to_index.insert(id, index);
        let mut guard = inner.latch(index);
        guard.copy(&page);
        self.unlatch();
        guard
    }
}

unsafe impl Sync for BufferPool {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_frame_latch() {
        let bp_inner = BufferPoolInner::new();
        // multiple threads using frame as a counter
        let num_threads = 10;
        let num_iterations = 10;
        // scoped threads
        thread::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    for _ in 0..num_iterations {
                        let mut guard = bp_inner.latch(0);
                        guard[0] += 1;
                    }
                });
            }
        });

        // check if the counter is correct
        let guard = bp_inner.latch(0);
        assert_eq!(guard[0], num_threads * num_iterations);
    }

    #[test]
    fn test_bp_and_frame_latch() {
        let bp = BufferPool::new();
        let id = 0;
        let num_threads = 10;
        let num_iterations = 10;
        thread::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    for _ in 0..num_iterations {
                        let mut guard = bp.get_page(id);
                        guard[0] += 1;
                    }
                });
            }
        });

        let guard = bp.get_page(id);
        assert_eq!(guard[0], num_threads * num_iterations);
    }
}
