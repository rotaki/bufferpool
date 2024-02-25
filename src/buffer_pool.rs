use crate::{
    buffer_frame::{BufferFrame, FrameReadGuard, FrameWriteGuard},
    page::Page,
};
use std::{
    cell::UnsafeCell,
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
};

pub const NUM_PAGES: usize = 1 << 16;

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

    pub fn get_frame(&self, index: usize) -> &BufferFrame {
        &self.pages[index]
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

    pub fn get_page_for_write(&self, id: usize) -> FrameWriteGuard {
        self.latch();
        let inner = unsafe { &mut *self.inner.get() };
        // Check if the page already exists
        if let Some(index) = inner.id_to_index.get(&id).copied() {
            let guard = inner.get_frame(index).write();
            self.unlatch();
            return guard;
        }
        let (old_id, index) = (0, 0); // TODO: implement a replacement policy. Returns (page_id, frame_index)
        inner.id_to_index.remove(&old_id);
        inner.id_to_index.insert(id, index);

        let mut guard = inner.get_frame(index).write();
        self.unlatch();

        let page = Page::new(); // TODO: read from disk
        guard.copy(&page);
        guard
    }

    pub fn get_page_for_read(&self, id: usize) -> FrameReadGuard {
        self.latch();
        let inner = unsafe { &mut *self.inner.get() };
        // Check if the page already exists
        if let Some(index) = inner.id_to_index.get(&id).copied() {
            let guard = inner.get_frame(index).read();
            self.unlatch();
            return guard;
        }
        let (old_id, index) = (0, 0); // TODO: implement a replacement policy. Returns (page_id, frame_index)
        inner.id_to_index.remove(&old_id);
        inner.id_to_index.insert(id, index);

        let mut guard = inner.get_frame(index).write();
        self.unlatch();

        let page = Page::new(); // TODO: read from disk
        guard.copy(&page);
        guard.downgrade()
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
                        let mut guard = bp_inner.get_frame(0).write();
                        guard[0] += 1;
                    }
                });
            }
        });

        // check if the counter is correct
        let guard = bp_inner.get_frame(0).read();
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
                        let mut guard = bp.get_page_for_write(id);
                        guard[0] += 1;
                    }
                });
            }
        });

        let guard = bp.get_page_for_read(id);
        assert_eq!(guard[0], num_threads * num_iterations);
    }
}
