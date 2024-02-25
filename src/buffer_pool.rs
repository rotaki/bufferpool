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

pub struct BufferPool {
    latch: AtomicBool,
    pages: UnsafeCell<Vec<BufferFrame>>,
    id_to_index: UnsafeCell<HashMap<usize, usize>>,
}

impl BufferPool {
    pub fn new() -> Self {
        let pages = (0..NUM_PAGES).map(|_| BufferFrame::default()).collect();
        BufferPool {
            latch: AtomicBool::new(false),
            id_to_index: UnsafeCell::new(HashMap::new()),
            pages: UnsafeCell::new(pages),
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

    pub fn get_page_for_write(&self, id: usize) -> Option<FrameWriteGuard> {
        self.latch();
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let pages = unsafe { &mut *self.pages.get() };
        // Check if the page already exists
        if let Some(index) = id_to_index.get(&id).copied() {
            let res = pages[index].try_write();
            self.unlatch();
            res
        } else {
            // Choose a page to evict
            let (old_id, index) = (0, 0); // TODO: implement a replacement policy. Returns (page_id, frame_index)
            if let Some(mut guard) = pages[index].try_write() {
                // Always get the frame latch before modifying the id_to_index map
                // Modify the id_to_index map
                id_to_index.remove(&old_id);
                id_to_index.insert(id, index);
                self.unlatch();

                let page = Page::new(); // TODO: read from disk
                guard.copy(&page);
                Some(guard)
            } else {
                self.unlatch();
                None
            }
        }
    }

    pub fn get_page_for_read(&self, id: usize) -> Option<FrameReadGuard> {
        self.latch();
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let pages = unsafe { &mut *self.pages.get() };

        // Check if the page already exists
        if let Some(index) = id_to_index.get(&id).copied() {
            let res = pages[index].try_read();
            self.unlatch();
            res
        } else {
            // Choose a page to evict
            let (old_id, index) = (0, 0); // TODO: implement a replacement policy. Returns (page_id, frame_index)
            if let Some(mut guard) = pages[index].try_write() {
                // Always get the frame latch before modifying the id_to_index map
                // Modify the id_to_index map
                id_to_index.remove(&old_id);
                id_to_index.insert(id, index);
                self.unlatch();

                let page = Page::new(); // TODO: read from disk
                guard.copy(&page);
                Some(guard.downgrade())
            } else {
                self.unlatch();
                None
            }
        }
    }
}

unsafe impl Sync for BufferPool {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

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
                        loop {
                            if let Some(mut guard) = bp.get_page_for_write(id) {
                                guard[0] += 1;
                                break;
                            } else {
                                // spin
                                std::hint::spin_loop();
                            }
                        }
                    }
                });
            }
        });

        let guard = bp.get_page_for_read(id).unwrap();
        assert_eq!(guard[0], num_threads * num_iterations);
    }
}
