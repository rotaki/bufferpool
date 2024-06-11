use super::mem_pool_trait::PageFrameKey;
use super::{eviction_policy::EvictionPolicy, mem_pool_trait::PageKey};
use crate::page::Page;
use crate::rwlatch::RwLatch;
use std::{
    cell::UnsafeCell,
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, Ordering},
        RwLock,
    },
};

pub struct BufferFrame<T: EvictionPolicy> {
    frame_id: u32, // An index of the frame in the buffer pool. This is a constant value.
    latch: RwLatch,
    is_dirty: AtomicBool, // Can be updated even when ReadGuard is held (see flush_all() in buffer_pool.rs)
    evict_info: RwLock<T>, // Can be updated even when ReadGuard is held (see get_page_for_read() in buffer_pool.rs)
    key: UnsafeCell<Option<PageKey>>, // Can only be updated when WriteGuard is held
    page: UnsafeCell<Page>, // Can only be updated when WriteGuard is held
}

unsafe impl<T: EvictionPolicy> Sync for BufferFrame<T> {}

impl<T: EvictionPolicy> BufferFrame<T> {
    pub fn new(frame_id: u32) -> Self {
        BufferFrame {
            frame_id,
            latch: RwLatch::default(),
            is_dirty: AtomicBool::new(false),
            key: UnsafeCell::new(None),
            evict_info: RwLock::new(T::new()),
            page: UnsafeCell::new(Page::new_empty()),
        }
    }

    pub fn frame_id(&self) -> u32 {
        self.frame_id
    }

    pub fn read(&self) -> FrameReadGuard<T> {
        self.latch.shared();
        FrameReadGuard {
            upgraded: AtomicBool::new(false),
            buffer_frame: self,
        }
    }

    pub fn try_read(&self) -> Option<FrameReadGuard<T>> {
        if self.latch.try_shared() {
            Some(FrameReadGuard {
                upgraded: AtomicBool::new(false),
                buffer_frame: self,
            })
        } else {
            None
        }
    }

    pub fn write(&self, make_dirty: bool) -> FrameWriteGuard<T> {
        self.latch.exclusive();
        if make_dirty {
            self.is_dirty.store(true, Ordering::Release);
        }
        FrameWriteGuard {
            downgraded: AtomicBool::new(false),
            buffer_frame: self,
        }
    }

    pub fn try_write(&self, make_dirty: bool) -> Option<FrameWriteGuard<T>> {
        if self.latch.try_exclusive() {
            if make_dirty {
                self.is_dirty.store(true, Ordering::Release);
            }
            Some(FrameWriteGuard {
                downgraded: AtomicBool::new(false),
                buffer_frame: self,
            })
        } else {
            None
        }
    }
}

impl<E: EvictionPolicy> Deref for BufferFrame<E> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        // SAFETY: This is safe because the latch is held shared.
        unsafe { &*self.page.get() }
    }
}

pub struct FrameReadGuard<'a, T: EvictionPolicy> {
    upgraded: AtomicBool,
    buffer_frame: &'a BufferFrame<T>,
}

impl<'a, T: EvictionPolicy> FrameReadGuard<'a, T> {
    pub fn frame_id(&self) -> u32 {
        self.buffer_frame.frame_id
    }

    pub(crate) fn page_key(&self) -> &Option<PageKey> {
        // SAFETY: This is safe because the latch is held shared.
        unsafe { &*self.buffer_frame.key.get() }
    }

    pub fn page_frame_key(&self) -> Option<PageFrameKey> {
        self.page_key().map(|p_key| {
            PageFrameKey::new_with_frame_id(p_key.c_key, p_key.page_id, self.frame_id())
        })
    }

    /// Returns a reference to the dirty flag.
    /// The flag can be modified even with the FrameReadGuard
    /// when dirty pages are flushed to disk.
    pub fn dirty(&self) -> &AtomicBool {
        &self.buffer_frame.is_dirty
    }

    pub fn evict_info(&self) -> &RwLock<T> {
        &self.buffer_frame.evict_info
    }

    pub fn try_upgrade(
        self,
        make_dirty: bool,
    ) -> Result<FrameWriteGuard<'a, T>, FrameReadGuard<'a, T>> {
        if self.buffer_frame.latch.try_upgrade() {
            self.upgraded.store(true, Ordering::Relaxed);
            if make_dirty {
                self.buffer_frame.is_dirty.store(true, Ordering::Release);
            }
            Ok(FrameWriteGuard {
                downgraded: AtomicBool::new(false),
                buffer_frame: self.buffer_frame,
            })
        } else {
            Err(self)
        }
    }
}

impl<'a, T: EvictionPolicy> Drop for FrameReadGuard<'a, T> {
    fn drop(&mut self) {
        if !self.upgraded.load(Ordering::Relaxed) {
            self.buffer_frame.latch.release_shared();
        }
    }
}

impl<T: EvictionPolicy> Deref for FrameReadGuard<'_, T> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        // SAFETY: This is safe because the latch is held shared.
        unsafe { &*self.buffer_frame.page.get() }
    }
}

impl<T: EvictionPolicy> Debug for FrameReadGuard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameReadGuard")
            .field("key", &self.page_key())
            .field("dirty", &self.dirty().load(Ordering::Relaxed))
            .finish()
    }
}

pub struct FrameWriteGuard<'a, T: EvictionPolicy> {
    downgraded: AtomicBool,
    buffer_frame: &'a BufferFrame<T>,
}

impl<'a, T: EvictionPolicy> FrameWriteGuard<'a, T> {
    pub fn frame_id(&self) -> u32 {
        self.buffer_frame.frame_id
    }

    pub(crate) fn page_key(&self) -> &Option<PageKey> {
        // SAFETY: This is safe because the latch is held exclusively.
        unsafe { &*self.buffer_frame.key.get() }
    }

    pub(crate) fn page_key_mut(&mut self) -> &mut Option<PageKey> {
        // SAFETY: This is safe because the latch is held exclusively.
        unsafe { &mut *self.buffer_frame.key.get() }
    }

    pub fn page_frame_key(&self) -> Option<PageFrameKey> {
        self.page_key().map(|p_key| {
            PageFrameKey::new_with_frame_id(p_key.c_key, p_key.page_id, self.frame_id())
        })
    }

    pub fn dirty(&self) -> &AtomicBool {
        &self.buffer_frame.is_dirty
    }

    pub fn evict_info(&self) -> &RwLock<T> {
        &self.buffer_frame.evict_info
    }

    pub fn eviction_score(&self) -> u64 {
        self.buffer_frame
            .evict_info
            .read()
            .unwrap()
            .score(self.buffer_frame)
    }

    pub fn downgrade(self) -> FrameReadGuard<'a, T> {
        self.buffer_frame.latch.downgrade();
        self.downgraded.store(true, Ordering::Relaxed);
        FrameReadGuard {
            upgraded: AtomicBool::new(false),
            buffer_frame: self.buffer_frame,
        }
    }

    pub fn clear(&mut self) {
        self.buffer_frame.is_dirty.store(false, Ordering::Release);
        self.buffer_frame.evict_info.write().unwrap().reset();
        self.page_key_mut().take();
    }
}

impl<'a, T: EvictionPolicy> Drop for FrameWriteGuard<'a, T> {
    fn drop(&mut self) {
        if !self.downgraded.load(Ordering::Relaxed) {
            self.buffer_frame.latch.release_exclusive();
        }
    }
}

impl<T: EvictionPolicy> Deref for FrameWriteGuard<'_, T> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        // SAFETY: This is safe because the latch is held exclusively.
        unsafe { &*self.buffer_frame.page.get() }
    }
}

impl<T: EvictionPolicy> DerefMut for FrameWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: This is safe because the latch is held exclusively.
        unsafe { &mut *self.buffer_frame.page.get() }
    }
}

impl<T: EvictionPolicy> Debug for FrameWriteGuard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameWriteGuard")
            .field("key", &self.page_key())
            .field("dirty", &self.dirty().load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use crate::bp::eviction_policy::DummyEvictionPolicy;

    use super::*;

    type TestBufferFrame = BufferFrame<DummyEvictionPolicy>;

    #[test]
    fn test_default_buffer_frame() {
        let buffer_frame = TestBufferFrame::new(0);
        assert_eq!(buffer_frame.is_dirty.load(Ordering::Relaxed), false);
        assert!(unsafe { &*buffer_frame.key.get() }.is_none());
    }

    #[test]
    fn test_read_access() {
        let buffer_frame = TestBufferFrame::new(0);
        let guard = buffer_frame.read();
        assert_eq!(guard.page_key(), &None);
        assert_eq!(guard.dirty().load(Ordering::Relaxed), false);
        guard.iter().all(|&x| x == 0);
        assert!(!guard.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_write_access() {
        let buffer_frame = TestBufferFrame::new(0);
        let mut guard = buffer_frame.write(true);
        assert_eq!(guard.page_key(), &None);
        assert_eq!(guard.dirty().load(Ordering::Relaxed), true);
        guard.iter().all(|&x| x == 0);
        guard[0] = 1;
        assert_eq!(guard[0], 1);
        assert_eq!(guard.dirty().load(Ordering::Relaxed), true);
    }

    #[test]
    fn test_concurrent_read_access() {
        let buffer_frame = TestBufferFrame::new(0);
        let guard1 = buffer_frame.read();
        let guard2 = buffer_frame.read();
        assert_eq!(guard1.page_key(), &None);
        assert_eq!(guard2.page_key(), &None);
        assert_eq!(guard1.dirty().load(Ordering::Relaxed), false);
        assert_eq!(guard2.dirty().load(Ordering::Relaxed), false);
        guard1.iter().all(|&x| x == 0);
        guard2.iter().all(|&x| x == 0);
        assert!(!guard1.dirty().load(Ordering::Relaxed));
        assert!(!guard2.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_concurrent_write_access() {
        let buffer_frame = Arc::new(TestBufferFrame::new(0));
        // Instantiate three threads, each increments the first element of the page by 1 for 80 times.
        // (80 * 3 < 255 so that the first element does not overflow)

        // scoped threads
        thread::scope(|s| {
            let t1_frame = buffer_frame.clone();
            let t2_frame = buffer_frame.clone();
            let t3_frame = buffer_frame.clone();
            let t1 = s.spawn(move || {
                for _ in 0..80 {
                    let mut guard1 = t1_frame.write(true);
                    guard1[0] += 1;
                }
            });
            let t2 = s.spawn(move || {
                for _ in 0..80 {
                    let mut guard2 = t2_frame.write(true);
                    guard2[0] += 1;
                }
            });
            let t3 = s.spawn(move || {
                for _ in 0..80 {
                    let mut guard3 = t3_frame.write(true);
                    guard3[0] += 1;
                }
            });
            t1.join().unwrap();
            t2.join().unwrap();
            t3.join().unwrap();
        });

        // Check if the first element is 240
        let guard = buffer_frame.read();
        assert_eq!(guard[0], 240);
    }

    #[test]
    fn test_upgrade_access() {
        let buffer_frame = TestBufferFrame::new(0);
        {
            // Upgrade read guard to write guard and modify the first element
            let guard = buffer_frame.read();
            let mut guard = guard.try_upgrade(true).unwrap();
            assert_eq!(guard.page_key(), &None);
            assert_eq!(guard.dirty().load(Ordering::Relaxed), true);
            guard.iter().all(|&x| x == 0);
            guard[0] = 1;
            assert_eq!(guard[0], 1);
            assert_eq!(guard.dirty().load(Ordering::Relaxed), true);
        }
        let guard = buffer_frame.read();
        assert_eq!(guard[0], 1);
        assert_eq!(guard.dirty().load(Ordering::Relaxed), true);
    }

    #[test]
    fn test_downgrade_access() {
        let buffer_frame = TestBufferFrame::new(0);
        let mut guard = buffer_frame.write(true);
        guard[0] = 1;
        let guard = guard.downgrade();
        assert_eq!(guard[0], 1);
        assert_eq!(guard.dirty().load(Ordering::Relaxed), true);
    }

    #[test]
    fn test_upgrade_and_downgrade_access() {
        let buffer_frame = TestBufferFrame::new(0);
        // read -> write(dirty=false) -> read -> write(dirty=true) -> read
        let guard = buffer_frame.read();
        assert_eq!(guard.dirty().load(Ordering::Relaxed), false);
        let mut guard = guard.try_upgrade(false).unwrap();
        guard[0] = 1;
        assert_eq!(guard.dirty().load(Ordering::Relaxed), false);
        let guard = guard.downgrade();
        assert_eq!(guard.dirty().load(Ordering::Relaxed), false);
        let mut guard = guard.try_upgrade(true).unwrap();
        guard[0] += 1;
        assert_eq!(guard.dirty().load(Ordering::Relaxed), true);
        let guard = guard.downgrade();
        assert_eq!(guard[0], 2);
        assert_eq!(guard.dirty().load(Ordering::Relaxed), true);
    }

    #[test]
    fn test_concurrent_upgrade_failure() {
        let buffer_frame = TestBufferFrame::new(0);
        let guard1 = buffer_frame.read();
        let _guard2 = buffer_frame.read();
        assert!(guard1.try_upgrade(true).is_err());
    }
}
