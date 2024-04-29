use super::mem_pool_trait::PageKey;
use crate::page::Page;
use crate::rwlatch::RwLatch;
use std::{
    cell::UnsafeCell,
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, Ordering},
};

pub struct BufferFrame {
    pub latch: RwLatch,
    pub is_dirty: AtomicBool,
    pub key: UnsafeCell<Option<PageKey>>,
    pub page: UnsafeCell<Page>,
    // phantom pinned to prevent moving out of the buffer frame
    _phantom: std::marker::PhantomPinned,
}

impl Default for BufferFrame {
    fn default() -> Self {
        BufferFrame {
            latch: RwLatch::default(),
            is_dirty: AtomicBool::new(false),
            key: UnsafeCell::new(None),
            page: UnsafeCell::new(Page::new_empty()),
            _phantom: std::marker::PhantomPinned,
        }
    }
}

unsafe impl Sync for BufferFrame {}

impl BufferFrame {
    pub fn read(&self) -> FrameReadGuard {
        self.latch.shared();
        FrameReadGuard {
            upgraded: AtomicBool::new(false),
            buffer_frame: self,
        }
    }

    pub fn try_read(&self) -> Option<FrameReadGuard> {
        if self.latch.try_shared() {
            Some(FrameReadGuard {
                upgraded: AtomicBool::new(false),
                buffer_frame: self,
            })
        } else {
            None
        }
    }

    pub fn write(&self, make_dirty: bool) -> FrameWriteGuard {
        self.latch.exclusive();
        if make_dirty {
            self.is_dirty.store(true, Ordering::Release);
        }
        FrameWriteGuard {
            downgraded: AtomicBool::new(false),
            buffer_frame: self,
        }
    }

    pub fn try_write(&self, make_dirty: bool) -> Option<FrameWriteGuard> {
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

pub struct FrameReadGuard<'a> {
    pub upgraded: AtomicBool,
    pub buffer_frame: &'a BufferFrame,
}

impl<'a> FrameReadGuard<'a> {
    pub fn key(&self) -> &Option<PageKey> {
        // SAFETY: This is safe because the latch is held shared.
        unsafe { &*self.buffer_frame.key.get() }
    }

    pub fn dirty(&self) -> &AtomicBool {
        &self.buffer_frame.is_dirty
    }

    pub fn try_upgrade(self, make_dirty: bool) -> Result<FrameWriteGuard<'a>, FrameReadGuard<'a>> {
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

impl<'a> Drop for FrameReadGuard<'a> {
    fn drop(&mut self) {
        if !self.upgraded.load(Ordering::Relaxed) {
            self.buffer_frame.latch.release_shared();
        }
    }
}

impl Deref for FrameReadGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        // SAFETY: This is safe because the latch is held shared.
        unsafe { &*self.buffer_frame.page.get() }
    }
}

impl Debug for FrameReadGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameReadGuard")
            .field("key", &self.key())
            .field("dirty", &self.dirty().load(Ordering::Relaxed))
            .finish()
    }
}

pub struct FrameWriteGuard<'a> {
    downgraded: AtomicBool,
    buffer_frame: &'a BufferFrame,
}

impl<'a> FrameWriteGuard<'a> {
    pub fn key(&self) -> &mut Option<PageKey> {
        // SAFETY: This is safe because the latch is held exclusively.
        unsafe { &mut *self.buffer_frame.key.get() }
    }

    pub fn dirty(&self) -> &AtomicBool {
        &self.buffer_frame.is_dirty
    }

    pub fn downgrade(self) -> FrameReadGuard<'a> {
        self.buffer_frame.latch.downgrade();
        self.downgraded.store(true, Ordering::Relaxed);
        FrameReadGuard {
            upgraded: AtomicBool::new(false),
            buffer_frame: self.buffer_frame,
        }
    }

    pub fn clear(&self) {
        self.buffer_frame.is_dirty.store(false, Ordering::Release);
        self.key().take();
    }
}

impl<'a> Drop for FrameWriteGuard<'a> {
    fn drop(&mut self) {
        if !self.downgraded.load(Ordering::Relaxed) {
            self.buffer_frame.latch.release_exclusive();
        }
    }
}

impl Deref for FrameWriteGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        // SAFETY: This is safe because the latch is held exclusively.
        unsafe { &*self.buffer_frame.page.get() }
    }
}

impl DerefMut for FrameWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: This is safe because the latch is held exclusively.
        unsafe { &mut *self.buffer_frame.page.get() }
    }
}

impl Debug for FrameWriteGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameWriteGuard")
            .field("key", &self.key())
            .field("dirty", &self.dirty().load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use super::*;

    #[test]
    fn test_default_buffer_frame() {
        let buffer_frame = BufferFrame::default();
        assert_eq!(buffer_frame.is_dirty.load(Ordering::Relaxed), false);
        assert!(unsafe { &*buffer_frame.key.get() }.is_none());
    }

    #[test]
    fn test_read_access() {
        let buffer_frame = BufferFrame::default();
        let guard = buffer_frame.read();
        assert_eq!(guard.key(), &None);
        assert_eq!(guard.dirty().load(Ordering::Relaxed), false);
        guard.iter().all(|&x| x == 0);
        assert!(!guard.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_write_access() {
        let buffer_frame = BufferFrame::default();
        let mut guard = buffer_frame.write(true);
        assert_eq!(guard.key(), &None);
        assert_eq!(guard.dirty().load(Ordering::Relaxed), true);
        guard.iter().all(|&x| x == 0);
        guard[0] = 1;
        assert_eq!(guard[0], 1);
        assert_eq!(guard.dirty().load(Ordering::Relaxed), true);
    }

    #[test]
    fn test_concurrent_read_access() {
        let buffer_frame = BufferFrame::default();
        let guard1 = buffer_frame.read();
        let guard2 = buffer_frame.read();
        assert_eq!(guard1.key(), &None);
        assert_eq!(guard2.key(), &None);
        assert_eq!(guard1.dirty().load(Ordering::Relaxed), false);
        assert_eq!(guard2.dirty().load(Ordering::Relaxed), false);
        guard1.iter().all(|&x| x == 0);
        guard2.iter().all(|&x| x == 0);
        assert!(!guard1.dirty().load(Ordering::Relaxed));
        assert!(!guard2.dirty().load(Ordering::Relaxed));
    }

    #[test]
    fn test_concurrent_write_access() {
        let buffer_frame = Arc::new(BufferFrame::default());
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
        let buffer_frame = BufferFrame::default();
        {
            // Upgrade read guard to write guard and modify the first element
            let guard = buffer_frame.read();
            let mut guard = guard.try_upgrade(true).unwrap();
            assert_eq!(guard.key(), &None);
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
        let buffer_frame = BufferFrame::default();
        let mut guard = buffer_frame.write(true);
        guard[0] = 1;
        let guard = guard.downgrade();
        assert_eq!(guard[0], 1);
        assert_eq!(guard.dirty().load(Ordering::Relaxed), true);
    }

    #[test]
    fn test_upgrade_and_downgrade_access() {
        let buffer_frame = BufferFrame::default();
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
        let buffer_frame = BufferFrame::default();
        let guard1 = buffer_frame.read();
        let guard2 = buffer_frame.read();
        assert!(guard1.try_upgrade(true).is_err());
    }
}
