use crate::{buffer_pool::ContainerPageKey, page::Page, rwlatch::RwLatch};
use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
};

pub struct BufferFrame {
    pub latch: RwLatch,
    pub key: UnsafeCell<Option<ContainerPageKey>>,
    pub page: UnsafeCell<Page>,
}

impl Default for BufferFrame {
    fn default() -> Self {
        BufferFrame {
            latch: RwLatch::default(),
            key: UnsafeCell::new(None),
            page: UnsafeCell::new(Page::new()),
        }
    }
}

unsafe impl Sync for BufferFrame {}

impl BufferFrame {
    pub fn read(&self) -> FrameReadGuard {
        self.latch.shared();
        FrameReadGuard { buffer_frame: self }
    }

    pub fn try_read(&self) -> Option<FrameReadGuard> {
        if self.latch.try_shared() {
            Some(FrameReadGuard { buffer_frame: self })
        } else {
            None
        }
    }

    pub fn write(&self) -> FrameWriteGuard {
        self.latch.exclusive();
        FrameWriteGuard { buffer_frame: self }
    }

    pub fn try_write(&self) -> Option<FrameWriteGuard> {
        if self.latch.try_exclusive() {
            Some(FrameWriteGuard { buffer_frame: self })
        } else {
            None
        }
    }
}

pub struct FrameWriteGuard<'a> {
    pub buffer_frame: &'a BufferFrame,
}

impl<'a> FrameWriteGuard<'a> {
    pub fn key(&self) -> &mut Option<ContainerPageKey> {
        // SAFETY: This is safe because the latch is held exclusively.
        unsafe { &mut *self.buffer_frame.key.get() }
    }

    pub fn downgrade(self) -> FrameReadGuard<'a> {
        self.buffer_frame.latch.downgrade();
        FrameReadGuard {
            buffer_frame: self.buffer_frame,
        }
    }
}

impl<'a> Drop for FrameWriteGuard<'a> {
    fn drop(&mut self) {
        self.buffer_frame.latch.release_exclusive();
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

pub struct FrameReadGuard<'a> {
    pub buffer_frame: &'a BufferFrame,
}

impl<'a> FrameReadGuard<'a> {
    pub fn key(&self) -> &Option<ContainerPageKey> {
        // SAFETY: This is safe because the latch is held shared.
        unsafe { &*self.buffer_frame.key.get() }
    }
}

impl<'a> Drop for FrameReadGuard<'a> {
    fn drop(&mut self) {
        self.buffer_frame.latch.release_shared();
    }
}

impl Deref for FrameReadGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        // SAFETY: This is safe because the latch is held shared.
        unsafe { &*self.buffer_frame.page.get() }
    }
}
