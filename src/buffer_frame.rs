use crate::{page::Page, rwlatch::RwLatch};
use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
};

pub struct FrameHeader {
    pub latch: RwLatch,
}

impl Default for FrameHeader {
    fn default() -> Self {
        FrameHeader {
            latch: RwLatch::default(),
        }
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

impl BufferFrame {
    pub fn read(&self) -> FrameReadGuard {
        self.header.latch.shared();
        FrameReadGuard { buffer_frame: self }
    }

    pub fn try_read(&self) -> Option<FrameReadGuard> {
        if self.header.latch.try_shared() {
            Some(FrameReadGuard { buffer_frame: self })
        } else {
            None
        }
    }

    pub fn write(&self) -> FrameWriteGuard {
        self.header.latch.exclusive();
        FrameWriteGuard { buffer_frame: self }
    }

    pub fn try_write(&self) -> Option<FrameWriteGuard> {
        if self.header.latch.try_exclusive() {
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
    pub fn downgrade(self) -> FrameReadGuard<'a> {
        self.buffer_frame.header.latch.downgrade();
        FrameReadGuard {
            buffer_frame: self.buffer_frame,
        }
    }
}

impl<'a> Drop for FrameWriteGuard<'a> {
    fn drop(&mut self) {
        self.buffer_frame.header.latch.release_exclusive();
    }
}

impl Deref for FrameWriteGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.buffer_frame.page.get() }
    }
}

impl DerefMut for FrameWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.buffer_frame.page.get() }
    }
}

pub struct FrameReadGuard<'a> {
    pub buffer_frame: &'a BufferFrame,
}

impl<'a> Drop for FrameReadGuard<'a> {
    fn drop(&mut self) {
        self.buffer_frame.header.latch.release_shared();
    }
}

impl Deref for FrameReadGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.buffer_frame.page.get() }
    }
}
