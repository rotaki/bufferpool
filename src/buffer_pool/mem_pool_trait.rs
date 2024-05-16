use super::buffer_frame::{FrameReadGuard, FrameWriteGuard};
use super::eviction_policy::EvictionPolicy;

use crate::{file_manager::FMStatus, page::PageId};

pub type DatabaseId = u16;
pub type ContainerId = u16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContainerKey {
    pub db_id: DatabaseId,
    pub c_id: ContainerId,
}

impl ContainerKey {
    pub fn new(db_id: DatabaseId, c_id: ContainerId) -> Self {
        ContainerKey { db_id, c_id }
    }
}

impl std::fmt::Display for ContainerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(db:{}, c:{})", self.db_id, self.c_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageKey {
    pub c_key: ContainerKey,
    pub page_id: PageId,
}

impl PageKey {
    pub fn new(c_key: ContainerKey, page_id: PageId) -> Self {
        PageKey { c_key, page_id }
    }
}

impl std::fmt::Display for PageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, p:{})", self.c_key, self.page_id)
    }
}

#[derive(Debug, PartialEq)]
pub enum MemPoolStatus {
    FileManagerNotFound,
    FileManagerError(FMStatus),
    PageNotFound,
    FrameReadLatchGrantFailed,
    FrameWriteLatchGrantFailed,
    CannotEvictPage,
    WriteBufferFull,
}

impl From<FMStatus> for MemPoolStatus {
    fn from(s: FMStatus) -> Self {
        MemPoolStatus::FileManagerError(s)
    }
}

impl std::fmt::Display for MemPoolStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemPoolStatus::FileManagerNotFound => write!(f, "[MP] File manager not found"),
            MemPoolStatus::FileManagerError(s) => s.fmt(f),
            MemPoolStatus::PageNotFound => write!(f, "[MP] Page not found"),
            MemPoolStatus::FrameReadLatchGrantFailed => {
                write!(f, "[MP] Frame read latch grant failed")
            }
            MemPoolStatus::FrameWriteLatchGrantFailed => {
                write!(f, "[MP] Frame write latch grant failed")
            }
            MemPoolStatus::CannotEvictPage => {
                write!(f, "[MP] All frames are latched and cannot evict page")
            }
            MemPoolStatus::WriteBufferFull => write!(f, "[MP] Write buffer is full"),
        }
    }
}

pub trait MemPool<T: EvictionPolicy>: Sync + Send {
    /// Create a new page for write.
    /// This function will allocate a new page in memory and return a FrameWriteGuard.
    /// In general, this function does not need to write the page to disk.
    /// Disk write will be handled when the page is evicted from the buffer pool.
    fn create_new_page_for_write(
        &self,
        c_key: ContainerKey,
    ) -> Result<FrameWriteGuard<T>, MemPoolStatus>;

    /// Get a page for read.
    /// This function will return a FrameReadGuard.
    /// This function assumes that a page is already created and either in memory or on disk.
    fn get_page_for_write(&self, key: PageKey) -> Result<FrameWriteGuard<T>, MemPoolStatus>;

    /// Get a page for write.
    /// This function will return a FrameWriteGuard.
    /// This function assumes that a page is already created and either in memory or on disk.
    fn get_page_for_read(&self, key: PageKey) -> Result<FrameReadGuard<T>, MemPoolStatus>;

    /// Reset the memory pool.
    /// This function will reset the memory pool to its initial state.
    /// This function is useful for testing purposes.
    fn reset(&self);

    #[cfg(test)]
    fn run_checks(&self);
}
