use super::buffer_frame::{FrameReadGuard, FrameWriteGuard};

use crate::{file_manager::FMStatus, page::PageId};

pub type DatabaseId = u16;
pub type ContainerId = u16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
        }
    }
}

pub trait MemPool: Sync {
    fn create_new_page_for_write(
        &self,
        c_key: ContainerKey,
    ) -> Result<FrameWriteGuard, MemPoolStatus>;
    fn get_page_for_write(&self, key: PageKey) -> Result<FrameWriteGuard, MemPoolStatus>;
    fn get_page_for_read(&self, key: PageKey) -> Result<FrameReadGuard, MemPoolStatus>;
    fn reset(&self);
}
