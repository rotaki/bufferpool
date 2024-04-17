use log::debug;

use super::{
    buffer_frame::{BufferFrame, FrameReadGuard, FrameWriteGuard},
    EvictionPolicy,
};

use crate::{
    file_manager::{FMStatus, FileManager},
    page::{Page, PageId},
    utils::init_logger,
};

use std::{
    cell::UnsafeCell,
    collections::HashMap,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

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

#[derive(Debug)]
pub enum BPStatus {
    FileManagerNotFound,
    FileManagerError(FMStatus),
    PageNotFound,
    FrameLatchGrantFailed,
}

impl From<FMStatus> for BPStatus {
    fn from(s: FMStatus) -> Self {
        BPStatus::FileManagerError(s)
    }
}

impl std::fmt::Display for BPStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BPStatus::FileManagerNotFound => write!(f, "[BP] File manager not found"),
            BPStatus::FileManagerError(s) => s.fmt(f),
            BPStatus::PageNotFound => write!(f, "[BP] Page not found"),
            BPStatus::FrameLatchGrantFailed => write!(f, "[BP] Frame latch grant failed"),
        }
    }
}

pub struct BufferPool<T: EvictionPolicy> {
    path: PathBuf,
    latch: AtomicBool,
    frames: UnsafeCell<Vec<BufferFrame>>,
    id_to_index: UnsafeCell<HashMap<PageKey, usize>>, // (c_id, page_id) -> index
    container_to_file: UnsafeCell<HashMap<ContainerKey, FileManager>>,
    eviction_policy: UnsafeCell<T>,
}

impl<T> BufferPool<T>
where
    T: EvictionPolicy,
{
    pub fn new<P: AsRef<std::path::Path>>(
        path: P,
        num_frames: usize,
        eviction_policy: T,
    ) -> Result<Self, BPStatus> {
        init_logger();
        debug!("Buffer pool created: num_frames: {}", num_frames);

        // Identify all the directories. A directory corresponds to a database.
        // A file in the directory corresponds to a container.
        // Create a FileManager for each file and store it in the container.
        let mut container = HashMap::new();
        for entry in std::fs::read_dir(&path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                let db_id = path.file_name().unwrap().to_str().unwrap().parse().unwrap();
                for entry in std::fs::read_dir(&path).unwrap() {
                    let entry = entry.unwrap();
                    let path = entry.path();
                    if path.is_file() {
                        let c_id = path.file_name().unwrap().to_str().unwrap().parse().unwrap();
                        let fm = FileManager::new(&path)?;
                        container.insert(ContainerKey::new(db_id, c_id), fm);
                    }
                }
            }
        }

        let frames = (0..num_frames).map(|_| BufferFrame::default()).collect();
        Ok(BufferPool {
            path: path.as_ref().to_path_buf(),
            latch: AtomicBool::new(false),
            id_to_index: UnsafeCell::new(HashMap::new()),
            frames: UnsafeCell::new(frames),
            container_to_file: UnsafeCell::new(container),
            eviction_policy: UnsafeCell::new(eviction_policy),
        })
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

    // Invariant: The latch must be held when calling this function
    fn handle_page_fault(&self, key: PageKey, new_page: bool) -> Result<FrameWriteGuard, BPStatus> {
        let pages = unsafe { &mut *self.frames.get() };
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };
        let eviction_policy = unsafe { &mut *self.eviction_policy.get() };

        let index = eviction_policy.choose_victim();
        let mut guard = pages[index]
            .try_write(false)
            .ok_or(BPStatus::FrameLatchGrantFailed)?;

        // Evict old page if necessary
        if let Some(old_key) = guard.key() {
            if guard.dirty().swap(false, Ordering::Acquire) {
                let file = container_to_file
                    .get(&old_key.c_key)
                    .ok_or(BPStatus::FileManagerNotFound)?;
                file.write_page(old_key.page_id, &*guard)?;
            }
            id_to_index.remove(&old_key);
            debug!("Page evicted: {}", old_key);
        }

        // Create a new page or read from disk
        let page = if new_page {
            Page::new(key.page_id)
        } else {
            let file = container_to_file
                .get(&key.c_key)
                .ok_or(BPStatus::FileManagerNotFound)?;
            file.read_page(key.page_id)?
        };

        guard.copy(&page);
        id_to_index.insert(key, index);
        *guard.key() = Some(key);
        eviction_policy.reset(index);
        eviction_policy.update(index);

        debug!("Page loaded: key: {}", key);
        Ok(guard)
    }

    /// Create a new page for write in memory.
    /// NOTE: This function does not write the page to disk.
    /// See more at `handle_page_fault(key, new_page=true)`
    pub fn create_new_page_for_write(
        &self,
        c_key: ContainerKey,
    ) -> Result<FrameWriteGuard, BPStatus> {
        self.latch();

        let fm = (unsafe { &mut *self.container_to_file.get() })
            .entry(c_key)
            .or_insert_with(|| {
                FileManager::new(
                    &self
                        .path
                        .join(c_key.db_id.to_string())
                        .join(c_key.c_id.to_string()),
                )
                .unwrap()
            });

        let page_id = fm.get_new_page_id();
        let key = PageKey::new(c_key, page_id);
        let res = self.handle_page_fault(key, true);
        if let Ok(ref guard) = res {
            guard.dirty().store(true, Ordering::Release);
        }

        self.unlatch();
        res
    }

    pub fn get_page_for_write(&self, key: PageKey) -> Result<FrameWriteGuard, BPStatus> {
        self.latch();

        let result = match unsafe { &mut *self.id_to_index.get() }.get(&key) {
            Some(&index) => {
                let guard = (unsafe { &mut *self.frames.get() })[index].try_write(true);
                if guard.is_some() {
                    unsafe { &mut *self.eviction_policy.get() }.update(index);
                }
                guard.ok_or(BPStatus::FrameLatchGrantFailed)
            }
            None => {
                let res = self.handle_page_fault(key, false);
                // If guard is ok, mark the page as dirty
                if let Ok(ref guard) = res {
                    guard.dirty().store(true, Ordering::Release);
                }
                res
            }
        };
        self.unlatch();
        result
    }

    pub fn get_last_page_for_write(
        &self,
        c_key: ContainerKey,
    ) -> Result<FrameWriteGuard, BPStatus> {
        self.latch();

        let fm = if let Some(fm) = (unsafe { &mut *self.container_to_file.get() }).get(&c_key) {
            fm
        } else {
            self.unlatch();
            return Err(BPStatus::FileManagerNotFound);
        };

        let num_pages = fm.get_num_pages();
        if num_pages == 0 {
            panic!("Create at least one page before calling this function")
        }
        let page_id = num_pages - 1;
        let key = PageKey::new(c_key, page_id);
        let result = match unsafe { &mut *self.id_to_index.get() }.get(&key) {
            Some(&index) => {
                let guard = (unsafe { &mut *self.frames.get() })[index].try_write(true);
                if guard.is_some() {
                    unsafe { &mut *self.eviction_policy.get() }.update(index);
                }
                guard.ok_or(BPStatus::FrameLatchGrantFailed)
            }
            None => {
                let res = self.handle_page_fault(key, false);
                // If guard is ok, mark the page as dirty
                if let Ok(ref guard) = res {
                    guard.dirty().store(true, Ordering::Release);
                }
                res
            }
        };
        self.unlatch();
        result
    }

    pub fn get_page_for_read(&self, key: PageKey) -> Result<FrameReadGuard, BPStatus> {
        self.latch();

        let result = match unsafe { &mut *self.id_to_index.get() }.get(&key) {
            Some(&index) => {
                let guard = (unsafe { &mut *self.frames.get() })[index].try_read();
                if guard.is_some() {
                    unsafe { &mut *self.eviction_policy.get() }.update(index);
                }
                guard.ok_or(BPStatus::FrameLatchGrantFailed)
            }
            None => self
                .handle_page_fault(key, false)
                .map(|guard| guard.downgrade()),
        };
        self.unlatch();
        result
    }

    pub fn flush_all(&self) -> Result<(), BPStatus> {
        self.latch();

        for frame in unsafe { &*self.frames.get() }.iter() {
            let frame = loop {
                if let Some(guard) = frame.try_read() {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            if frame.dirty().swap(false, Ordering::Acquire) {
                // swap is required to avoid concurrent flushes
                let key = frame.key().unwrap();
                if let Some(file) = (unsafe { &mut *self.container_to_file.get() }).get(&key.c_key)
                {
                    file.write_page(key.page_id, &*frame)?;
                } else {
                    self.unlatch();
                    return Err(BPStatus::FileManagerNotFound);
                }
            }
        }

        self.unlatch();
        Ok(())
    }

    /// Reset the buffer pool to its initial state.
    /// This will not flush the dirty pages to disk.
    /// This also removes all the files in disk.
    pub fn reset(&self) {
        self.latch();

        unsafe { &mut *self.id_to_index.get() }.clear();
        unsafe { &mut *self.container_to_file.get() }.clear();
        unsafe { &mut *self.eviction_policy.get() }.reset_all();

        for frame in unsafe { &mut *self.frames.get() }.iter_mut() {
            let mut frame = loop {
                if let Some(guard) = frame.try_write(false) {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            frame.clear();
        }

        for entry in std::fs::read_dir(&self.path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            std::fs::remove_file(path).unwrap();
        }

        self.unlatch();
    }
}

#[cfg(test)]
impl<T: EvictionPolicy> BufferPool<T> {
    pub fn check_all_frames_unlatched(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            assert!(!frame.latch.is_exclusive());
            assert!(!frame.latch.is_shared());
        }
    }

    // Invariant: id_to_index contains all the pages in the buffer pool
    pub fn check_id_to_index(&self) {
        let id_to_index = unsafe { &*self.id_to_index.get() };
        let mut index_to_id = HashMap::new();
        for (k, &v) in id_to_index.iter() {
            index_to_id.insert(v, k);
        }
        let frames = unsafe { &*self.frames.get() };
        for (i, frame) in frames.iter().enumerate() {
            if index_to_id.contains_key(&i) {
                assert_eq!((unsafe { *frame.key.get() }).unwrap(), *index_to_id[&i]);
            } else {
                assert_eq!(unsafe { *frame.key.get() }, None);
            }
        }
        // println!("id_to_index: {:?}", id_to_index);
    }

    pub fn check_frame_id_and_page_id_match(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            if let Some(key) = unsafe { *frame.key.get() } {
                let page_id = unsafe { &*frame.page.get() }.get_id();
                assert_eq!(key.page_id, page_id);
            }
        }
    }

    pub fn is_in_buffer_pool(&self, key: PageKey) -> bool {
        let id_to_index = unsafe { &*self.id_to_index.get() };
        id_to_index.contains_key(&key)
    }
}

unsafe impl<T: EvictionPolicy> Sync for BufferPool<T> {}

#[cfg(test)]
mod tests {
    use super::super::LFUEvictionPolicy;
    use super::*;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_bp_and_frame_latch() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        // create a directory for the database
        std::fs::create_dir(temp_dir.path().join(db_id.to_string())).unwrap();
        {
            let num_pages = 10;
            let ep = LFUEvictionPolicy::new(num_pages);
            let bp = BufferPool::new(temp_dir.path(), num_pages, ep).unwrap();
            let c_key = ContainerKey::new(db_id, 0);
            let frame = bp.create_new_page_for_write(c_key).unwrap();
            let key = frame.key().unwrap();
            drop(frame);

            let num_threads = 3;
            let num_iterations = 80; // Note: u8 max value is 255
            thread::scope(|s| {
                for _ in 0..num_threads {
                    s.spawn(|| {
                        for _ in 0..num_iterations {
                            loop {
                                if let Ok(mut guard) = bp.get_page_for_write(key) {
                                    guard[0] += 1;
                                    break;
                                } else {
                                    // spin
                                    println!("spin: {:?}", thread::current().id());
                                    std::hint::spin_loop();
                                }
                            }
                        }
                    });
                }
            });

            bp.check_all_frames_unlatched();
            bp.check_id_to_index();
            let guard = bp.get_page_for_read(key).unwrap();
            assert_eq!(guard[0], num_threads * num_iterations);
        }
    }

    #[test]
    fn test_bp_write_back_simple() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        // create a directory for the database
        std::fs::create_dir(temp_dir.path().join(db_id.to_string())).unwrap();
        {
            let num_pages = 1;
            let ep = LFUEvictionPolicy::new(num_pages);
            let bp = BufferPool::new(temp_dir.path(), 1, ep).unwrap();
            let c_key = ContainerKey::new(db_id, 0);

            let key1 = {
                let mut guard = bp.create_new_page_for_write(c_key).unwrap();
                assert_eq!(guard[0], 0);
                guard[0] = 1;
                guard.key().unwrap()
            };
            let key2 = {
                let mut guard = bp.create_new_page_for_write(c_key).unwrap();
                assert_eq!(guard[0], 0);
                guard[0] = 2;
                guard.key().unwrap()
            };
            bp.check_all_frames_unlatched();
            // check contents of evicted page
            {
                let guard = bp.get_page_for_read(key1).unwrap();
                assert_eq!(guard[0], 1);
            }
            // check contents of the page in the buffer pool
            {
                let guard = bp.get_page_for_read(key2).unwrap();
                assert_eq!(guard[0], 2);
            }
        }
    }

    #[test]
    fn test_bp_write_back_many() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        // create a directory for the database
        std::fs::create_dir(temp_dir.path().join(db_id.to_string())).unwrap();
        {
            let mut keys = Vec::new();
            let num_pages = 1;
            let ep = LFUEvictionPolicy::new(num_pages);
            let bp = BufferPool::new(temp_dir.path(), 1, ep).unwrap();
            let c_key = ContainerKey::new(db_id, 0);

            for i in 0..100 {
                let mut guard = bp.create_new_page_for_write(c_key).unwrap();
                guard[0] = i;
                keys.push(guard.key().unwrap());

                bp.check_id_to_index();
            }
            bp.check_all_frames_unlatched();
            for (i, key) in keys.iter().enumerate() {
                let guard: FrameReadGuard<'_> = bp.get_page_for_read(*key).unwrap();
                assert_eq!(guard[0], i as u8);
            }
        }
    }

    #[test]
    fn test_bp_no_write_back_if_not_dirty() {
        // TODO: Implement a mock file manager to check if write_page is called
        // when replacing a non-dirty page with a new page.
    }
}
