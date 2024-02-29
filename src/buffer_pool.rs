use log::debug;

use crate::{
    buffer_frame::{BufferFrame, FrameReadGuard, FrameWriteGuard},
    eviction_policy::EvictionPolicy,
    file_manager::FileManager,
    page::Page,
    utils::init_logger,
};
use std::{
    cell::UnsafeCell,
    collections::{hash_map::Entry, HashMap},
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContainerPageKey {
    container_id: usize,
    page_id: usize,
}

impl ContainerPageKey {
    pub fn new(container_id: usize, page_id: usize) -> Self {
        ContainerPageKey {
            container_id,
            page_id,
        }
    }
}

pub struct BufferPool {
    path: PathBuf,
    latch: AtomicBool,
    frames: UnsafeCell<Vec<BufferFrame>>,
    id_to_index: UnsafeCell<HashMap<ContainerPageKey, usize>>, // (container_id, page_id) -> index
    container_to_file: UnsafeCell<HashMap<usize, FileManager>>,
    eviction_policy: UnsafeCell<EvictionPolicy>,
}

impl BufferPool {
    pub fn new<P: AsRef<std::path::Path>>(path: P, num_pages: usize) -> Self {
        init_logger();
        debug!("Buffer pool created: num_pages: {}", num_pages);

        // Identify all the files in the directory. Parse the file name to a number.
        // Create a FileManager for each file and store it in the container.
        let mut container = HashMap::new();
        for entry in std::fs::read_dir(&path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let file_name = path.file_name().unwrap().to_str().unwrap();
            let file_id = file_name.parse::<usize>().unwrap();
            let file_manager = FileManager::new(path);
            container.insert(file_id, file_manager);
        }

        let frames = (0..num_pages).map(|_| BufferFrame::default()).collect();
        BufferPool {
            path: path.as_ref().to_path_buf(),
            latch: AtomicBool::new(false),
            id_to_index: UnsafeCell::new(HashMap::new()),
            frames: UnsafeCell::new(frames),
            container_to_file: UnsafeCell::new(container),
            eviction_policy: UnsafeCell::new(EvictionPolicy::new(num_pages)),
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

    pub fn create_new_page<T: Fn(&mut Page)>(
        &self,
        container_id: usize,
        init: &T,
    ) -> ContainerPageKey {
        // Reading and writing to the following data structures must be done while holding the latch
        // on the buffer pool.
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        self.latch();

        match container_to_file.entry(container_id) {
            Entry::Occupied(mut entry) => {
                let file_manager = entry.get_mut();
                self.unlatch();

                let page_id = file_manager.get_new_page_id();
                let mut page = Page::new();
                init(&mut page);
                // TODO: Write log
                file_manager.write_page(page_id, &page);
                debug!(
                    "New page created: c_id: {}, p_id: {}",
                    container_id, page_id
                );
                ContainerPageKey::new(container_id, page_id)
            }
            Entry::Vacant(entry) => {
                let file_manager = FileManager::new(self.path.join(container_id.to_string()));
                let file_manager = entry.insert(file_manager);
                self.unlatch();

                let page_id = file_manager.get_new_page_id();
                let mut page = Page::new();
                init(&mut page);
                // TODO: Write log
                file_manager.write_page(page_id, &page);
                debug!(
                    "New page created: c_id: {}, p_id: {}",
                    container_id, page_id
                );
                ContainerPageKey::new(container_id, page_id)
            }
        }
    }

    pub fn get_page_for_write(&self, key: ContainerPageKey) -> Option<FrameWriteGuard> {
        // Reading and writing to the following data structures must be done while holding the latch
        // on the buffer pool.
        let pages = unsafe { &mut *self.frames.get() };
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };
        let eviction_policy = unsafe { &mut *self.eviction_policy.get() };

        self.latch();

        // Check if the page already exists
        if let Some(index) = id_to_index.get(&key).copied() {
            let res = pages[index].try_write();

            eviction_policy.update(index);

            self.unlatch();
            res
        } else {
            let index = eviction_policy.choose_victim(); // Victim frame index

            if let Some(mut guard) = pages[index].try_write() {
                // Always get the frame latch before modifying the id_to_index map
                let old_key = guard.key();
                let is_dirty = guard.is_dirty();
                if let Some(old_key_inner) = old_key {
                    // If the page is dirty, write it to disk
                    if *is_dirty {
                        let file = container_to_file
                            .get(&old_key_inner.container_id)
                            .expect("file not found");
                        file.write_page(old_key_inner.page_id, &*guard);
                    }
                    id_to_index.remove(&old_key_inner);
                    debug!(
                        "Page evicted: c_id: {}, p_id: {}",
                        old_key_inner.container_id, old_key_inner.page_id
                    );
                }
                id_to_index.insert(key, index);
                *old_key = Some(key);
                *is_dirty = false;

                eviction_policy.reset(index);
                eviction_policy.update(index);

                let file = container_to_file
                    .get(&key.container_id)
                    .expect("file not found");
                self.unlatch();

                let page = file.read_page(key.page_id);
                guard.copy(&page);
                debug!(
                    "Page loaded: c_id: {}, p_id: {}",
                    key.container_id, key.page_id
                );
                Some(guard)
            } else {
                self.unlatch();
                None
            }
        }
    }

    pub fn get_page_for_read(&self, key: ContainerPageKey) -> Option<FrameReadGuard> {
        // Modification to the following data structures must be done while holding the latch
        // on the buffer pool.

        let pages = unsafe { &mut *self.frames.get() };
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };
        let eviction_policy = unsafe { &mut *self.eviction_policy.get() };

        self.latch();

        // Check if the page already exists
        if let Some(index) = id_to_index.get(&key).copied() {
            let res = pages[index].try_read();

            eviction_policy.update(index);

            self.unlatch();
            res
        } else {
            let index = eviction_policy.choose_victim();

            if let Some(mut guard) = pages[index].try_write() {
                // Always get the frame latch before modifying the id_to_index map
                let old_key = guard.key();
                let is_dirty = guard.is_dirty();
                if let Some(old_key_inner) = old_key {
                    // If the page is dirty, write it to disk
                    if *is_dirty {
                        let file = container_to_file
                            .get(&old_key_inner.container_id)
                            .expect("file not found");
                        file.write_page(old_key_inner.page_id, &*guard);
                    }
                    id_to_index.remove(&old_key_inner);
                    debug!(
                        "Page evicted: c_id: {}, p_id: {}",
                        old_key_inner.container_id, old_key_inner.page_id
                    );
                }
                id_to_index.insert(key, index);
                *old_key = Some(key);
                *is_dirty = false;

                eviction_policy.reset(index);
                eviction_policy.update(index);

                let file = container_to_file
                    .get(&key.container_id)
                    .expect("file not found");
                self.unlatch();

                let page = file.read_page(key.page_id);
                guard.copy(&page);
                debug!(
                    "Page loaded: c_id: {}, p_id: {}",
                    key.container_id, key.page_id
                );
                Some(guard.downgrade())
            } else {
                self.unlatch();
                None
            }
        }
    }
}

#[cfg(test)]
impl BufferPool {
    pub fn check_all_frames_unlatched(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            assert!(!frame.latch.is_exclusive());
            assert!(!frame.latch.is_shared());
        }
    }

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

    pub fn is_in_buffer_pool(&self, key: ContainerPageKey) -> bool {
        let id_to_index = unsafe { &*self.id_to_index.get() };
        id_to_index.contains_key(&key)
    }
}

unsafe impl Sync for BufferPool {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_bp_and_frame_latch() {
        let temp_dir = TempDir::new().unwrap();
        {
            let bp = BufferPool::new(temp_dir.path(), 10);
            let key = bp.create_new_page(0, &|_: &mut Page| {});
            let num_threads = 3;
            let num_iterations = 80; // Note: u8 max value is 255
            thread::scope(|s| {
                for _ in 0..num_threads {
                    s.spawn(|| {
                        for _ in 0..num_iterations {
                            loop {
                                if let Some(mut guard) = bp.get_page_for_write(key) {
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
        let tempdir = TempDir::new().unwrap();
        {
            let bp = BufferPool::new(tempdir.path(), 1);
            let container_id = 0;
            let key1 = {
                let key = bp.create_new_page(container_id, &|page: &mut Page| {
                    page[0] = 0;
                });
                let mut guard = bp.get_page_for_write(key).unwrap();
                assert_eq!(guard[0], 0);
                guard[0] = 1;
                key
            };
            let key2 = {
                let new_key = bp.create_new_page(container_id, &|page: &mut Page| {
                    page[0] = 0;
                });
                let mut guard = bp.get_page_for_write(new_key).unwrap();
                assert_eq!(guard[0], 0);
                guard[0] = 2;
                new_key
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
        {
            let mut keys = Vec::new();
            let bp = BufferPool::new(temp_dir.path(), 1);
            let container_id = 0;
            for i in 0..100 {
                let key = bp.create_new_page(container_id, &|page: &mut Page| {
                    page[0] = 0;
                });
                let mut guard = bp.get_page_for_write(key).unwrap();
                guard[0] = i;
                keys.push(key);

                bp.check_id_to_index();
            }
            bp.check_all_frames_unlatched();
            for (i, key) in keys.iter().enumerate() {
                let guard: FrameReadGuard<'_> = bp.get_page_for_read(*key).unwrap();
                assert_eq!(guard[0], i as u8);
            }
        }
    }
}
