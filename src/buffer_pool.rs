use crate::{
    buffer_frame::{BufferFrame, FrameReadGuard, FrameWriteGuard},
    file_manager::FileManager,
};
use std::{
    cell::UnsafeCell,
    collections::hash_map::Entry,
    collections::HashMap,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

pub const NUM_PAGES: usize = 1 << 16;

pub struct BufferPool {
    path: PathBuf,
    latch: AtomicBool,
    pages: UnsafeCell<Vec<BufferFrame>>,
    id_to_index: UnsafeCell<HashMap<(usize, usize), usize>>, // (container_id, page_id) -> index
    container_to_file: UnsafeCell<HashMap<usize, FileManager>>,
}

impl BufferPool {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Self {
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

        let pages = (0..NUM_PAGES).map(|_| BufferFrame::default()).collect();
        BufferPool {
            path: path.as_ref().to_path_buf(),
            latch: AtomicBool::new(false),
            id_to_index: UnsafeCell::new(HashMap::new()),
            pages: UnsafeCell::new(pages),
            container_to_file: UnsafeCell::new(container),
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

    pub fn create_new_page(&self, container_id: usize) -> usize {
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        self.latch();

        match container_to_file.entry(container_id) {
            Entry::Occupied(mut entry) => {
                let file_manager = entry.get_mut();
                self.unlatch();

                let page_id = file_manager.new_page();
                page_id
            }
            Entry::Vacant(entry) => {
                let file_manager = FileManager::new(self.path.join(container_id.to_string()));
                let file_manager = entry.insert(file_manager);
                self.unlatch();

                let page_id = file_manager.new_page();
                page_id
            }
        }
    }

    pub fn get_page_for_write(
        &self,
        container_id: usize,
        page_id: usize,
    ) -> Option<FrameWriteGuard> {
        // Modification to the following data structures must be done while holding the latch
        // on the buffer pool.
        let pages = unsafe { &mut *self.pages.get() };
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        self.latch();

        // Check if the page already exists
        if let Some(index) = id_to_index.get(&(container_id, page_id)).copied() {
            let res = pages[index].try_write();
            self.unlatch();
            res
        } else {
            // Choose a page to evict
            let (old_id, index) = ((0, 0), 0); // TODO: implement a replacement policy. Returns ((container_id, page_id), frame_index)
            if let Some(mut guard) = pages[index].try_write() {
                // Always get the frame latch before modifying the id_to_index map
                // Modify the id_to_index map
                id_to_index.remove(&old_id);
                id_to_index.insert((container_id, page_id), index);
                let file = container_to_file
                    .get(&container_id)
                    .expect("file not found");
                self.unlatch();

                let page = file.read_page(page_id);
                guard.copy(&page);
                Some(guard)
            } else {
                self.unlatch();
                None
            }
        }
    }

    pub fn get_page_for_read(&self, container_id: usize, page_id: usize) -> Option<FrameReadGuard> {
        // Modification to the following data structures must be done while holding the latch
        // on the buffer pool.

        let pages = unsafe { &mut *self.pages.get() };
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        self.latch();

        // Check if the page already exists
        if let Some(index) = id_to_index.get(&(container_id, page_id)).copied() {
            let res = pages[index].try_read();
            self.unlatch();
            res
        } else {
            // Choose a page to evict
            let (old_id, index) = ((0, 0), 0); // TODO: implement a replacement policy. Returns (page_id, frame_index)
            if let Some(mut guard) = pages[index].try_write() {
                // Always get the frame latch before modifying the id_to_index map
                // Modify the id_to_index map
                id_to_index.remove(&old_id);
                id_to_index.insert((container_id, page_id), index);
                let file = container_to_file
                    .get(&container_id)
                    .expect("file not found");
                self.unlatch();

                let page = file.read_page(page_id);
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
    use tempfile::TempDir;

    #[test]
    fn test_bp_and_frame_latch() {
        let temp_dir = TempDir::new().unwrap();
        {
            // Create a new file in the tempdir with name 0
            let file_manager = FileManager::new(temp_dir.path().join("0"));
            assert_eq!(file_manager.new_page(), 0);
        }
        {
            let bp = BufferPool::new(temp_dir.path());
            let page_id = bp.create_new_page(0);
            let num_threads = 3;
            let num_iterations = 80; // Note: u8 max value is 255
            thread::scope(|s| {
                for _ in 0..num_threads {
                    s.spawn(|| {
                        for _ in 0..num_iterations {
                            loop {
                                if let Some(mut guard) = bp.get_page_for_write(0, page_id) {
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

            let guard = bp.get_page_for_read(0, page_id).unwrap();
            assert_eq!(guard[0], num_threads * num_iterations);
        }
    }
}
