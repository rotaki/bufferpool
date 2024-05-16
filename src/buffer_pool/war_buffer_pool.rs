/*
use lazy_static::lazy_static;
use tempfile::TempDir;

use crate::{log, log_debug, random::gen_random_int, rwlatch::RwLatch};

use super::{
    buffer_frame::{BufferFrame, FrameReadGuard, FrameWriteGuard},
    eviction_policy::EvictionPolicy,
    mem_pool_trait::{ContainerKey, MemPool, MemPoolStatus, PageKey},
};

use crate::{
    file_manager::{FMStatus, FileManager},
    page::{Page, PageId},
};

use std::{
    cell::UnsafeCell,
    collections::{
        btree_map::{BTreeMap, Entry},
        BTreeSet, HashMap, HashSet, VecDeque,
    },
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    result,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
};

const EVICTION_SCAN_DEPTH: usize = 10;
const EVICTION_AGGRESSIVENESS: f64 = 0.5; // Move 10% of the EVICTION_SCAN_DEPTH frames to the write_buffer
const EVICTION_COUNT: usize = (EVICTION_SCAN_DEPTH as f64 * EVICTION_AGGRESSIVENESS) as usize;
const WRITER_WAIT_TIME: u64 = 1; // 1ms
const NUM_WRITE_BUFFERS: usize = 100;

#[cfg(feature = "stat")]
/// Statistics #pages evicted from the buffer pool.
struct BPStats {
    hit_rate: UnsafeCell<[usize; 2]>, // [faults, total]
    victim_status: UnsafeCell<[usize; 4]>,
}

#[cfg(feature = "stat")]
impl BPStats {
    fn new() -> Self {
        BPStats {
            hit_rate: UnsafeCell::new([0, 0]),            // [faults, total]
            victim_status: UnsafeCell::new([0, 0, 0, 0]), // [free, clean, dirty, all_latched]
        }
    }

    fn to_string(&self) -> String {
        let hit_rate = unsafe { &*self.hit_rate.get() };
        let victim_status = unsafe { &*self.victim_status.get() };
        let mut result = String::new();
        result.push_str("Buffer Pool Statistics\n");
        result.push_str("Hit Rate\n");
        result.push_str(&format!(
            "Hits  :  {:8} ({:.2}%)\n",
            hit_rate[1] - hit_rate[0],
            (hit_rate[1] - hit_rate[0]) as f64 / hit_rate[1] as f64 * 100.0
        ));
        result.push_str(&format!(
            "Faults:  {:8} ({:.2}%)\n",
            hit_rate[0],
            hit_rate[0] as f64 / hit_rate[1] as f64 * 100.0
        ));
        result.push_str(&format!("Total :  {:8} ({:.2}%)\n", hit_rate[1], 100.0));
        result.push_str("\n");
        result.push_str("Eviction Statistics\n");
        let op_types = ["Free", "Clean", "Dirty", "All Latched", "Total"];
        let total = victim_status.iter().sum::<usize>();
        for i in 0..4 {
            result.push_str(&format!(
                "{:12}: {:8} ({:.2}%)\n",
                op_types[i],
                victim_status[i],
                (victim_status[i] as f64 / total as f64) * 100.0
            ));
        }
        result.push_str(&format!(
            "{:12}: {:8} ({:.2}%)\n",
            op_types[4], total, 100.0
        ));
        result
    }

    fn merge(&self, other: &BPStats) {
        let hit_rate = unsafe { &mut *self.hit_rate.get() };
        let other_hit_rate = unsafe { &*other.hit_rate.get() };
        for i in 0..2 {
            hit_rate[i] += other_hit_rate[i];
        }
        let victim_status = unsafe { &mut *self.victim_status.get() };
        let other_victim_status = unsafe { &*other.victim_status.get() };
        for i in 0..4 {
            victim_status[i] += other_victim_status[i];
        }
    }

    fn clear(&self) {
        let hit_rate = unsafe { &mut *self.hit_rate.get() };
        for i in 0..2 {
            hit_rate[i] = 0;
        }
        let victim_status = unsafe { &mut *self.victim_status.get() };
        for i in 0..4 {
            victim_status[i] = 0;
        }
    }
}

#[cfg(feature = "stat")]
struct LocalBPStat {
    pub stat: BPStats,
}

#[cfg(feature = "stat")]
impl Drop for LocalBPStat {
    fn drop(&mut self) {
        GLOBAL_BP_STAT.lock().unwrap().merge(&self.stat);
    }
}

#[cfg(feature = "stat")]
lazy_static! {
    static ref GLOBAL_BP_STAT: Mutex<BPStats> = Mutex::new(BPStats::new());
}

#[cfg(feature = "stat")]
thread_local! {
    static LOCAL_BP_STAT: LocalBPStat = LocalBPStat {
        stat: BPStats::new(),
    };
}

#[cfg(feature = "stat")]
fn inc_local_bp_lookup() {
    LOCAL_BP_STAT.with(|stat| {
        let hit_rate = unsafe { &mut *stat.stat.hit_rate.get() };
        hit_rate[1] += 1;
    });
}

#[cfg(feature = "stat")]
fn inc_local_bp_faults() {
    LOCAL_BP_STAT.with(|stat| {
        let hit_rate = unsafe { &mut *stat.stat.hit_rate.get() };
        hit_rate[0] += 1;
    });
}

#[cfg(feature = "stat")]
fn inc_local_bp_free_victim() {
    LOCAL_BP_STAT.with(|stat| {
        let victim_status = unsafe { &mut *stat.stat.victim_status.get() };
        victim_status[0] += 1;
    });
}

#[cfg(feature = "stat")]
fn inc_local_bp_clean_victim() {
    LOCAL_BP_STAT.with(|stat| {
        let victim_status = unsafe { &mut *stat.stat.victim_status.get() };
        victim_status[1] += 1;
    });
}

#[cfg(feature = "stat")]
fn inc_local_bp_dirty_victim() {
    LOCAL_BP_STAT.with(|stat| {
        let victim_status = unsafe { &mut *stat.stat.victim_status.get() };
        victim_status[2] += 1;
    });
}

#[cfg(feature = "stat")]
fn inc_local_bp_all_latched_victim() {
    LOCAL_BP_STAT.with(|stat| {
        let victim_status = unsafe { &mut *stat.stat.victim_status.get() };
        victim_status[3] += 1;
    });
}

struct WriteBuffer {
    path: PathBuf,
    empty_map: BTreeSet<usize>, // Stores the empty slots in the buffer.
    id_to_index: BTreeMap<PageKey, usize>, // Variable size hashmap. Stores the mapping from (c_id, page_id) to the index of the buffer.
    buffers: Vec<Page>, // Fixed size (num_pages). Stores the dirty pages to be written to disk.
    container_to_file: HashMap<ContainerKey, FileManager>,
}

impl WriteBuffer {
    pub fn new<P: AsRef<Path>>(path: P, num_pages: usize) -> Result<Self, MemPoolStatus> {
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
        Ok(WriteBuffer {
            path: path.as_ref().to_path_buf(),
            empty_map: (0..num_pages).collect(),
            id_to_index: BTreeMap::new(),
            buffers: (0..num_pages).map(|_| Page::new_empty()).collect(),
            container_to_file: container,
        })
    }

    pub fn next_page_id(&mut self, c_key: ContainerKey) -> PageId {
        let file = self.container_to_file.entry(c_key).or_insert_with(|| {
            FileManager::new(
                &self
                    .path
                    .join(c_key.db_id.to_string())
                    .join(c_key.c_id.to_string()),
            )
            .unwrap()
        });
        file.fetch_add_page_id()
    }

    /// Returns true if the page is read from the file.
    /// If the page is read from the write buffer, then returns false.
    /// This is necessary to determine if the page is dirty or not.
    pub fn read_page(&mut self, key: PageKey, page: &mut Page) -> Result<bool, MemPoolStatus> {
        // If there is a page in the buffer, then copy the page to the input page and remove the page from the buffer.
        // Otherwise, read the page from the file and copy it to the input page.
        if let Some(index) = self.id_to_index.remove(&key) {
            log_debug!("Reading page from write buffer: {}", key);
            assert!(self.empty_map.insert(index));
            page.copy(&self.buffers[index]);
            Ok(false)
        } else {
            log_debug!("Reading page from file: {}", key);
            let file = self
                .container_to_file
                .get(&key.c_key)
                .ok_or(MemPoolStatus::FileManagerNotFound)?;
            file.read_page(key.page_id, page)?;
            Ok(true)
        }
    }

    pub fn write_page(&mut self, key: PageKey, page: &Page) -> Result<(), MemPoolStatus> {
        log_debug!("Inserting page to write buffer: {}", key);
        let index = match self.id_to_index.entry(key) {
            Entry::Occupied(index) => {
                println!("Duplicate entry: {} at index: {}", key, index.get());
                panic!("There should not be any duplicate entries in the write buffer")
            }
            Entry::Vacant(e) => {
                log_debug!("Moving page to write buffer: {}", key);
                let index = self.empty_map.pop_first();
                if let Some(index) = index {
                    e.insert(index);
                    Ok(index)
                } else {
                    Err(MemPoolStatus::WriteBufferFull)
                }
            }
        };
        if let Ok(index) = index {
            self.buffers[index].copy(page);
        }
        index.map(|_| ())
    }

    // Asynchronously write all the pages in the buffer to the file.
    pub fn flush_pages(&mut self) -> Result<(), MemPoolStatus> {
        for (key, index) in self.id_to_index.iter() {
            let file = self
                .container_to_file
                .get(&key.c_key)
                .ok_or(MemPoolStatus::FileManagerNotFound)?;
            log_debug!("Flushing page: {} at index: {}", key, index);
            file.write_page(key.page_id, &self.buffers[*index])?;
            self.empty_map.insert(*index);
        }
        self.clear();
        Ok(())
    }

    fn clear(&mut self) {
        self.id_to_index.clear();
    }

    fn reset(&mut self) {
        self.clear();
        self.container_to_file.clear();
        for entry in std::fs::read_dir(&self.path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            std::fs::remove_file(path).unwrap();
        }
    }
}

pub struct Frames<T: EvictionPolicy> {
    num_frames: usize,
    eviction_candidates: [(u64, usize); EVICTION_SCAN_DEPTH],
    free_frames: VecDeque<usize>, // Used for initial free frames only
    frames: Vec<BufferFrame<T>>, // The Vec<frames> is fixed size. If not fixed size, then Box must be used to ensure that the frame does not move when the vector is resized.
    write_buffer: Arc<RwLock<WriteBuffer>>,
    writer_thread: Option<(Arc<AtomicBool>, std::thread::JoinHandle<()>)>,
}

impl<T: EvictionPolicy> Frames<T> {
    pub fn new<P: AsRef<Path>>(path: P, num_frames: usize) -> Result<Self, MemPoolStatus> {
        let write_buffer = Arc::new(RwLock::new(WriteBuffer::new(path, NUM_WRITE_BUFFERS)?));
        Ok(Frames {
            num_frames,
            eviction_candidates: [(0, 0); EVICTION_SCAN_DEPTH],
            free_frames: (0..num_frames).collect(),
            frames: (0..num_frames).map(|_| BufferFrame::default()).collect(),
            write_buffer,
            writer_thread: None,
        })
    }

    pub fn get(&self, index: usize) -> &BufferFrame<T> {
        &self.frames[index]
    }

    pub fn get_mut(&mut self, index: usize) -> &mut BufferFrame<T> {
        &mut self.frames[index]
    }

    /// Find a free frame in the buffer pool.
    /// If free frame is not found, then evict a page from the buffer pool.
    /// The evicted page will be written to the write_buffer.
    /// If the write_buffer is full, then return
    fn alloc_frame(
        &mut self,
        id_to_index: &mut HashMap<PageKey, usize>,
    ) -> Result<usize, MemPoolStatus> {
        // 1. We first find a free frame.
        // print frame keys
        let idx = if !self.free_frames.is_empty() {
            #[cfg(feature = "stat")]
            inc_local_bp_free_victim();
            log_debug!("Free frame found");
            let idx = self.free_frames.pop_front().unwrap();
            idx
        } else {
            // Scan the frames to find the victim
            if EVICTION_SCAN_DEPTH > self.num_frames {
                for (_, idx) in id_to_index.iter() {
                    let frame = &self.frames[*idx];
                    self.eviction_candidates[*idx] = if frame.is_locked() {
                        (u64::MAX, *idx)
                    } else {
                        (frame.eviction_score(), *idx)
                    }
                }
                for i in id_to_index.len()..EVICTION_SCAN_DEPTH {
                    self.eviction_candidates[i] = (u64::MAX, 0);
                }
            } else {
                // Randomly select EVICTION_SCAN_DEPTH frames to scan
                for (i, (_, idx)) in id_to_index.iter().enumerate() {
                    if i >= EVICTION_SCAN_DEPTH {
                        break;
                    }
                    let frame = &self.frames[*idx];
                    self.eviction_candidates[i] = if frame.is_locked() {
                        (u64::MAX, *idx)
                    } else {
                        (frame.eviction_score(), *idx)
                    }
                }
            }
            // Sort the frames based on the score
            self.eviction_candidates.sort_by_key(|(score, _)| *score);

            if self.eviction_candidates[0].0 == u64::MAX {
                #[cfg(feature = "stat")]
                inc_local_bp_all_latched_victim();
                return Err(MemPoolStatus::CannotEvictPage);
            }

            // Move the EVICTION_COUNT frames to the write_buffer if the frame is dirty
            for i in 0..EVICTION_COUNT {
                let (score, idx) = self.eviction_candidates[i];
                if score == u64::MAX {
                    // These are the latched frames
                    break;
                }
                // For all un-latched frames with the score in the top EVICTION_COUNT
                // move the idx to the free_frames list and move the page to the write_buffer if it is dirty
                let guard = self.frames[idx].write(false);
                id_to_index.remove(&guard.key().unwrap());
                guard.evict_info().write().unwrap().reset();
                if i != 0 {
                    // First frame will be skipped because it will be used as the victim
                    self.free_frames.push_back(idx);
                }
                if guard.dirty().load(Ordering::Relaxed) {
                    let key = guard.key().unwrap();
                    loop {
                        let mut write_buffer = self.write_buffer.write().unwrap();
                        match write_buffer.write_page(key, &*guard) {
                            Ok(_) => break,
                            Err(MemPoolStatus::WriteBufferFull) => {
                                log_debug!(
                                    ">>> Write buffer is full. Sleeping for {}ms <<< ",
                                    WRITER_WAIT_TIME
                                );
                                drop(write_buffer);
                                std::thread::sleep(std::time::Duration::from_millis(
                                    WRITER_WAIT_TIME,
                                ));
                            }
                            Err(_) => {
                                panic!("Unexpected error");
                            }
                        }
                    }
                    guard.dirty().store(false, Ordering::Relaxed);
                    *guard.key() = None;
                }
            }

            self.eviction_candidates[0].1
        };
        Ok(idx)
    }

    pub fn create_new_page(
        &mut self,
        c_key: ContainerKey,
        id_to_index: &mut HashMap<PageKey, usize>,
    ) -> Result<usize, MemPoolStatus> {
        let idx = self.alloc_frame(id_to_index)?;
        let page_key = self.write_buffer.write().unwrap().next_page_id(c_key);
        log_debug!("Creating new page: {}", page_key);
        id_to_index.insert(PageKey::new(c_key, page_key), idx);
        let mut guard = self.frames[idx].try_write(true).unwrap();
        guard.set_id(page_key);
        *guard.key() = Some(PageKey::new(c_key, page_key));
        {
            let mut evict_info = guard.evict_info().write().unwrap();
            evict_info.reset();
            evict_info.update();
        }
        Ok(idx)
    }

    pub fn get_page(
        &mut self,
        page_key: PageKey,
        id_to_index: &mut HashMap<PageKey, usize>,
    ) -> Result<usize, MemPoolStatus> {
        #[cfg(feature = "stat")]
        inc_local_bp_lookup();
        let idx = self.alloc_frame(id_to_index)?;
        id_to_index.insert(page_key, idx);
        let mut guard = self.frames[idx].write(false);
        if self
            .write_buffer
            .write()
            .unwrap()
            .read_page(page_key, &mut *guard)
            .unwrap()
        {
            // Page is read from the file
            guard.dirty().store(false, Ordering::Relaxed);
        } else {
            // Page is read from the write buffer
            guard.dirty().store(true, Ordering::Relaxed);
        }
        *guard.key() = Some(page_key);
        {
            let mut evict_info = guard.evict_info().write().unwrap();
            evict_info.reset();
            evict_info.update();
        }
        Ok(idx)
    }

    pub fn reset(&mut self) {
        for frame in self.frames.iter_mut() {
            let frame = loop {
                if let Some(guard) = frame.try_write(false) {
                    break guard;
                }
                // spin
                std::hint::spin_loop();
            };
            frame.clear();
        }
        self.free_frames = (0..self.num_frames).collect();
        self.write_buffer.write().unwrap().reset();
    }

    pub fn start_writer_thread(&mut self) {
        log_debug!("Writer thread started");
        let flag = Arc::new(AtomicBool::new(true));
        let flag_clone = flag.clone();
        let write_buffer_clone = self.write_buffer.clone();
        let writer_thread = std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_millis(WRITER_WAIT_TIME));
            log_debug!("Writer thread woke up");
            if !flag_clone.load(Ordering::Relaxed) {
                log_debug!("Writer thread stopped");
                break;
            }
            let mut write_buffer = write_buffer_clone.write().unwrap();
            if write_buffer.id_to_index.is_empty() {
                log_debug!("Write buffer is empty");
                continue;
            }
            write_buffer.flush_pages().unwrap();
        });
        self.writer_thread = Some((flag, writer_thread));
    }

    pub fn stop_writer_thread(&mut self) {
        if let Some((flag, writer_thread)) = self.writer_thread.take() {
            flag.store(false, Ordering::Relaxed);
            writer_thread.join().unwrap();
        }
    }
}

impl<T: EvictionPolicy> Deref for Frames<T> {
    type Target = Vec<BufferFrame<T>>;

    fn deref(&self) -> &Self::Target {
        &self.frames
    }
}

impl<T: EvictionPolicy> DerefMut for Frames<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.frames
    }
}

impl<T: EvictionPolicy> Drop for Frames<T> {
    fn drop(&mut self) {
        self.stop_writer_thread();
    }
}

/// Write-after-read buffer pool.
pub struct WARBufferPool<T: EvictionPolicy> {
    latch: RwLatch,
    frames: UnsafeCell<Frames<T>>,
    id_to_index: UnsafeCell<HashMap<PageKey, usize>>, // (c_id, page_id) -> index
}

impl<T> WARBufferPool<T>
where
    T: EvictionPolicy,
{
    pub fn new<P: AsRef<std::path::Path>>(
        path: P,
        num_frames: usize,
    ) -> Result<Self, MemPoolStatus> {
        log_debug!("Buffer pool created: num_frames: {}", num_frames);

        let mut frames = Frames::new(path, num_frames)?;
        frames.start_writer_thread();

        Ok(WARBufferPool {
            latch: RwLatch::default(),
            id_to_index: UnsafeCell::new(HashMap::new()),
            frames: UnsafeCell::new(frames),
        })
    }

    pub fn eviction_stats(&self) -> String {
        #[cfg(feature = "stat")]
        {
            let stats = GLOBAL_BP_STAT.lock().unwrap();
            LOCAL_BP_STAT.with(|local_stat| {
                stats.merge(&local_stat.stat);
                local_stat.stat.clear();
            });
            stats.to_string()
        }
        #[cfg(not(feature = "stat"))]
        {
            "Stat is disabled".to_string()
        }
    }

    // #[cfg(test)]
    // pub fn choose_victim(&self) -> Option<(usize, bool)> {
    //     let frames = unsafe { &mut *self.frames.get() };
    //     frames.choose_victim(&self.write_buffer)
    // }

    fn shared(&self) {
        self.latch.shared();
    }

    fn exclusive(&self) {
        self.latch.exclusive();
    }

    fn release_shared(&self) {
        self.latch.release_shared();
    }

    fn release_exclusive(&self) {
        self.latch.release_exclusive();
    }

    /// Create a new page for write in memory.
    /// NOTE: This function does not write the page to disk.
    /// See more at `handle_page_fault(key, new_page=true)`
    /// The newly allocated page is not formatted except for the page id.
    /// The caller is responsible for initializing the page.
    pub fn create_new_page_for_write(
        &self,
        c_key: ContainerKey,
    ) -> Result<FrameWriteGuard<T>, MemPoolStatus> {
        log_debug!("Page create: {}", c_key);

        self.exclusive();

        let frames = unsafe { &mut *self.frames.get() };
        let id_to_index = unsafe { &mut *self.id_to_index.get() };

        let res = frames.create_new_page(c_key, id_to_index);

        let res = match res {
            Ok(idx) => {
                let guard = frames[idx].try_write(true).unwrap();
                Ok(guard)
            }
            Err(e) => Err(e),
        };

        self.release_exclusive();
        res
    }

    pub fn get_page_for_write(&self, key: PageKey) -> Result<FrameWriteGuard<T>, MemPoolStatus> {
        #[cfg(feature = "stat")]
        inc_local_bp_lookup();
        log_debug!("Page write: {}", key);

        {
            self.shared();
            let id_to_index = unsafe { &mut *self.id_to_index.get() };
            let frames = unsafe { &mut *self.frames.get() };

            if let Some(&index) = id_to_index.get(&key) {
                let guard = frames[index].try_write(true);
                if guard.is_some() {
                    guard
                        .as_ref()
                        .unwrap()
                        .evict_info()
                        .write()
                        .unwrap()
                        .update();
                }
                self.release_shared();
                return guard.ok_or(MemPoolStatus::FrameWriteLatchGrantFailed);
            }
            self.release_shared();
        }

        self.exclusive();

        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let frames = unsafe { &mut *self.frames.get() };

        // We need to recheck the id_to_index because another thread might have inserted the page
        let result = match id_to_index.get(&key) {
            Some(&index) => {
                let guard = frames[index].try_write(true);
                if guard.is_some() {
                    guard
                        .as_ref()
                        .unwrap()
                        .evict_info()
                        .write()
                        .unwrap()
                        .update();
                }
                guard.ok_or(MemPoolStatus::FrameWriteLatchGrantFailed)
            }
            None => {
                let idx = frames.get_page(key, id_to_index);
                // If guard is ok, mark the page as dirty
                let res = match idx {
                    Ok(idx) => {
                        let guard = frames[idx].try_write(true).unwrap();
                        guard.dirty().store(true, Ordering::Relaxed);
                        Ok(guard)
                    }
                    Err(e) => Err(e),
                };
                res
            }
        };
        self.release_exclusive();
        result
    }

    pub fn get_page_for_read(&self, key: PageKey) -> Result<FrameReadGuard<T>, MemPoolStatus> {
        #[cfg(feature = "stat")]
        inc_local_bp_lookup();
        log_debug!("Page read: {}", key);

        {
            self.shared();
            let id_to_index = unsafe { &mut *self.id_to_index.get() };
            let frames = unsafe { &mut *self.frames.get() };

            if let Some(&index) = id_to_index.get(&key) {
                let guard = frames[index].try_read();
                if guard.is_some() {
                    guard
                        .as_ref()
                        .unwrap()
                        .evict_info()
                        .write()
                        .unwrap()
                        .update();
                }
                self.release_shared();
                return guard.ok_or(MemPoolStatus::FrameReadLatchGrantFailed);
            }

            self.release_shared();
        }

        self.exclusive();

        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let frames = unsafe { &mut *self.frames.get() };

        let result = match id_to_index.get(&key) {
            Some(&index) => {
                let guard = frames[index].try_read();
                if guard.is_some() {
                    guard
                        .as_ref()
                        .unwrap()
                        .evict_info()
                        .write()
                        .unwrap()
                        .update();
                }
                guard.ok_or(MemPoolStatus::FrameReadLatchGrantFailed)
            }
            None => {
                let idx = frames.get_page(key, id_to_index);
                let res = match idx {
                    Ok(idx) => {
                        let guard = frames[idx].try_read();
                        guard.ok_or(MemPoolStatus::FrameReadLatchGrantFailed)
                    }
                    Err(e) => Err(e),
                };
                res
            }
        };
        self.release_exclusive();
        result
    }

    /// Reset the buffer pool to its initial state.
    /// This will not flush the dirty pages to disk.
    /// This also removes all the files in disk.
    pub fn reset(&self) {
        self.exclusive();

        unsafe { &mut *self.id_to_index.get() }.clear();
        unsafe { &mut *self.frames.get() }.reset();

        self.release_exclusive();
    }
}

impl<T: EvictionPolicy> Drop for WARBufferPool<T> {
    fn drop(&mut self) {
        let frames = unsafe { &mut *self.frames.get() };
        frames.stop_writer_thread();
    }
}

impl<T> MemPool<T> for WARBufferPool<T>
where
    T: EvictionPolicy,
{
    fn create_new_page_for_write(
        &self,
        c_key: ContainerKey,
    ) -> Result<FrameWriteGuard<T>, MemPoolStatus> {
        WARBufferPool::create_new_page_for_write(self, c_key)
    }

    fn get_page_for_write(&self, key: PageKey) -> Result<FrameWriteGuard<T>, MemPoolStatus> {
        WARBufferPool::get_page_for_write(self, key)
    }

    fn get_page_for_read(&self, key: PageKey) -> Result<FrameReadGuard<T>, MemPoolStatus> {
        WARBufferPool::get_page_for_read(self, key)
    }

    fn reset(&self) {
        WARBufferPool::reset(self);
    }

    #[cfg(test)]
    fn run_checks(&self) {
        WARBufferPool::run_checks(self);
    }
}

#[cfg(test)]
impl<T: EvictionPolicy> WARBufferPool<T> {
    pub fn run_checks(&self) {
        self.check_all_frames_unlatched();
        self.check_id_to_index();
        self.check_frame_id_and_page_id_match();
    }

    pub fn check_all_frames_unlatched(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            assert!(!frame.is_exclusive());
            assert!(!frame.is_shared());
        }
    }

    // Invariant: id_to_index contains all the pages in the buffer pool
    pub fn check_id_to_index(&self) {
        let id_to_index = unsafe { &*self.id_to_index.get() };
        log_debug!("id_to_index: {:?}", id_to_index);
        let mut index_to_id = HashMap::new();
        for (k, &v) in id_to_index.iter() {
            index_to_id.insert(v, k);
        }
        let frames = unsafe { &*self.frames.get() };
        for (i, frame) in frames.iter().enumerate() {
            let frame = frame.read();
            if index_to_id.contains_key(&i) {
                assert_eq!(frame.key().unwrap(), *index_to_id[&i]);
            } else {
                assert_eq!(frame.key(), &None);
            }
        }
        // println!("id_to_index: {:?}", id_to_index);
    }

    pub fn check_frame_id_and_page_id_match(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            let frame = frame.read();
            if let Some(key) = frame.key() {
                let page_id = frame.get_id();
                assert_eq!(key.page_id, page_id);
            }
        }
    }

    pub fn is_in_buffer_pool(&self, key: PageKey) -> bool {
        let id_to_index = unsafe { &*self.id_to_index.get() };
        id_to_index.contains_key(&key)
    }
}

unsafe impl<T: EvictionPolicy> Sync for WARBufferPool<T> {}

#[cfg(test)]
mod tests {
    use crate::buffer_pool::eviction_policy::LRUEvictionPolicy;
    use crate::{log, log_trace};

    use super::*;
    use std::thread;
    use tempfile::TempDir;

    pub type TestWARBufferPool = WARBufferPool<LRUEvictionPolicy>;

    #[test]
    fn test_bp_and_frame_latch() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        // create a directory for the database
        std::fs::create_dir(temp_dir.path().join(db_id.to_string())).unwrap();
        {
            let num_frames = 10;
            let bp = TestWARBufferPool::new(temp_dir.path(), num_frames).unwrap();
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
                                    log_trace!("Spin");
                                    std::hint::spin_loop();
                                }
                            }
                        }
                    });
                }
            });
            bp.run_checks();
            {
                let guard = bp.get_page_for_read(key).unwrap();
                assert_eq!(guard[0], num_threads * num_iterations);
            }
            bp.run_checks();
        }
    }

    #[test]
    fn test_bp_write_back_simple() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        // create a directory for the database
        std::fs::create_dir(temp_dir.path().join(db_id.to_string())).unwrap();
        {
            let num_frames = 1;
            let bp = TestWARBufferPool::new(temp_dir.path(), num_frames).unwrap();
            let c_key = ContainerKey::new(db_id, 0);

            let key1 = {
                let mut guard = bp.create_new_page_for_write(c_key).unwrap();
                guard[0] = 1;
                guard.key().unwrap()
            };
            let key2 = {
                let mut guard = bp.create_new_page_for_write(c_key).unwrap();
                guard[0] = 2;
                guard.key().unwrap()
            };
            thread::sleep(std::time::Duration::from_millis(10));
            bp.run_checks();
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
            bp.run_checks();
            drop(bp);
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
            let num_frames = 1;
            let bp = TestWARBufferPool::new(temp_dir.path(), num_frames).unwrap();
            let c_key = ContainerKey::new(db_id, 0);

            for i in 0..100 {
                let mut guard = bp.create_new_page_for_write(c_key).unwrap();
                guard[0] = i;
                keys.push(guard.key().unwrap());
            }
            bp.run_checks();
            for (i, key) in keys.iter().enumerate() {
                let guard = bp.get_page_for_read(*key).unwrap();
                assert_eq!(guard[0], i as u8);
            }
            bp.run_checks();
        }
    }

    #[test]
    fn test_bp_create_new_page() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        // create a directory for the database
        std::fs::create_dir(temp_dir.path().join(db_id.to_string())).unwrap();

        let num_pages = 2;
        let bp = TestWARBufferPool::new(temp_dir.path(), num_pages).unwrap();
        let c_key = ContainerKey::new(db_id, 0);

        let num_traversal = 100;

        let mut count = 0;
        let mut keys = Vec::new();

        for _ in 0..num_traversal {
            let mut guard1 = bp.create_new_page_for_write(c_key).unwrap();
            guard1[0] = count;
            count += 1;
            keys.push(guard1.key().unwrap());

            let mut guard2 = bp.create_new_page_for_write(c_key).unwrap();
            guard2[0] = count;
            count += 1;
            keys.push(guard2.key().unwrap());
        }

        bp.run_checks();

        // Traverse by 2 pages at a time
        for i in 0..num_traversal {
            let guard1 = bp.get_page_for_read(keys[i * 2]).unwrap();
            assert_eq!(guard1[0], i as u8 * 2);
            let guard2 = bp.get_page_for_read(keys[i * 2 + 1]).unwrap();
            assert_eq!(guard2[0], i as u8 * 2 + 1);
        }

        bp.run_checks();
    }

    #[test]
    fn test_bp_all_frames_latched() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        // create a directory for the database
        std::fs::create_dir(temp_dir.path().join(db_id.to_string())).unwrap();

        let num_pages = 1;
        let bp = TestWARBufferPool::new(temp_dir.path(), num_pages).unwrap();
        let c_key = ContainerKey::new(db_id, 0);

        let mut guard1 = bp.create_new_page_for_write(c_key).unwrap();
        guard1[0] = 1;

        // Try to get a new page for write. This should fail because all the frames are latched.
        let res = bp.create_new_page_for_write(c_key);
        assert_eq!(res.unwrap_err(), MemPoolStatus::CannotEvictPage);

        drop(guard1);

        // Now, we should be able to get a new page for write.
        let guard2 = bp.create_new_page_for_write(c_key).unwrap();
        drop(guard2);
    }

    #[test]
    fn test_bp_stats() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        // create a directory for the database
        std::fs::create_dir(temp_dir.path().join(db_id.to_string())).unwrap();

        let num_pages = 1;
        let bp = TestWARBufferPool::new(temp_dir.path(), num_pages).unwrap();
        let c_key = ContainerKey::new(db_id, 0);

        let key_1 = {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = 1;
            guard.key().unwrap()
        };

        let stats = bp.eviction_stats();
        println!("{}", stats);

        let key_2 = {
            let mut guard = bp.create_new_page_for_write(c_key).unwrap();
            guard[0] = 2;
            guard.key().unwrap()
        };

        let stats = bp.eviction_stats();
        println!("{}", stats);

        {
            let guard = bp.get_page_for_read(key_1).unwrap();
            assert_eq!(guard[0], 1);
        }

        let stats = bp.eviction_stats();
        println!("{}", stats);

        {
            let guard = bp.get_page_for_read(key_2).unwrap();
            assert_eq!(guard[0], 2);
        }

        let stats = bp.eviction_stats();
        println!("{}", stats);
    }
}
*/
