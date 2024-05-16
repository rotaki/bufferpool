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
    collections::{HashMap, HashSet, VecDeque},
    ops::{Deref, DerefMut},
    path::PathBuf,
    result,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

const NUM_FRAMES_LARGE_THRESHOLD: usize = 1000;
const EVICTION_SCAN_DEPTH: usize = 10;

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

pub struct Frames<T: EvictionPolicy> {
    head: Mutex<usize>, // Head of the circular buffer. The head contains the most recently used frame. The next frame contains the second most recently used frame, and so on. The previous frame contains the least recently used frame.
    num_frames: usize,
    frames: Vec<BufferFrame<T>>, // The Vec<frames> is fixed size. If not fixed size, then Pin must be used to ensure that the frame does not move when the vector is resized.
}

impl<T: EvictionPolicy> Frames<T> {
    pub fn new(num_frames: usize) -> Self {
        let frames: Vec<BufferFrame<T>> = (0..num_frames).map(|_| BufferFrame::default()).collect();
        for i in 0..num_frames {
            if i == 0 {
                frames[i].set_next(1);
                frames[i].set_prev(num_frames - 1);
            } else if i == num_frames - 1 {
                frames[i].set_next(0);
                frames[i].set_prev(num_frames - 2);
            } else {
                frames[i].set_next(i + 1);
                frames[i].set_prev(i - 1);
            }
        }
        Frames {
            head: Mutex::new(0),
            num_frames,
            frames,
        }
    }

    /// Move the frame-index to the head of the doubly linked list
    /// This function gets called by multiple reader threads when the shared latch to the buffer pool is held.
    /// It assures thread-safety by acquiring the lock on the head.
    pub fn move_to_head(&self, index: usize) {
        let mut current_head = self.head.lock().unwrap();
        let current_tail = self.frames[*current_head].get_prev();
        if index == *current_head {
            return;
        } else if index == current_tail {
            *current_head = index;
            return;
        } else {
            let frame = &self.frames[index];
            // First, remove the frame from the doubly linked list
            let frame_prev_id = frame.get_prev();
            let frame_next_id = frame.get_next();
            self.frames[frame_prev_id].set_next(frame_next_id);
            self.frames[frame_next_id].set_prev(frame_prev_id);

            // Then, insert the frame to the head
            frame.set_prev(current_tail);
            frame.set_next(*current_head);
            self.frames[current_tail].set_next(index);
            self.frames[*current_head].set_prev(index);
            *current_head = index;
        }
    }

    /// Choose a victim frame to be evicted.
    /// Return the index of the frame and whether the frame is dirty (requires writing back to disk).
    /// If all the frames are locked, then return None.
    /// This function gets called by a **single-thread** when the exclusive latch to the buffer pool is held.
    pub fn choose_victim(&self) -> Option<(usize, bool)> {
        // Find a unlatched frame from the head
        let mut checked = *self.head.lock().unwrap();
        let mut count = 0;
        let mut dirty_victim = None;
        loop {
            if count == EVICTION_SCAN_DEPTH {
                if let Some(idx) = dirty_victim {
                    return Some((idx, true));
                } else {
                    return None;
                }
            }
            count += 1;
            let current_idx = self.frames[checked].get_prev();
            if self.frames[current_idx].is_locked() {
                checked = current_idx;
                continue;
            } else if self.frames[current_idx].is_dirty() {
                if dirty_victim.is_none() {
                    dirty_victim = Some(current_idx);
                }
                checked = current_idx;
                continue;
            } else {
                return Some((current_idx, false));
            }
        }
    }

    #[cfg(test)]
    pub fn print_doubly_linked_list(&self) -> String {
        let mut current = *self.head.lock().unwrap();
        let mut count = 0;
        let mut res = String::new();
        res.push_str("[");
        loop {
            res.push_str(&format!("{}->", current));
            count += 1;
            current = self.frames[current].get_next();
            if current == *self.head.lock().unwrap() {
                res.push_str(&format!("{}], count: {}", current, count));
                break;
            }
        }
        res
    }

    /*
    fn choose_victim_num_frames_large(&mut self) -> Option<(usize, bool)> {
        if !self.initial_free_frames.is_empty() {
            #[cfg(feature = "stat")]
            inc_local_bp_free_victim();
            let idx = self.initial_free_frames.pop_front().unwrap();
            Some((idx, false))
        } else {
            // Scan the frames to find the victim
            let mut clean_frame_with_min_score = (0, u64::MAX); // (index, score)
            let mut drity_frame_with_min_score = (0, u64::MAX); // (index, score)

            // randomly select EVICTION_SCAN_DEPTH frames to scan
            for i in 0..EVICTION_SCAN_DEPTH {
                let rand_idx = gen_random_int(0, self.num_frames - 1);
                self.eviction_candidates[i] = rand_idx;
            }
            for i in self.eviction_candidates.iter() {
                let frame = &self.frames[*i];
                if frame.is_locked() {
                    continue;
                }
                let score = frame.eviction_score();
                if !frame.is_dirty() {
                    if score < clean_frame_with_min_score.1 {
                        clean_frame_with_min_score = (*i, score);
                    }
                } else {
                    if score < drity_frame_with_min_score.1 {
                        drity_frame_with_min_score = (*i, score);
                    }
                }
            }
            if clean_frame_with_min_score.1 != u64::MAX {
                #[cfg(feature = "stat")]
                inc_local_bp_clean_victim();
                // We can evict a clean frame
                Some((clean_frame_with_min_score.0, false))
            } else if drity_frame_with_min_score.1 != u64::MAX {
                #[cfg(feature = "stat")]
                inc_local_bp_dirty_victim();
                // We need to evict a dirty frame because we have no clean frames
                Some((drity_frame_with_min_score.0, true))
            } else {
                #[cfg(feature = "stat")]
                inc_local_bp_all_latched_victim();
                // All the frames are locked
                None
            }
        }
    }

    fn choose_victim_num_frames_small(&mut self) -> Option<(usize, bool)> {
        if !self.initial_free_frames.is_empty() {
            #[cfg(feature = "stat")]
            inc_local_bp_free_victim();
            let idx = self.initial_free_frames.pop_front().unwrap();
            Some((idx, false))
        } else {
            // Scan the frames to find the victim
            let mut clean_frame_with_min_score = (0, u64::MAX); // (index, score)
            let mut drity_frame_with_min_score = (0, u64::MAX); // (index, score)
            for (i, frame) in self.frames.iter().enumerate() {
                if frame.is_locked() {
                    continue;
                }
                let score = frame.eviction_score();
                if !frame.is_dirty() {
                    if score < clean_frame_with_min_score.1 {
                        clean_frame_with_min_score = (i, score);
                    }
                } else {
                    if score < drity_frame_with_min_score.1 {
                        drity_frame_with_min_score = (i, score);
                    }
                }
            }
            if clean_frame_with_min_score.1 != u64::MAX {
                #[cfg(feature = "stat")]
                inc_local_bp_clean_victim();
                // We can evict a clean frame
                Some((clean_frame_with_min_score.0, false))
            } else if drity_frame_with_min_score.1 != u64::MAX {
                #[cfg(feature = "stat")]
                inc_local_bp_dirty_victim();
                // We need to evict a dirty frame because we have no clean frames
                Some((drity_frame_with_min_score.0, true))
            } else {
                #[cfg(feature = "stat")]
                inc_local_bp_all_latched_victim();
                // All the frames are locked
                None
            }
        }
    }
    */
}

impl<T: EvictionPolicy> Deref for Frames<T> {
    type Target = Vec<BufferFrame<T>>;

    fn deref(&self) -> &Self::Target {
        &self.frames
    }
}

/// Read-after-write buffer pool.
pub struct RAWBufferPool<T: EvictionPolicy> {
    path: PathBuf,
    latch: RwLatch,
    frames: UnsafeCell<Frames<T>>,
    id_to_index: UnsafeCell<HashMap<PageKey, usize>>, // (c_id, page_id) -> index
    container_to_file: UnsafeCell<HashMap<ContainerKey, FileManager>>,
}

impl<T> RAWBufferPool<T>
where
    T: EvictionPolicy,
{
    pub fn new<P: AsRef<std::path::Path>>(
        path: P,
        num_frames: usize,
    ) -> Result<Self, MemPoolStatus> {
        log_debug!("Buffer pool created: num_frames: {}", num_frames);

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

        Ok(RAWBufferPool {
            path: path.as_ref().to_path_buf(),
            latch: RwLatch::default(),
            id_to_index: UnsafeCell::new(HashMap::new()),
            frames: UnsafeCell::new(Frames::new(num_frames)),
            container_to_file: UnsafeCell::new(container),
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

    #[cfg(test)]
    pub fn choose_victim(&self) -> Option<(usize, bool)> {
        let frames = unsafe { &mut *self.frames.get() };
        frames.choose_victim()
    }

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

    // Invariant: The exclusive latch must be held when calling this function
    fn handle_page_fault(
        &self,
        key: PageKey,
        new_page: bool,
    ) -> Result<FrameWriteGuard<T>, MemPoolStatus> {
        #[cfg(feature = "stat")]
        inc_local_bp_faults();

        let frames = unsafe { &mut *self.frames.get() };
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        let (index, is_dirty) = if let Some((index, is_dirty)) = frames.choose_victim() {
            (index, is_dirty)
        } else {
            return Err(MemPoolStatus::CannotEvictPage);
        };
        let mut guard = frames[index].try_write(false).unwrap(); // We have already checked that the frame is not locked

        // Evict old page if necessary
        if let Some(old_key) = guard.key() {
            if is_dirty {
                guard.dirty().store(false, Ordering::Relaxed);
                let file = container_to_file
                    .get(&old_key.c_key)
                    .ok_or(MemPoolStatus::FileManagerNotFound)?;
                file.write_page(old_key.page_id, &*guard)?;
            }
            id_to_index.remove(&old_key);
            log_debug!("Page evicted: {}", old_key);
        }

        // Create a new page or read from disk
        if new_page {
            guard.set_id(key.page_id);
        } else {
            let file = container_to_file
                .get(&key.c_key)
                .ok_or(MemPoolStatus::FileManagerNotFound)?;
            file.read_page(key.page_id, &mut *guard)?;
        };

        id_to_index.insert(key, index);
        *guard.key() = Some(key);

        log_debug!("Page loaded: key: {}", key);
        Ok(guard)
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
        self.exclusive();

        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        let fm = container_to_file.entry(c_key).or_insert_with(|| {
            FileManager::new(
                &self
                    .path
                    .join(c_key.db_id.to_string())
                    .join(c_key.c_id.to_string()),
            )
            .unwrap()
        });

        let page_id = fm.fetch_add_page_id();
        let key = PageKey::new(c_key, page_id);
        let res: Result<FrameWriteGuard<T>, MemPoolStatus> = self.handle_page_fault(key, true);
        if let Ok(ref guard) = res {
            guard.dirty().store(true, Ordering::Release);
        } else {
            fm.fetch_sub_page_id();
        }

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
                    frames.move_to_head(index);
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
                    frames.move_to_head(index);
                }
                guard.ok_or(MemPoolStatus::FrameWriteLatchGrantFailed)
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
                    frames.move_to_head(index);
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
                    frames.move_to_head(index);
                }
                guard.ok_or(MemPoolStatus::FrameReadLatchGrantFailed)
            }
            None => self
                .handle_page_fault(key, false)
                .map(|guard| guard.downgrade()),
        };
        self.release_exclusive();
        result
    }

    pub fn flush_all(&self) -> Result<(), MemPoolStatus> {
        self.shared();

        let frames = unsafe { &*self.frames.get() };
        let container_to_file = unsafe { &mut *self.container_to_file.get() };

        for frame in frames.iter() {
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
                if let Some(file) = container_to_file.get(&key.c_key) {
                    file.write_page(key.page_id, &*frame)?;
                } else {
                    self.release_shared();
                    return Err(MemPoolStatus::FileManagerNotFound);
                }
            }
        }

        self.release_shared();
        Ok(())
    }

    /// Reset the buffer pool to its initial state.
    /// This will not flush the dirty pages to disk.
    /// This also removes all the files in disk.
    pub fn reset(&self) {
        self.exclusive();

        unsafe { &mut *self.id_to_index.get() }.clear();
        unsafe { &mut *self.container_to_file.get() }.clear();

        let frames = unsafe { &mut *self.frames.get() };

        for frame in frames.iter() {
            let frame = loop {
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

        self.release_exclusive();
    }
}

impl<T> MemPool<T> for RAWBufferPool<T>
where
    T: EvictionPolicy,
{
    fn create_new_page_for_write(
        &self,
        c_key: ContainerKey,
    ) -> Result<FrameWriteGuard<T>, MemPoolStatus> {
        RAWBufferPool::create_new_page_for_write(self, c_key)
    }

    fn get_page_for_write(&self, key: PageKey) -> Result<FrameWriteGuard<T>, MemPoolStatus> {
        RAWBufferPool::get_page_for_write(self, key)
    }

    fn get_page_for_read(&self, key: PageKey) -> Result<FrameReadGuard<T>, MemPoolStatus> {
        RAWBufferPool::get_page_for_read(self, key)
    }

    fn reset(&self) {
        RAWBufferPool::reset(self);
    }

    #[cfg(test)]
    fn run_checks(&self) {
        RAWBufferPool::run_checks(self);
    }
}

#[cfg(test)]
impl<T: EvictionPolicy> RAWBufferPool<T> {
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

unsafe impl<T: EvictionPolicy> Sync for RAWBufferPool<T> {}

#[cfg(test)]
mod tests {
    use crate::buffer_pool::eviction_policy::LRUEvictionPolicy;
    use crate::{log, log_trace};

    use super::*;
    use std::thread;
    use tempfile::TempDir;

    pub type TestRAWBufferPool = RAWBufferPool<LRUEvictionPolicy>;

    #[test]
    fn test_bp_and_frame_latch() {
        let temp_dir = TempDir::new().unwrap();
        let db_id = 0;
        // create a directory for the database
        std::fs::create_dir(temp_dir.path().join(db_id.to_string())).unwrap();
        {
            let num_frames = 10;
            let bp = TestRAWBufferPool::new(temp_dir.path(), num_frames).unwrap();
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
            let bp = TestRAWBufferPool::new(temp_dir.path(), num_frames).unwrap();
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
            let bp = TestRAWBufferPool::new(temp_dir.path(), num_frames).unwrap();
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
        let bp = TestRAWBufferPool::new(temp_dir.path(), num_pages).unwrap();
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
        let bp = TestRAWBufferPool::new(temp_dir.path(), num_pages).unwrap();
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
        let bp = TestRAWBufferPool::new(temp_dir.path(), num_pages).unwrap();
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
