use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::Arc,
};

#[cfg(feature = "stat")]
use stat::*;

use crate::{bp::prelude::*, page::Page};
use crate::{log_info, page::PageId};

use super::shortkeypage::{ShortKeyPage, SHORT_KEY_PAGE_HEADER_SIZE};
use crate::page::AVAILABLE_PAGE_SIZE;

const DEFAULT_BUCKET_NUM: usize = 4096;
const PAGE_HEADER_SIZE: usize = 0;

pub struct PagedHashMap<E: EvictionPolicy, T: MemPool<E>> {
    func: Box<dyn Fn(&[u8], &[u8]) -> Vec<u8>>, // func(old_value, new_value) -> new_value
    pub bp: Arc<T>,
    c_key: ContainerKey,
    pub bucket_num: usize, // number of hash header pages
    phantom: PhantomData<E>,
}

impl<E: EvictionPolicy, T: MemPool<E>> PagedHashMap<E, T> {
    pub fn new(
        func: Box<dyn Fn(&[u8], &[u8]) -> Vec<u8>>,
        bp: Arc<T>,
        c_key: ContainerKey,
        from_container: bool,
    ) -> Self {
        if from_container {
            todo!("Implement from container");
            // let bucket_num = bp.get_page_for_read(c_id, 0, |root_page| {
            //     <Page as HashMetaPage>::get_n(root_page)
            // });
            // PagedHashMap {
            //     func,
            //     bp,
            //     c_id,
            //     bucket_num,
            // }
        } else {
            let root_page = bp.create_new_page_for_write(c_key).unwrap();
            #[cfg(feature = "stat")]
            inc_local_stat_total_page_count();
            // Need to do something for the root page e.g. set n
            log_info!(
                "Root page id: {}, Need to set root page",
                root_page.get_id()
            );
            assert_eq!(root_page.get_id(), 0, "root page id should be 0");

            for i in 1..DEFAULT_BUCKET_NUM + 1 {
                let mut new_page = bp.create_new_page_for_write(c_key).unwrap();
                #[cfg(feature = "stat")]
                inc_local_stat_total_page_count();
                new_page.init();
                assert_eq!(
                    new_page.get_id() as usize,
                    i,
                    "Initial new page id should be {}",
                    i
                );
            }

            PagedHashMap {
                func,
                bp: bp.clone(),
                c_key,
                bucket_num: DEFAULT_BUCKET_NUM,
                phantom: PhantomData,
            }
        }
    }

    fn hash<K: AsRef<[u8]> + Hash>(&self, key: &K) -> PageId {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (((hasher.finish() as usize) % self.bucket_num + 1) as usize) as PageId
    }

    pub fn insert<K: AsRef<[u8]> + Hash>(&self, key: K, value: K) -> Option<Vec<u8>> {
        let mut chain_len = 0;
        #[cfg(feature = "stat")]
        inc_local_stat_insert_count();

        let required_space = key.as_ref().len() + value.as_ref().len() + 6; // 6 is for key_len, val_offset, val_len
        if required_space > AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE {
            panic!("key and value should be less than a (page size - meta data size)");
        }

        let mut page_key = PageFrameKey::new(self.c_key, self.hash(&key));
        let mut current_page = self.bp.get_page_for_write(page_key).unwrap();
        #[cfg(feature = "stat")]
        {
            chain_len += 1;
        }

        let mut current_value: Option<Vec<u8>> = None;

        loop {
            let mut inserted = false;
            (inserted, current_value) =
                current_page.insert_with_function(key.as_ref(), value.as_ref(), &self.func);
            if inserted {
                #[cfg(feature = "stat")]
                {
                    update_local_stat_max_chain_len(chain_len);
                    update_local_stat_min_chain_len(chain_len);
                    update_to_global_stat();
                }
                return current_value;
            }
            if current_page.get_next_page_id() == 0 {
                break;
            }
            if current_value.is_some() {
                break;
            }
            page_key = PageFrameKey::new(self.c_key, current_page.get_next_page_id());
            current_page = self.bp.get_page_for_write(page_key).unwrap();
            #[cfg(feature = "stat")]
            {
                chain_len += 1;
            }
        }

        // 2 cases:
        // - no next page in chain: need to create new page.
        // - current_page is full and has next page in chain: need to move to next page in chain.

        // Apply the function on existing value or insert new
        let new_value = if let Some(ref val) = current_value {
            (self.func)(val, value.as_ref())
        } else {
            value.as_ref().to_vec()
        };

        while current_page.get_next_page_id() != 0 {
            page_key = PageFrameKey::new(self.c_key, current_page.get_next_page_id());
            current_page = self.bp.get_page_for_write(page_key).unwrap();
            #[cfg(feature = "stat")]
            {
                chain_len += 1;
            }
            let (inserted, _) = current_page.insert(key.as_ref(), &new_value);
            if inserted {
                #[cfg(feature = "stat")]
                {
                    update_local_stat_max_chain_len(chain_len);
                    update_local_stat_min_chain_len(chain_len);
                    update_to_global_stat();
                }
                return current_value;
            }
            #[cfg(feature = "stat")]
            {
                chain_len += 1;
            }
        }

        // If we reach here, we need to create a new page
        let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
        #[cfg(feature = "stat")]
        {
            update_local_stat_max_chain_len(chain_len);
            update_local_stat_min_chain_len(chain_len);
            inc_local_stat_total_page_count();
            update_to_global_stat();
        }

        new_page.init();
        current_page.set_next_page_id(new_page.get_id());

        new_page.insert(key.as_ref(), &new_value);
        current_value
    }

    pub fn get<K: AsRef<[u8]> + Hash>(&self, key: K) -> Option<Vec<u8>> {
        #[cfg(feature = "stat")]
        {
            inc_local_stat_get_count();
            update_to_global_stat();
        }

        let mut page_key = PageFrameKey::new(self.c_key, self.hash(&key));
        let mut result: Option<Vec<u8>> = None;

        loop {
            let current_page = self.bp.get_page_for_read(page_key).unwrap();
            result = current_page.get(key.as_ref());
            if result.is_some() || current_page.get_next_page_id() == 0 {
                break;
            }
            page_key = PageFrameKey::new(self.c_key, current_page.get_next_page_id());
        }
        result
    }

    pub fn remove<K: AsRef<[u8]> + Hash>(&self, key: K) -> Option<Vec<u8>> {
        #[cfg(feature = "stat")]
        {
            inc_local_stat_remove_count();
            update_to_global_stat();
        }

        let mut page_key = PageFrameKey::new(self.c_key, self.hash(&key));
        let mut result: Option<Vec<u8>> = None;

        loop {
            let mut current_page = self.bp.get_page_for_write(page_key).unwrap();
            result = current_page.remove(key.as_ref());
            if result.is_some() || current_page.get_next_page_id() == 0 {
                break;
            }
            page_key = PageFrameKey::new(self.c_key, current_page.get_next_page_id());
        }
        result
    }

    pub fn iter(&self) -> PagedHashMapIter<E, T> {
        let page_key = PageFrameKey::new(self.c_key, 1);
        let current_page = self.bp.get_page_for_read(page_key).unwrap();
        PagedHashMapIter {
            map: self,
            current_page: Some(current_page),
            current_index: 0,
            current_bucket: 1,
        }
    }

    #[cfg(feature = "stat")]
    pub fn stats(&self) -> String {
        LOCAL_STAT.with(|s| s.stat.to_string())
        // GLOBAL_STAT.lock().unwrap().to_string()
    }
}

unsafe impl<E: EvictionPolicy, T: MemPool<E>> Sync for PagedHashMap<E, T> {}
unsafe impl<E: EvictionPolicy, T: MemPool<E>> Send for PagedHashMap<E, T> {}

pub struct PagedHashMapIter<'a, E: EvictionPolicy, T: MemPool<E>> {
    map: &'a PagedHashMap<E, T>,
    current_page: Option<FrameReadGuard<'a, E>>,
    current_index: usize,
    current_bucket: usize,
}

impl<'a, E: EvictionPolicy, T: MemPool<E>> Iterator for PagedHashMapIter<'a, E, T> {
    type Item = (Vec<u8>, Vec<u8>); // Key and value types

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(page) = &self.current_page {
            if self.current_index < (page.num_slots() as usize) { 
                let sks = page.decode_shortkey_slot((self.current_index as u16));
                let skv = page.decode_shortkey_value_by_id((self.current_index as u16));
                
                let mut key = sks.key_prefix.to_vec();
                key.extend_from_slice(&skv.remain_key);
                // slice length of sks.key_len
                key.truncate(sks.key_len as usize);

                let value = skv.vals.to_vec();

                self.current_index += 1;
                return Some((key, value)); // Adjust based on actual data structure
            }

            let next_page_id = page.get_next_page_id();

            // If end of current page, fetch next
            if next_page_id != 0 {
                self.current_page = self.map.bp.get_page_for_read(PageFrameKey::new(self.map.c_key, next_page_id)).ok();
                self.current_index = 0;
                continue;
            }

            // No more pages in the current bucket, move to next bucket
            self.current_bucket += 1;
            if self.current_bucket <= self.map.bucket_num {
                self.current_page = self.map.bp.get_page_for_read(PageFrameKey::new(self.map.c_key, (self.current_bucket as u32))).ok();
                self.current_index = 0;
                continue;
            }

            // All buckets processed
            break;
        }

        None
    }
}

#[cfg(feature = "stat")]
mod stat {
    use super::*;
    use lazy_static::lazy_static;
    use std::{cell::UnsafeCell, os::unix::thread, sync::Mutex};

    pub struct PagedHashMapStat {
        pub insert_count: UnsafeCell<usize>,
        pub get_count: UnsafeCell<usize>,
        pub remove_count: UnsafeCell<usize>,

        pub total_page_count: UnsafeCell<usize>,
        pub max_chain_len: UnsafeCell<usize>,
        pub min_chain_len: UnsafeCell<usize>,
    }

    impl PagedHashMapStat {
        pub fn new() -> Self {
            PagedHashMapStat {
                insert_count: UnsafeCell::new(0),
                get_count: UnsafeCell::new(0),
                remove_count: UnsafeCell::new(0),
                total_page_count: UnsafeCell::new(0),
                max_chain_len: UnsafeCell::new(0),
                min_chain_len: UnsafeCell::new(999),
            }
        }

        pub fn inc_insert_count(&self) {
            unsafe {
                *self.insert_count.get() += 1;
            }
        }

        pub fn inc_get_count(&self) {
            unsafe {
                *self.get_count.get() += 1;
            }
        }

        pub fn inc_remove_count(&self) {
            unsafe {
                *self.remove_count.get() += 1;
            }
        }

        pub fn inc_total_page_count(&self) {
            unsafe {
                *self.total_page_count.get() += 1;
            }
        }

        pub fn update_max_chain_len(&self, chain_len: usize) {
            unsafe {
                let max_chain_len = self.max_chain_len.get();
                if chain_len > *max_chain_len {
                    *max_chain_len = chain_len;
                }
            }
        }

        pub fn update_min_chain_len(&self, chain_len: usize) {
            unsafe {
                let min_chain_len = self.min_chain_len.get();
                if chain_len < *min_chain_len {
                    *min_chain_len = chain_len;
                }
            }
        }

        pub fn to_string(&self) -> String {
            format!(
                "Paged Hash Map Statistics\ninsert_count: {}\nget_count: {}\nremove_count: {}\ntotal_page_count: {}\nmax_chain_len: {}\nmin_chain_len: {}",
                unsafe { *self.insert_count.get() },
                unsafe { *self.get_count.get() },
                unsafe { *self.remove_count.get() },
                unsafe { *self.total_page_count.get() },
                unsafe { *self.max_chain_len.get() },
                unsafe { *self.min_chain_len.get() }
            )
        }

        pub fn merge(&self, other: &PagedHashMapStat) {
            let insert_count = unsafe { &mut *self.insert_count.get() };
            let get_count = unsafe { &mut *self.get_count.get() };
            let remove_count = unsafe { &mut *self.remove_count.get() };
            let total_page_count = unsafe { &mut *self.total_page_count.get() };
            let max_chain_len = unsafe { &mut *self.max_chain_len.get() };
            let min_chain_len = unsafe { &mut *self.min_chain_len.get() };

            *insert_count += unsafe { &*other.insert_count.get() };
            *get_count += unsafe { &*other.get_count.get() };
            *remove_count += unsafe { &*other.remove_count.get() };
            *total_page_count += unsafe { &*other.total_page_count.get() };
            *max_chain_len = std::cmp::max(*max_chain_len, *unsafe { &*other.max_chain_len.get() });
            *min_chain_len = std::cmp::min(*min_chain_len, *unsafe { &*other.min_chain_len.get() });
        }
    }

    pub struct LocalStat {
        pub stat: PagedHashMapStat,
    }

    impl Drop for LocalStat {
        fn drop(&mut self) {
            GLOBAL_STAT.lock().unwrap().merge(&self.stat);
        }
    }

    lazy_static! {
        pub static ref GLOBAL_STAT: Mutex<PagedHashMapStat> = Mutex::new(PagedHashMapStat::new());
    }

    thread_local! {
        pub static LOCAL_STAT: LocalStat = LocalStat {
            stat: PagedHashMapStat::new()
        };
    }

    pub fn inc_local_stat_insert_count() {
        LOCAL_STAT.with(|s| {
            s.stat.inc_insert_count();
        });
    }

    pub fn inc_local_stat_get_count() {
        LOCAL_STAT.with(|s| {
            s.stat.inc_get_count();
        });
    }

    pub fn inc_local_stat_remove_count() {
        LOCAL_STAT.with(|s| {
            s.stat.inc_remove_count();
        });
    }

    pub fn inc_local_stat_total_page_count() {
        LOCAL_STAT.with(|s| {
            s.stat.inc_total_page_count();
        });
    }

    pub fn update_local_stat_max_chain_len(chain_len: usize) {
        LOCAL_STAT.with(|s| {
            s.stat.update_max_chain_len(chain_len);
        });
    }

    pub fn update_local_stat_min_chain_len(chain_len: usize) {
        LOCAL_STAT.with(|s| {
            s.stat.update_min_chain_len(chain_len);
        });
    }

    pub fn update_to_global_stat() {
        LOCAL_STAT.with(|s| {
            GLOBAL_STAT.lock().unwrap().merge(&s.stat);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{distributions::Alphanumeric, Rng};
    use std::collections::HashMap;
    use std::sync::Arc;

    // A simple hash function mimic for testing
    fn simple_hash_func(old_val: &[u8], new_val: &[u8]) -> Vec<u8> {
        [old_val, new_val].concat()
    }

    // Initialize the PagedHashMap for testing
    fn setup_paged_hash_map<E: EvictionPolicy, T: MemPool<E>>(bp: Arc<T>) -> PagedHashMap<E, T> {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let func = Box::new(simple_hash_func);
        PagedHashMap::new(func, bp, c_key, false)
    }

    /// Helper function to generate random strings of a given length
    fn random_string(length: usize) -> Vec<u8> {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(|c| c as u8)
            .collect()
    }

    #[test]
    fn test_insert_and_get() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value = "test_value".as_bytes();

        assert!(
            map.insert(key, value).is_none(),
            "Insert should succeed and return None on first insert"
        );

        let retrieved_value = map.get(key);
        assert_eq!(
            retrieved_value,
            Some(value.to_vec()),
            "Retrieved value should match the inserted value"
        );
    }

    #[test]
    fn test_update_existing_key() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value1 = "value1".as_bytes();
        let value2 = "value2".as_bytes();

        map.insert(key, value1);
        let old_value = map.insert(key, value2);
        assert_eq!(
            old_value,
            Some(value1.to_vec()),
            "Insert should return old value on update"
        );

        let retrieved_value = map.get(key);
        assert_eq!(
            retrieved_value,
            Some([value1, value2].concat()),
            "Retrieved value should be concatenated values"
        );
    }

    #[test]
    fn test_remove_key() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value = "test_value".as_bytes();

        map.insert(key, value);
        let removed_value = map.remove(key);
        assert_eq!(
            removed_value,
            Some(value.to_vec()),
            "Remove should return the removed value"
        );

        let retrieved_value = map.get(key);
        assert!(
            retrieved_value.is_none(),
            "Key should no longer exist in the map"
        );
    }

    #[test]
    fn test_page_overflow_and_chain_handling() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key1 = "key1".as_bytes();
        let value1 = vec![0u8; AVAILABLE_PAGE_SIZE / 2]; // Half page size to simulate near-full page
        let key2 = "key2".as_bytes();
        let value2 = vec![0u8; AVAILABLE_PAGE_SIZE / 2]; // Another half to trigger page overflow

        map.insert(key1, &value1);
        map.insert(key2, &value2); // Should trigger handling of a new page in chain

        let retrieved_value1 = map.get(key1);
        let retrieved_value2 = map.get(key2);
        assert_eq!(
            retrieved_value1,
            Some(value1),
            "First key should be retrievable"
        );
        assert_eq!(
            retrieved_value2,
            Some(value2),
            "Second key should be retrievable in a new page"
        );
    }

    #[test]
    fn test_edge_case_key_value_sizes() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = random_string(AVAILABLE_PAGE_SIZE / 4); // Large key
        let value = random_string(
            AVAILABLE_PAGE_SIZE / 2 - PAGE_HEADER_SIZE - SHORT_KEY_PAGE_HEADER_SIZE - 10,
        ); // Large value

        assert!(
            map.insert(&key, &value).is_none(),
            "Should handle large sizes without panic"
        );
        assert_eq!(
            map.get(&key),
            Some(value),
            "Should retrieve large value correctly"
        );
    }

    #[test]
    fn test_random_operations() {
        let map = Arc::new(setup_paged_hash_map(get_in_mem_pool()));
        let mut rng = rand::thread_rng();
        let mut data = HashMap::new();
        let mut inserted_keys = Vec::new(); // Track keys that are currently valid for removal

        for _ in 0..1000 {
            let operation: u8 = rng.gen_range(0..4);
            let key = random_string(10);
            let value = random_string(20);

            match operation {
                0 => {
                    // Insert
                    map.insert(&key, &value);
                    data.insert(key.clone(), value.clone());
                    inserted_keys.push(key); // Add key to the list of valid removal candidates
                }
                1 => {
                    // Update
                    if let Some(v) = data.get_mut(&key) {
                        *v = value.clone();
                        map.insert(&key, &value);
                    }
                }
                2 => {
                    // Get
                    let expected = data.get(&key);
                    assert_eq!(map.get(&key), expected.cloned(), "Mismatched values on get");
                }
                3 if !inserted_keys.is_empty() => {
                    // Remove
                    let remove_index = rng.gen_range(0..inserted_keys.len());
                    let remove_key = inserted_keys.remove(remove_index);
                    assert!(
                        map.remove(&remove_key).is_some(),
                        "Remove should return Some for existing key"
                    );
                    assert!(
                        map.get(&remove_key).is_none(),
                        "Get should return None after remove"
                    );
                    data.remove(&remove_key);
                }
                _ => {} // Skip remove if no keys are available
            }
        }
    }

    #[test]
    fn test_sequential_inserts_multiple_pages() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let mut last_key = vec![];

        for i in 0..(DEFAULT_BUCKET_NUM * 500) {
            // Insert more than the default number of buckets
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            map.insert(&key, &value);
            last_key = key.clone();

            // Check insertion correctness
            assert_eq!(
                map.get(&key),
                Some(value),
                "Each inserted key should retrieve correctly"
            );
        }

        // Ensure the last inserted key is still retrievable, implying multi-page handling
        assert!(
            map.get(&last_key).is_some(),
            "The last key should still be retrievable, indicating multi-page handling"
        );
    }

    #[test]
    fn test_random_operations_without_remove() {
        let map = Arc::new(setup_paged_hash_map(get_in_mem_pool()));
        let mut rng = rand::thread_rng();
        let mut data = HashMap::new();
        let mut inserted_keys = Vec::new(); // Track keys that are currently valid for removal

        for _ in 0..200000 {
            let operation: u8 = rng.gen_range(0..3);
            let key = random_string(10);
            let value = random_string(20);

            match operation {
                0 => {
                    // Insert
                    map.insert(&key, &value);
                    data.insert(key.clone(), value.clone());
                    inserted_keys.push(key); // Add key to the list of valid removal candidates
                }
                1 => {
                    // Update
                    if let Some(v) = data.get_mut(&key) {
                        *v = value.clone();
                        map.insert(&key, &value);
                    }
                }
                2 => {
                    // Get
                    let expected = data.get(&key);
                    assert_eq!(map.get(&key), expected.cloned(), "Mismatched values on get");
                }
                3 if !inserted_keys.is_empty() => {
                    // Remove
                    let remove_index = rng.gen_range(0..inserted_keys.len());
                    let remove_key = inserted_keys.remove(remove_index);
                    assert!(
                        map.remove(&remove_key).is_some(),
                        "Remove should return Some for existing key"
                    );
                    assert!(
                        map.get(&remove_key).is_none(),
                        "Get should return None after remove"
                    );
                    data.remove(&remove_key);
                }
                _ => {} // Skip remove if no keys are available
            }
        }
    }

    #[test]
    fn test_iterator_basic() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let mut expected_data = HashMap::new();

        // Insert a few key-value pairs
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            map.insert(&key, &value);
            expected_data.insert(key, value);
        }

        // Use the iterator to fetch all key-value pairs
        let mut iterated_data = HashMap::new();
        for (key, value) in map.iter() {
            iterated_data.insert(key, value);
        }

        // Check if all inserted data was iterated over correctly
        assert_eq!(expected_data, iterated_data, "Iterator did not retrieve all key-value pairs correctly");
    }

    #[test]
    fn test_iterator_across_pages() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let mut expected_data = HashMap::new();

        // Insert more key-value pairs than would fit on a single page
        for i in 0..1000 {  // Adjust the count according to the capacity of a single page
            let key = format!("key_large_{}", i).into_bytes();
            let value = format!("large_value_{}", i).into_bytes();
            map.insert(&key, &value);
            expected_data.insert(key, value);
        }

        // Use the iterator to fetch all key-value pairs
        let mut iterated_data = HashMap::new();
        for (key, value) in map.iter() {
            iterated_data.insert(key, value);
        }

        // Check if all inserted data was iterated over correctly
        assert_eq!(expected_data, iterated_data, "Iterator did not handle page overflow correctly");
    }

    // #[test]
    // fn test_concurrent_inserts_and_gets() {
    //     let map = Arc::new(setup_paged_hash_map(get_in_mem_pool()));
    //     let barrier = Arc::new(Barrier::new(10));
    //     let mut handles = vec![];

    //     for _ in 0..10 {
    //         let map_clone = map.clone();
    //         let barrier_clone = barrier.clone();
    //         handles.push(thread::spawn(move || {
    //             barrier_clone.wait();
    //             let key = random_string(10);
    //             let value = random_string(20);
    //             map_clone.insert(&key, &value);
    //             assert_eq!(map_clone.get(&key), Some(value));
    //         }));
    //     }

    //     for handle in handles {
    //         handle.join().expect("Thread panicked");
    //     }
    // }

    // #[test]
    // fn stress_test_concurrent_operations() {
    //     let map = Arc::new(setup_paged_hash_map(get_in_mem_pool()));
    //     let num_threads = 10; // Number of concurrent threads
    //     let num_operations_per_thread = 1000; // Operations per thread
    //     let barrier = Arc::new(Barrier::new(num_threads));
    //     let mut handles = vec![];

    //     for _ in 0..num_threads {
    //         let map_clone = Arc::clone(&map);
    //         let barrier_clone = Arc::clone(&barrier);
    //         handles.push(thread::spawn(move || {
    //             barrier_clone.wait(); // Ensure all threads start at the same time
    //             let mut rng = thread_rng();

    //             for _ in 0..num_operations_per_thread {
    //                 let key = random_string(10);
    //                 let value = random_string(20);
    //                 let operation: u8 = rng.gen_range(0..4);

    //                 match operation {
    //                     0 => { // Insert
    //                         map_clone.insert(&key, &value);
    //                     },
    //                     1 if !map_clone.get(&key).is_none() => { // Update only if key exists
    //                         map_clone.insert(&key, &value);
    //                     },
    //                     2 => { // Get
    //                         let _ = map_clone.get(&key);
    //                     },
    //                     3 if !map_clone.get(&key).is_none() => { // Remove only if key exists
    //                         map_clone.remove(&key);
    //                     },
    //                     _ => {}
    //                 }
    //             }
    //         }));
    //     }

    //     for handle in handles {
    //         handle.join().expect("Thread panicked during execution");
    //     }

    //     // Optionally, perform any final checks or cleanup here
    // }
}
