use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::Arc,
};

use crate::bp::prelude::*;
use crate::{log_info, page::PageId};

use super::shortkeypage::{ShortKeyPage, SHORT_KEY_PAGE_HEADER_SIZE};
use crate::page::AVAILABLE_PAGE_SIZE;

const DEFAULT_BUCKET_NUM: usize = 512;
const PAGE_HEADER_SIZE: usize = 0;

pub struct PagedHashMap<E: EvictionPolicy, T: MemPool<E>> {
    func: Box<dyn Fn(&[u8], &[u8]) -> Vec<u8>>, // func(old_value, new_value) -> new_value
    bp: Arc<T>,
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
            // Need to do something for the root page e.g. set n
            log_info!(
                "Root page id: {}, Need to set root page",
                root_page.get_id()
            );
            assert_eq!(root_page.get_id(), 0, "root page id should be 0");

            for i in 1..DEFAULT_BUCKET_NUM + 1 {
                let mut new_page = bp.create_new_page_for_write(c_key).unwrap();
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
        let required_space = key.as_ref().len() + value.as_ref().len() + 6; // 6 is for key_len, val_offset, val_len
        if required_space > AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE {
            panic!("key and value should be less than a (page size - meta data size)");
        }

        let mut page_key = PageFrameKey::new(self.c_key, self.hash(&key));
        let mut current_page = self.bp.get_page_for_write(page_key).unwrap();

        let mut current_value: Option<Vec<u8>> = None;

        loop {
            let mut inserted = false;
            (inserted, current_value) =
                current_page.insert_with_function(key.as_ref(), value.as_ref(), &self.func);
            if inserted {
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
            let (inserted, _) = current_page.insert(key.as_ref(), &new_value);
            if inserted {
                return current_value;
            }
        }

        // If we reach here, we need to create a new page
        let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
        new_page.init();
        current_page.set_next_page_id(new_page.get_id());

        new_page.insert(key.as_ref(), &new_value);
        current_value
    }

    pub fn get<K: AsRef<[u8]> + Hash>(&self, key: K) -> Option<Vec<u8>> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{distributions::Alphanumeric, Rng};
    use std::sync::Arc;
    use std::collections::HashMap;
    
    
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
