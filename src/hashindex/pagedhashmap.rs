use core::panic;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::{atomic::AtomicU32, Arc}, time::Duration,
};

use clap::Error;
#[cfg(feature = "stat")]
use stat::*;

use crate::{bp::prelude::*, page::{self, Page}};
use crate::{log_info, page::PageId};

use super::shortkeypage::{ShortKeyPage, ShortKeyPageError, SHORT_KEY_PAGE_HEADER_SIZE};
use crate::page::AVAILABLE_PAGE_SIZE;

const DEFAULT_BUCKET_NUM: usize = 4096;
const PAGE_HEADER_SIZE: usize = 0;

pub struct PagedHashMap<E: EvictionPolicy, T: MemPool<E>> {
    // func: Box<dyn Fn(&[u8], &[u8]) -> Vec<u8>>, // func(old_value, new_value) -> new_value
    pub bp: Arc<T>,
    c_key: ContainerKey,

    pub bucket_num: usize,         // number of hash header pages
    frame_buckets: Vec<AtomicU32>, // vec of frame_id for each bucket
    // bucket_metas: Vec<BucketMeta>, // first_frame_id, last_page_id, last_frame_id, bloomfilter // no need to be page
    phantom: PhantomData<E>,
}

#[derive(Debug)]
pub enum PagedHashMapError {
    KeyExists,
    KeyNotFound,
    OutOfSpace,
    PageCreationFailed,
    WriteLatchFailed,
    Other(String),
}

struct BucketMeta {
    first_frame_id: u32,
    last_page_id: u32,
    last_frame_id: u32,
    bloomfilter: Vec<u8>,
}

impl<E: EvictionPolicy, T: MemPool<E>> PagedHashMap<E, T> {
    pub fn new(
        // func: Box<dyn Fn(&[u8], &[u8]) -> Vec<u8>>,
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
            let mut frame_buckets = (0..DEFAULT_BUCKET_NUM + 1)
                .map(|_| AtomicU32::new(u32::MAX))
                .collect::<Vec<AtomicU32>>();
            //vec![AtomicU32::new(u32::MAX); DEFAULT_BUCKET_NUM + 1];

            // SET ROOT: Need to do something for the root page e.g. set n
            let root_page = bp.create_new_page_for_write(c_key).unwrap();
            #[cfg(feature = "stat")]
            inc_local_stat_total_page_count();
            log_info!(
                "Root page id: {}, Need to set root page",
                root_page.get_id()
            );
            assert_eq!(root_page.get_id(), 0, "root page id should be 0");
            frame_buckets[0].store(root_page.frame_id(), std::sync::atomic::Ordering::Release);
            //root_page.frame_id();

            // SET HASH BUCKET PAGES
            for i in 1..DEFAULT_BUCKET_NUM + 1 {
                let mut new_page = bp.create_new_page_for_write(c_key).unwrap();
                #[cfg(feature = "stat")]
                inc_local_stat_total_page_count();
                frame_buckets[i].store(new_page.frame_id(), std::sync::atomic::Ordering::Release);
                new_page.init();
                assert_eq!(
                    new_page.get_id() as usize,
                    i,
                    "Initial new page id should be {}",
                    i
                );
            }

            PagedHashMap {
                // func,
                bp: bp.clone(),
                c_key,
                bucket_num: DEFAULT_BUCKET_NUM,
                frame_buckets,
                phantom: PhantomData,
            }
        }
    }

    fn hash<K: AsRef<[u8]> + Hash>(&self, key: &K) -> PageId {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (((hasher.finish() as usize) % self.bucket_num + 1) as usize) as PageId
    }

    /// Insert a new key-value pair into the index.
    /// If the key already exists, it will return an error.
    pub fn insert<K: AsRef<[u8]> + Hash>(&self, key: K, val: K) -> Result<(), PagedHashMapError> {
        #[cfg(feature = "stat")]
        inc_local_stat_insert_count();
        
        let required_space = key.as_ref().len() + val.as_ref().len() + 6; // 6 is for key_len, val_offset, val_len
        if required_space > AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE {
            panic!("key and value should be less than a (page size - meta data size)");
        }

        let hashed_key = self.hash(&key);
        let expect_frame_id =
            self.frame_buckets[hashed_key as usize].load(std::sync::atomic::Ordering::Acquire);

        let page_key = PageFrameKey::new_with_frame_id(self.c_key, hashed_key, expect_frame_id);
        
        let mut last_page = self.insert_traverse_to_endofchain_for_write(page_key, key.as_ref())?;
        match last_page.insert(key.as_ref(), val.as_ref()) {
            Ok(_) => Ok(()),
            Err(ShortKeyPageError::KeyExists) => panic!("Key exists should detected in traverse_to_endofchain_for_write"),
            Err(ShortKeyPageError::OutOfSpace) => {
                let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
                #[cfg(feature = "stat")]{
                    inc_local_stat_total_page_count();
                }
                new_page.init();
                last_page.set_next_page_id(new_page.get_id());
                last_page.set_next_frame_id(new_page.frame_id());
                match new_page.insert(key.as_ref(), val.as_ref()) {
                    Ok(_) => Ok(()),
                    Err(err) => panic!("Inserting to new page should succeed: {:?}", err)
                }
            },
            Err(err) => Err(PagedHashMapError::Other(format!("Insert error: {:?}", err))),
        }


        // let mut current_page = self.bp.get_page_for_read(page_key).unwrap();
        // if current_page.frame_id() != expect_frame_id {
        //     self.frame_buckets[hashed_key as usize].store(
        //         current_page.frame_id(),
        //         std::sync::atomic::Ordering::Release,
        //     );
        // }

        // let mut chain_len = 0;
        // #[cfg(feature = "stat")]
        // {
        //     chain_len += 1;
        // }

        // loop {
        //     if current_page.get_next_page_id() == 0 {
        //         // current_page = match current_page.try_upgrade(true) {
        //         //     Ok(mut upgraded_page) => {
        //         //         upgraded_page.set_next_frame_id(next_page.frame_id());
        //         //         upgraded_page.downgrade()
        //         //     }
        //         //     Err(current_page) => {
                        
        //         //     },
        //         // };
        //         match current_page.insert(key.as_ref(), value.as_ref()) {
        //             Ok(_) => {
        //                 #[cfg(feature = "stat")]
        //                 {
        //                     update_local_stat_max_chain_len(chain_len);
        //                     update_local_stat_min_chain_len(chain_len);
        //                 }
        //                 return Ok(());
        //             }
        //             Err(ShortKeyPageError::KeyExists) => {
        //                 return Err(PagedHashMapError::KeyExists);
        //             }
        //             Err(ShortKeyPageError::OutOfSpace) => {
        //                 break;
        //             }
        //             Err(err) => {
        //                 return Err(PagedHashMapError::Other(format!("Insert error: {:?}", err)));
        //             }
        //         }
        //     } else {
        //         match current_page.get(key.as_ref()) {
        //             Some(_) => {
        //                 return Err(PagedHashMapError::KeyExists);
        //             }
        //             None => {
        //                 let (next_page_id, next_frame_id) = (
        //                     current_page.get_next_page_id(),
        //                     current_page.get_next_frame_id(),
        //                 );
        //                 page_key = PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
        //                 let next_page = self.bp.get_page_for_write(page_key).unwrap();
        //                 if next_frame_id != next_page.frame_id() {
        //                     current_page.set_next_frame_id(next_page.frame_id());
        //                 }
        //                 #[cfg(feature = "stat")]
        //                 {
        //                     chain_len += 1;
        //                 }
        //                 current_page = next_page;
        //             }
        //         }
        //     }
        // }

        // // If we reach here, we need to create a new page
        // let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
        // #[cfg(feature = "stat")]
        // {
        //     chain_len += 1;
        //     update_local_stat_max_chain_len(chain_len);
        //     update_local_stat_min_chain_len(chain_len);
        //     inc_local_stat_total_page_count();
        // }
        // new_page.init();
        // current_page.set_next_page_id(new_page.get_id());
        // current_page.set_next_frame_id(new_page.frame_id());

        // match new_page.insert(key.as_ref(), value.as_ref()) {
        //     Ok(_) => Ok(()),
        //     Err(err) => Err(PagedHashMapError::Other(format!("Insert error: {:?}", err))),
        // }
    }

    fn insert_traverse_to_endofchain_for_write(&self, page_key: PageFrameKey, key: &[u8]) -> Result<FrameWriteGuard<E>, PagedHashMapError> {
        let base = Duration::from_millis(1);
        let mut attempts = 0;
        loop {
            let page = self.try_insert_traverse_to_endofchain_for_write(page_key, key);
            match page {
                Ok(page) => {
                    return Ok(page);
                }
                Err(PagedHashMapError::WriteLatchFailed) => {
                    attempts += 1;
                    std::thread::sleep(base * attempts);
                }
                Err(PagedHashMapError::KeyExists) => {
                    return Err(PagedHashMapError::KeyExists);
                }
                Err(err) => {
                    panic!("Traverse to end of chain error: {:?}", err);
                }
            }    
        }
    }

    fn try_insert_traverse_to_endofchain_for_write(&self, page_key: PageFrameKey, key: &[u8]) -> Result<FrameWriteGuard<E>, PagedHashMapError> {
        let mut current_page = self.read_page(page_key);
        loop {
            if current_page.is_exist(key) {
                return Err(PagedHashMapError::KeyExists);
            }
            let next_page_id = current_page.get_next_page_id();
            if next_page_id == 0 {
                match current_page.try_upgrade(true) {
                    Ok(mut upgraded_page) => {
                        return Ok(upgraded_page);
                    }
                    Err(current_page) => {
                        return Err(PagedHashMapError::WriteLatchFailed);
                    },
                };
            }
            let next_frame_id = current_page.get_next_frame_id();
            let next_page_key = PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
            let next_page = self.read_page(next_page_key);
            if next_frame_id != next_page.frame_id() {
                current_page = match current_page.try_upgrade(true) {
                    Ok(mut upgraded_page) => {
                        upgraded_page.set_next_frame_id(next_page.frame_id());
                        upgraded_page.downgrade()
                    }
                    Err(current_page) => current_page,
                };
            }
            current_page = next_page;
        }
    }

    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard<E> {
        loop {
            let page = self.bp.get_page_for_read(page_key);
            match page {
                Ok(page) => {
                    return page;
                }
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    std::hint::spin_loop();
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(err) => {
                    panic!("Read page error: {:?}", err);
                }
            }
        }
    }

    /// Update the value of an existing key.
    /// If the key does not exist, it will return an error.
    pub fn update<K: AsRef<[u8]> + Hash>(
        &self,
        key: K,
        value: K,
    ) -> Result<Vec<u8>, PagedHashMapError> {
        #[cfg(feature = "stat")]
        inc_local_stat_insert_count();

        let hashed_key = self.hash(&key);
        let expect_frame_id =
            self.frame_buckets[hashed_key as usize].load(std::sync::atomic::Ordering::Acquire);

        let mut page_key = PageFrameKey::new_with_frame_id(self.c_key, hashed_key, expect_frame_id);
        let mut current_page = self.bp.get_page_for_write(page_key).unwrap();
        if current_page.frame_id() != expect_frame_id {
            self.frame_buckets[hashed_key as usize].store(
                current_page.frame_id(),
                std::sync::atomic::Ordering::Release,
            );
        }

        let mut chain_len = 0;
        #[cfg(feature = "stat")]
        {
            chain_len += 1;
        }

        loop {
            match current_page.update(key.as_ref(), value.as_ref()) {
                Ok(old_value) => {
                    #[cfg(feature = "stat")]
                    {
                        update_local_stat_max_chain_len(chain_len);
                        update_local_stat_min_chain_len(chain_len);
                    }
                    return Ok(old_value);
                }
                Err(ShortKeyPageError::KeyNotFound) => {
                    let (next_page_id, next_frame_id) = (
                        current_page.get_next_page_id(),
                        current_page.get_next_frame_id(),
                    );
                    if next_page_id == 0 {
                        return Err(PagedHashMapError::KeyNotFound);
                    }
                    page_key =
                        PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
                    let next_page = self.bp.get_page_for_write(page_key).unwrap();
                    if next_frame_id != next_page.frame_id() {
                        current_page.set_next_frame_id(next_page.frame_id());
                    }
                    #[cfg(feature = "stat")]
                    {
                        chain_len += 1;
                    }
                    current_page = next_page;
                }
                Err(err) => {
                    return Err(PagedHashMapError::Other(format!("Update error: {:?}", err)));
                }
            }
        }
    }

    /// Upsert a key-value pair into the index.
    /// If the key already exists, it will update the value and return old value.
    /// If the key does not exist, it will insert a new key-value pair.
    pub fn upsert<K: AsRef<[u8]> + Hash>(
        &self,
        key: K,
        value: K,
    ) -> Result<Option<Vec<u8>>, PagedHashMapError> {
        #[cfg(feature = "stat")]
        inc_local_stat_insert_count();

        let required_space = key.as_ref().len() + value.as_ref().len() + 6; // 6 is for key_len, val_offset, val_len
        if required_space > AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE {
            panic!("key and value should be less than a (page size - meta data size)");
        }

        let hashed_key = self.hash(&key);
        let expect_frame_id =
            self.frame_buckets[hashed_key as usize].load(std::sync::atomic::Ordering::Acquire);

        let mut page_key = PageFrameKey::new_with_frame_id(self.c_key, hashed_key, expect_frame_id);
        let mut current_page = self.bp.get_page_for_write(page_key).unwrap();
        if current_page.frame_id() != expect_frame_id {
            self.frame_buckets[hashed_key as usize].store(
                current_page.frame_id(),
                std::sync::atomic::Ordering::Release,
            );
        }

        let mut chain_len = 0;
        #[cfg(feature = "stat")]
        {
            chain_len += 1;
        }

        loop {
            match current_page.upsert(key.as_ref(), value.as_ref()) {
                (true, old_value) => {
                    #[cfg(feature = "stat")]
                    {
                        update_local_stat_max_chain_len(chain_len);
                        update_local_stat_min_chain_len(chain_len);
                    }
                    return Ok(old_value);
                }
                (false, old_value) => {
                    let (next_page_id, next_frame_id) = (
                        current_page.get_next_page_id(),
                        current_page.get_next_frame_id(),
                    );
                    if next_page_id == 0 {
                        break;
                    }
                    page_key =
                        PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
                    let next_page = self.bp.get_page_for_write(page_key).unwrap();
                    if next_frame_id != next_page.frame_id() {
                        current_page.set_next_frame_id(next_page.frame_id());
                    }
                    #[cfg(feature = "stat")]
                    {
                        chain_len += 1;
                    }
                    current_page = next_page;
                }
            }
        }

        // If we reach here, we need to create a new page
        let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
        #[cfg(feature = "stat")]
        {
            chain_len += 1;
            update_local_stat_max_chain_len(chain_len);
            update_local_stat_min_chain_len(chain_len);
            inc_local_stat_total_page_count();
        }
        new_page.init();
        current_page.set_next_page_id(new_page.get_id());
        current_page.set_next_frame_id(new_page.frame_id());

        match new_page.upsert(key.as_ref(), value.as_ref()) {
            (true, old_value) => Ok(old_value),
            (false, _) => Err(PagedHashMapError::OutOfSpace),
        }
    }

    /// Upsert with a custom merge function.
    /// If the key already exists, it will update the value with the merge function.
    /// If the key does not exist, it will insert a new key-value pair.
    pub fn upsert_with_merge<K: AsRef<[u8]> + Hash>(
        &self,
        key: K,
        value: K,
        merge: fn(&[u8], &[u8]) -> Vec<u8>,
    ) -> Option<Vec<u8>> {
        let mut chain_len = 0;
        #[cfg(feature = "stat")]
        inc_local_stat_insert_count();

        let required_space = key.as_ref().len() + value.as_ref().len() + 6; // 6 is for key_len, val_offset, val_len
        if required_space > AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE {
            panic!("key and value should be less than a (page size - meta data size)");
        }

        let hashed_key = self.hash(&key);
        let expect_frame_id =
            self.frame_buckets[hashed_key as usize].load(std::sync::atomic::Ordering::Acquire);

        let mut page_key = PageFrameKey::new_with_frame_id(self.c_key, hashed_key, expect_frame_id);
        let mut current_page = self.bp.get_page_for_write(page_key).unwrap();
        if current_page.frame_id() != expect_frame_id {
            self.frame_buckets[hashed_key as usize].store(
                current_page.frame_id(),
                std::sync::atomic::Ordering::Release,
            );
            // self.frame_buckets[hashed_key as usize] = current_page.frame_id();
        }
        #[cfg(feature = "stat")]
        {
            chain_len += 1;
        }

        let mut current_value: Option<Vec<u8>> = None;

        loop {
            let mut inserted = false;
            (inserted, current_value) =
                current_page.upsert_with_merge(key.as_ref(), value.as_ref(), &merge);
            if inserted {
                #[cfg(feature = "stat")]
                {
                    update_local_stat_max_chain_len(chain_len);
                    update_local_stat_min_chain_len(chain_len);
                }
                return current_value;
            }
            let (next_page_id, next_frame_id) = (
                current_page.get_next_page_id(),
                current_page.get_next_frame_id(),
            );

            if next_page_id == 0 {
                break;
            }
            if current_value.is_some() {
                break;
            }
            page_key = PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
            let next_page = self.bp.get_page_for_write(page_key).unwrap();
            if next_frame_id != next_page.frame_id() {
                current_page.set_next_frame_id(next_page.frame_id());
            }
            #[cfg(feature = "stat")]
            {
                chain_len += 1;
            }
            current_page = next_page;
        }

        // 2 cases:
        // - no next page in chain: need to create new page. (next_page_id == 0)
        // - current_page is full and has next page in chain: need to move to next page in chain. (current_value.is_some())

        // Apply the function on existing value or insert new
        let new_value = if let Some(ref val) = current_value {
            (merge)(val, value.as_ref())
        } else {
            value.as_ref().to_vec()
        };

        let mut next_page_id = current_page.get_next_page_id();

        while next_page_id != 0 {
            let next_frame_id = current_page.get_next_frame_id();

            page_key = PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
            let next_page = self.bp.get_page_for_write(page_key).unwrap();
            if next_frame_id != next_page.frame_id() {
                current_page.set_next_frame_id(next_page.frame_id());
            }
            #[cfg(feature = "stat")]
            {
                chain_len += 1;
            }
            current_page = next_page;

            let (inserted, _) = current_page.upsert(key.as_ref(), &new_value);
            if inserted {
                #[cfg(feature = "stat")]
                {
                    update_local_stat_max_chain_len(chain_len);
                    update_local_stat_min_chain_len(chain_len);
                }
                return current_value;
            }
            next_page_id = current_page.get_next_page_id();
        }

        // If we reach here, we need to create a new page
        let mut new_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
        #[cfg(feature = "stat")]
        {
            chain_len += 1;
            update_local_stat_max_chain_len(chain_len);
            update_local_stat_min_chain_len(chain_len);
            inc_local_stat_total_page_count();
        }

        new_page.init();
        current_page.set_next_page_id(new_page.get_id());
        current_page.set_next_frame_id(new_page.frame_id());

        new_page.upsert(key.as_ref(), &new_value);
        current_value
    }

    /// Get the value of a key from the index.
    /// If the key does not exist, it will return an error.
    pub fn get<K: AsRef<[u8]> + Hash>(&self, key: K) -> Result<Vec<u8>, PagedHashMapError> {
        #[cfg(feature = "stat")]
        {
            inc_local_stat_get_count();
        }

        let hashed_key = self.hash(&key);
        let expect_frame_id =
            self.frame_buckets[hashed_key as usize].load(std::sync::atomic::Ordering::Acquire);

        let mut page_key = PageFrameKey::new_with_frame_id(self.c_key, hashed_key, expect_frame_id);
        let mut current_page = self.bp.get_page_for_read(page_key).unwrap();
        if current_page.frame_id() != expect_frame_id {
            self.frame_buckets[hashed_key as usize].store(
                current_page.frame_id(),
                std::sync::atomic::Ordering::Release,
            );
            // self.frame_buckets[hashed_key as usize] = current_page.frame_id();
        }

        let mut result: Option<Vec<u8>> = None;

        loop {
            result = current_page.get(key.as_ref());
            let (next_page_id, next_frame_id) = (
                current_page.get_next_page_id(),
                current_page.get_next_frame_id(),
            );
            if result.is_some() || next_page_id == 0 {
                break;
            }
            page_key = PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
            let next_page = self.bp.get_page_for_read(page_key).unwrap();
            if next_frame_id != next_page.frame_id() {
                current_page = match current_page.try_upgrade(true) {
                    Ok(mut upgraded_page) => {
                        upgraded_page.set_next_frame_id(next_page.frame_id());
                        upgraded_page.downgrade()
                    }
                    Err(current_page) => current_page,
                };
            }
            current_page = next_page;
        }
        match result {
            Some(v) => Ok(v),
            None => Err(PagedHashMapError::KeyNotFound),
        }
    }

    pub fn remove<K: AsRef<[u8]> + Hash>(&self, key: K) -> Option<Vec<u8>> {
        #[cfg(feature = "stat")]
        {
            inc_local_stat_remove_count();
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
        let stats = GLOBAL_STAT.lock().unwrap();
        LOCAL_STAT.with(|local_stat| {
            stats.merge(&local_stat.stat);
            local_stat.stat.clear();
        });
        stats.to_string()
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
                self.current_page = self
                    .map
                    .bp
                    .get_page_for_read(PageFrameKey::new(self.map.c_key, next_page_id))
                    .ok();
                self.current_index = 0;
                continue;
            }

            // No more pages in the current bucket, move to next bucket
            self.current_bucket += 1;
            if self.current_bucket <= self.map.bucket_num {
                self.current_page = self
                    .map
                    .bp
                    .get_page_for_read(PageFrameKey::new(
                        self.map.c_key,
                        (self.current_bucket as u32),
                    ))
                    .ok();
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

        pub fn clear(&self) {
            unsafe {
                *self.insert_count.get() = 0;
                *self.get_count.get() = 0;
                *self.remove_count.get() = 0;
                *self.total_page_count.get() = 0;
                *self.max_chain_len.get() = 0;
                *self.min_chain_len.get() = 999;
            }
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
        // PagedHashMap::new(func, bp, c_key, false)
        PagedHashMap::new(bp, c_key, false)
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
            map.insert(key, value).is_ok(),
            "Insert should succeed and return Ok on first insert"
        );

        let retrieved_value = map.get(key).unwrap();
        assert_eq!(
            retrieved_value,
            value.to_vec(),
            "Retrieved value should match inserted value"
        );
    }

    #[test]
    fn test_update_existing_key() {
        let mut map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value1 = "value1".as_bytes();
        let value2 = "value2".as_bytes();
        let func = simple_hash_func;

        map.upsert_with_merge(key, value1, func);
        let old_value = map.upsert_with_merge(key, value2, func);
        assert_eq!(
            old_value,
            Some(value1.to_vec()),
            "Insert should return old value on update"
        );

        let retrieved_value = map.get(key).unwrap();
        assert_eq!(
            retrieved_value,
            [value1, value2].concat(),
            "Retrieved value should be concatenated values"
        );
    }

    #[test]
    fn test_remove_key() {
        let mut map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value = "test_value".as_bytes();
        let func = simple_hash_func;

        map.upsert_with_merge(key, value, func);
        let removed_value = map.remove(key);
        assert_eq!(
            removed_value,
            Some(value.to_vec()),
            "Remove should return the removed value"
        );

        let retrieved_value = map.get(key);
        // check error
        assert!(
            retrieved_value.is_err(),
            "Get should return an error after key is removed"
        );
    }

    #[test]
    fn test_page_overflow_and_chain_handling() {
        let mut map = setup_paged_hash_map(get_in_mem_pool());
        let key1 = "key1".as_bytes();
        let value1 = vec![0u8; AVAILABLE_PAGE_SIZE / 2]; // Half page size to simulate near-full page
        let key2 = "key2".as_bytes();
        let value2 = vec![0u8; AVAILABLE_PAGE_SIZE / 2]; // Another half to trigger page overflow

        let func = simple_hash_func;

        map.upsert_with_merge(key1, &value1, func);
        map.upsert_with_merge(key2, &value2, func); // Should trigger handling of a new page in chain

        let retrieved_value1 = map.get(key1).unwrap();
        let retrieved_value2 = map.get(key2).unwrap();
        assert_eq!(retrieved_value1, value1, "First key should be retrievable");
        assert_eq!(
            retrieved_value2, value2,
            "Second key should be retrievable in a new page"
        );
    }

    #[test]
    fn test_edge_case_key_value_sizes() {
        let mut map = setup_paged_hash_map(get_in_mem_pool());
        let key = random_string(AVAILABLE_PAGE_SIZE / 4); // Large key
        let value = random_string(
            AVAILABLE_PAGE_SIZE / 2 - PAGE_HEADER_SIZE - SHORT_KEY_PAGE_HEADER_SIZE - 10,
        ); // Large value

        let func = simple_hash_func;

        assert!(
            map.upsert_with_merge(&key, &value, func).is_none(),
            "Should handle large sizes without panic"
        );
        assert_eq!(
            map.get(&key).unwrap(),
            value,
            "Should retrieve large value correctly"
        );
    }

    #[test]
    fn test_random_operations() {
        // let mut map = Arc::new(setup_paged_hash_map(get_in_mem_pool()));
        let mut map = setup_paged_hash_map(get_in_mem_pool());
        let mut rng = rand::thread_rng();
        let mut data = HashMap::new();
        let mut inserted_keys = Vec::new(); // Track keys that are currently valid for removal

        let func = simple_hash_func;

        for _ in 0..1000 {
            let operation: u8 = rng.gen_range(0..4);
            let key = random_string(10);
            let value = random_string(20);

            match operation {
                0 => {
                    // upsert_with_merge
                    map.upsert_with_merge(&key, &value, func);
                    data.insert(key.clone(), value.clone());
                    inserted_keys.push(key); // Add key to the list of valid removal candidates
                }
                1 => {
                    // Update
                    if let Some(v) = data.get_mut(&key) {
                        *v = value.clone();
                        map.upsert_with_merge(&key, &value, func);
                    }
                }
                2 => {
                    // Get
                    let expected = data.get(&key);
                    // if key is not in data, map.get is error
                    match expected {
                        Some(v) => {
                            assert_eq!(
                                map.get(&key).unwrap(),
                                v.clone(),
                                "Mismatched values on get"
                            );
                        }
                        None => {
                            assert!(
                                map.get(&key).is_err(),
                                "Get should return an error if key is not in data"
                            );
                        }
                    }
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
                        map.get(&remove_key).is_err(),
                        "Get should return None after remove"
                    );
                    data.remove(&remove_key);
                }
                _ => {} // Skip remove if no keys are available
            }
        }
    }

    #[test]
    fn test_sequential_upsert_with_merge_multiple_pages() {
        let mut map = setup_paged_hash_map(get_in_mem_pool());
        let mut last_key = vec![];

        let func = simple_hash_func;

        for i in 0..(DEFAULT_BUCKET_NUM * 10) {
            // upsert_with_merge more than the default number of buckets
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            map.upsert_with_merge(&key, &value, func);
            last_key = key.clone();

            // Check insertion correctness
            assert_eq!(
                map.get(&key).unwrap(),
                value,
                "Each upsert_with_mergeed key should retrieve correctly"
            );
        }

        // Ensure the last upsert_with_mergeed key is still retrievable, implying multi-page handling
        assert!(
            map.get(&last_key).is_ok(),
            "The last key should still be retrievable, indicating multi-page handling"
        );
    }

    #[test]
    fn test_random_operations_without_remove() {
        // let mut map = Arc::new(setup_paged_hash_map(get_in_mem_pool()));
        let mut map = setup_paged_hash_map(get_in_mem_pool());
        let mut rng = rand::thread_rng();
        let mut data = HashMap::new();
        let mut inserted_keys = Vec::new(); // Track keys that are currently valid for removal

        let func = simple_hash_func;

        for _ in 0..200000 {
            let operation: u8 = rng.gen_range(0..3);
            let key = random_string(10);
            let value = random_string(20);

            match operation {
                0 => {
                    // upsert_with_merge
                    map.upsert_with_merge(&key, &value, func);
                    data.insert(key.clone(), value.clone());
                    inserted_keys.push(key); // Add key to the list of valid removal candidates
                }
                1 => {
                    // Update
                    if let Some(v) = data.get_mut(&key) {
                        *v = value.clone();
                        map.upsert_with_merge(&key, &value, func);
                    }
                }
                2 => {
                    // Get
                    let expected = data.get(&key);
                    match expected {
                        Some(v) => {
                            assert_eq!(
                                map.get(&key).unwrap(),
                                v.clone(),
                                "Mismatched values on get"
                            );
                        }
                        None => {
                            assert!(
                                map.get(&key).is_err(),
                                "Get should return an error if key is not in data"
                            );
                        }
                    }
                    // assert_eq!(map.get(&key), expected.cloned(), "Mismatched values on get");
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
                        map.get(&remove_key).is_err(),
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
        let mut map = setup_paged_hash_map(get_in_mem_pool());
        let mut expected_data = HashMap::new();

        let func = simple_hash_func;

        // upsert_with_merge a few key-value pairs
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            map.upsert_with_merge(&key, &value, func);
            expected_data.insert(key, value);
        }

        // Use the iterator to fetch all key-value pairs
        let mut iterated_data = HashMap::new();
        for (key, value) in map.iter() {
            iterated_data.insert(key, value);
        }

        // Check if all upsert_with_mergeed data was iterated over correctly
        assert_eq!(
            expected_data, iterated_data,
            "Iterator did not retrieve all key-value pairs correctly"
        );
    }

    #[test]
    fn test_iterator_across_pages() {
        let mut map = setup_paged_hash_map(get_in_mem_pool());
        let mut expected_data = HashMap::new();

        let func = simple_hash_func;

        // upsert_with_merge more key-value pairs than would fit on a single page
        for i in 0..1000 {
            // Adjust the count according to the capacity of a single page
            let key = format!("key_large_{}", i).into_bytes();
            let value = format!("large_value_{}", i).into_bytes();
            map.upsert_with_merge(&key, &value, func);
            expected_data.insert(key, value);
        }

        // Use the iterator to fetch all key-value pairs
        let mut iterated_data = HashMap::new();
        for (key, value) in map.iter() {
            iterated_data.insert(key, value);
        }

        // Check if all upsert_with_mergeed data was iterated over correctly
        assert_eq!(
            expected_data, iterated_data,
            "Iterator did not handle page overflow correctly"
        );
    }

    #[test]
    fn test_insert_existing_key() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value1 = "value1".as_bytes();
        let value2 = "value2".as_bytes();

        map.insert(key, value1).unwrap();
        assert!(
            map.insert(key, value2).is_err(),
            "Insert should return Err when key already exists"
        );

        let retrieved_value = map.get(key).unwrap();
        assert_eq!(
            retrieved_value,
            value1.to_vec(),
            "Retrieved value should match the initially inserted value"
        );
    }

    #[test]
    fn test_update_existing_key2() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value1 = "value1".as_bytes();
        let value2 = "value2".as_bytes();

        map.insert(key, value1).unwrap();
        let old_value = map.update(key, value2).unwrap();
        assert_eq!(old_value, value1.to_vec(), "Update should return old value");

        let retrieved_value = map.get(key).unwrap();
        assert_eq!(
            retrieved_value,
            value2.to_vec(),
            "Retrieved value should match the updated value"
        );
    }

    #[test]
    fn test_update_non_existent_key() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value = "value".as_bytes();

        assert!(
            map.update(key, value).is_err(),
            "Update should return Err when key does not exist"
        );
    }

    #[test]
    fn test_upsert() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value1 = "value1".as_bytes();
        let value2 = "value2".as_bytes();

        let old_value = map.upsert(key, value1).unwrap();
        assert_eq!(
            old_value, None,
            "Upsert should return None when key is newly inserted"
        );

        let old_value = map.upsert(key, value2).unwrap();
        assert_eq!(
            old_value,
            Some(value1.to_vec()),
            "Upsert should return old value when key is updated"
        );

        let retrieved_value = map.get(key).unwrap();
        assert_eq!(
            retrieved_value,
            value2.to_vec(),
            "Retrieved value should match the last upserted value"
        );
    }

    #[test]
    fn test_remove_key2() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = "test_key".as_bytes();
        let value = "test_value".as_bytes();

        map.insert(key, value).unwrap();
        let removed_value = map.remove(key).unwrap();
        assert_eq!(
            removed_value,
            value.to_vec(),
            "Remove should return the removed value"
        );

        let retrieved_value = map.get(key);
        assert!(
            retrieved_value.is_err(),
            "Key should no longer exist in the map"
        );
    }

    #[test]
    fn test_page_overflow_and_chain_handling2() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key1 = "key1".as_bytes();
        let value1 = vec![0u8; AVAILABLE_PAGE_SIZE / 2]; // Half page size to simulate near-full page
        let key2 = "key2".as_bytes();
        let value2 = vec![0u8; AVAILABLE_PAGE_SIZE / 2]; // Another half to trigger page overflow

        map.insert(key1, &value1).unwrap();
        map.insert(key2, &value2).unwrap(); // Should trigger handling of a new page in chain

        let retrieved_value1 = map.get(key1).unwrap();
        let retrieved_value2 = map.get(key2).unwrap();
        assert_eq!(retrieved_value1, value1, "First key should be retrievable");
        assert_eq!(
            retrieved_value2, value2,
            "Second key should be retrievable in a new page"
        );
    }

    #[test]
    fn test_edge_case_key_value_sizes2() {
        let map = setup_paged_hash_map(get_in_mem_pool());
        let key = random_string(AVAILABLE_PAGE_SIZE / 4); // Large key
        let value = random_string(
            AVAILABLE_PAGE_SIZE / 2 - PAGE_HEADER_SIZE - SHORT_KEY_PAGE_HEADER_SIZE - 10,
        ); // Large value

        assert!(
            map.upsert(&key, &value).is_ok(),
            "Should handle large sizes without panic"
        );
        assert_eq!(
            map.get(&key).unwrap(),
            value,
            "Should retrieve large value correctly"
        );
    }

    #[test]
    fn test_random_operations2() {
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
                    map.upsert(&key, &value).unwrap();
                    data.insert(key.clone(), value.clone());
                    inserted_keys.push(key); // Add key to the list of valid removal candidates
                }
                1 => {
                    // Update
                    if let Some(v) = data.get_mut(&key) {
                        *v = value.clone();
                        map.update(&key, &value).unwrap();
                    }
                }
                2 => {
                    // Get
                    let expected = data.get(&key);
                    match expected {
                        Some(v) => {
                            assert_eq!(
                                map.get(&key).unwrap(),
                                v.clone(),
                                "Mismatched values on get"
                            );
                        }
                        None => {
                            assert!(
                                map.get(&key).is_err(),
                                "Get should return an error if key is not in data"
                            );
                        }
                    }
                    // assert_eq!(map.get(&key).unwrap(), expected.cloned().unwrap(), "Mismatched values on get");
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
                        map.get(&remove_key).is_err(),
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

        for i in 0..(DEFAULT_BUCKET_NUM * 10) {
            // Insert more than the default number of buckets
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            map.upsert(&key, &value).unwrap();
            last_key = key.clone();

            // Check insertion correctness
            assert_eq!(
                map.get(&key).unwrap(),
                value,
                "Each inserted key should retrieve correctly"
            );
        }

        // Ensure the last inserted key is still retrievable, implying multi-page handling
        assert!(
            map.get(&last_key).is_ok(),
            "The last key should still be retrievable, indicating multi-page handling"
        );
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
