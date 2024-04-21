use std::sync::Arc;

use crate::{
    buffer_pool::prelude::*,
    page::{Page, PageId},
    write_ahead_log::{prelude::LogRecord, LogBufferRef},
};

use super::foster_btree_page::{BTreeKey, FosterBtreePage};

pub enum TreeStatus {
    Ok,
    NotFound,
    NotInPageRange,
    Duplicate,
    NotReadyForPhysicalDelete,
    BPStatus(BPStatus),
}

impl From<BPStatus> for TreeStatus {
    fn from(status: BPStatus) -> Self {
        TreeStatus::BPStatus(status)
    }
}

pub struct FosterBtree {
    pub c_key: ContainerKey,
    pub root_key: PageKey,
    pub bp: BufferPoolRef,
    // pub wal_buffer: LogBufferRef,
}

impl FosterBtree {
    /// System transaction that allocates a new page.
    fn allocate_page(&self) -> PageKey {
        let mut foster_page = self.bp.create_new_page_for_write(self.c_key).unwrap();
        let page_key = foster_page.key().unwrap();
        // Write log
        // {
        //     let log_record = LogRecord::SysTxnAllocPage {
        //         txn_id: 0,
        //         page_id: page_key.page_id,
        //     };
        //     let lsn = self.wal_buffer.append_log(&log_record.to_bytes());
        //     foster_page.set_lsn(lsn);
        // }
        page_key
    }

    /// Move half of the slots to the foster child
    /// If this is the right-most page, then the foster child will also be the right most page.
    /// * This means that the high fence of the foster child will be the same as the high fence of this page.
    /// If this is the left-most page, then the foster child iwll *NOT* be the left-most page.
    /// * This means that the high fence of the foster child will be the same as the low fence of the foster child.
    fn split(this: &mut Page, foster_child: &mut Page) {
        if this.slot_count() - 2 < 2 {
            // -2 to exclude the low and high fences.
            // Minimum 4 slots are required to split.
            panic!("Cannot split the page with less than 2 real slots");
        }

        // This page keeps [0, mid) slots and mid key with the foster_child_page_id
        // Foster child keeps [mid, active_slot_count) slots

        let mid = this.slot_count() / 2;
        assert!(!this.is_fence(mid));

        {
            // Set the foster_child's low and high fence and foster children flag
            let mid_key: &[u8] = this.get_raw_key(mid);
            foster_child.set_low_fence(mid_key);
            if this.is_right_most() {
                // If this is the right-most page, then the foster child will also be the right most page.
                foster_child.set_high_fence(&[]);
                foster_child.set_right_most(true);
            } else {
                let high_fence = this.get_high_fence();
                foster_child.set_high_fence(high_fence.as_ref());
            }
            if this.has_foster_child() {
                // If this page has foster children, then the foster child will also have foster children.
                foster_child.set_foster_child(true);
            }
            if this.is_leaf() {
                // If this page is a leaf, then the foster child will also be a leaf.
                foster_child.set_leaf(true);
            }
            assert!(foster_child.slot_count() == 2);
        }

        {
            // Move the half of the slots to the foster child
            let recs = {
                let mut recs = Vec::new();
                for i in mid..this.high_fence_slot_id() {
                    // Does snot include the high fence
                    recs.push((this.get_raw_key(i), this.get_val(i)));
                }
                recs
            };
            let res = foster_child.insert_sorted(recs);
            assert!(res);
        }

        {
            // Remove the moved slots from this page. Do not remove the high fence. Insert the foster key with the foster_child_page_id.
            let foster_key = this.get_raw_key(mid).to_owned();
            let foster_page_id_bytes = foster_child.get_id().to_be_bytes();
            let end = this.high_fence_slot_id();
            this.remove_range(mid, end);
            this.insert(&foster_key, &foster_page_id_bytes, false);
            this.compact_space();

            #[cfg(debug_assertions)]
            {
                // Check that foster key is in the correct position.
                let foster_slot_id = this.lower_bound_slot_id(&BTreeKey::new(&foster_key));
                assert!(foster_slot_id == this.foster_child_slot_id());
            }

            // Mark that this page has foster children
            this.set_foster_child(true);
        }

        #[cfg(debug_assertions)]
        {
            this.run_consistency_checks(true);
            foster_child.run_consistency_checks(true);
        }
    }

    /// Adopt the foster child of the child page.
    /// We first check if parent is full.
    /// If not full, then the parent page adopt the foster child of the child page.
    /// The foster child of the child page is removed and becomes the child of the parent page.
    ///
    /// Parent page
    /// * insert the foster key and the foster child page id.
    ///
    /// Child page
    /// * remove the foster key from the child page.
    /// * remove the foster child flag and set the high fence to the foster key.
    fn adopt(parent: &mut Page, child: &mut Page) {
        if !child.has_foster_child() {
            panic!("The child page does not have a foster child");
        }
        let foster_child_slot_id = child.foster_child_slot_id();
        let foster_key = child.get_foster_key().to_owned();
        let foster_child_page_id = child.get_foster_page_id();

        // Try insert into parent page.
        let inserted = parent.insert(&foster_key, &foster_child_page_id.to_be_bytes(), false);

        if inserted {
            // Remove the foster key from the child page.
            child.remove_at(foster_child_slot_id);
            child.set_foster_child(false);
            child.set_high_fence(foster_key.as_ref());
        } else {
            // Need to split the parent page.
            todo!("Need to split the parent page");
        }
    }

    pub fn create_new(
        txn_id: u64,
        c_key: ContainerKey,
        bp: BufferPoolRef,
        // wal_buffer: LogBufferRef,
    ) -> Self {
        // Create a root page
        let root_key = {
            let mut root_page = bp.create_new_page_for_write(c_key).unwrap();
            root_page.init_as_root();
            let root_key = root_page.key().unwrap();
            // Write log
            // {
            //     let log_record = LogRecord::SysTxnAllocPage {
            //         txn_id,
            //         page_id: root_key.page_id,
            //     };
            //     let lsn = wal_buffer.append_log(&log_record.to_bytes());
            //     root_page.set_lsn(lsn);
            // }
            root_key
        };
        FosterBtree {
            c_key,
            root_key,
            bp: bp.clone(),
            // wal_buffer,
        }
    }

    fn traverse_to_leaf_for_read(&self, key: &[u8]) -> Result<FrameReadGuard, TreeStatus> {
        let mut current_page = self.bp.get_page_for_read(self.root_key)?;
        loop {
            let foster_page = &current_page;
            if foster_page.is_leaf() {
                break;
            }
            let page_key = {
                let slot_id = foster_page.lower_bound_slot_id(&BTreeKey::new(key));
                let page_id_bytes = foster_page.get_val(slot_id);
                let page_id = PageId::from_be_bytes(page_id_bytes.try_into().unwrap());
                PageKey::new(self.c_key, page_id)
            };

            let next_page = self.bp.get_page_for_read(page_key)?;
            // Now we have two locks. We need to release the lock of the current page.

            current_page = next_page;
        }
        Ok(current_page)
    }

    fn traverse_to_leaf_for_write(&self, key: &[u8]) -> Result<FrameWriteGuard, TreeStatus> {
        let mut current_page = self.bp.get_page_for_write(self.root_key)?;
        loop {
            let foster_page = &current_page;
            if foster_page.is_leaf() {
                break;
            }
            let page_key = {
                let slot_id = foster_page.lower_bound_slot_id(&BTreeKey::new(key));
                let page_id_bytes = foster_page.get_val(slot_id);
                let page_id = PageId::from_be_bytes(page_id_bytes.try_into().unwrap());
                PageKey::new(self.c_key, page_id)
            };

            let next_page = self.bp.get_page_for_write(page_key)?;
            // Check if there is foster child in the next page.
            if next_page.has_foster_child() {
                let new_page_key = self.allocate_page();
                let new_page = self.bp.get_page_for_write(new_page_key).unwrap();
            }

            // Now we have two locks. We need to release the lock of the current page.

            current_page = next_page;
        }
        Ok(current_page)
    }

    pub fn get_key(&self, key: &[u8]) -> Result<Vec<u8>, TreeStatus> {
        let foster_page = self.traverse_to_leaf_for_read(key)?;
        let slot_id = foster_page.lower_bound_slot_id(&BTreeKey::new(key));
        if foster_page.get_btree_key(slot_id) == BTreeKey::new(key) {
            // Exact match
            if foster_page.is_ghost_slot(slot_id) {
                Err(TreeStatus::NotFound)
            } else {
                Ok(foster_page.get_val(slot_id).to_vec())
            }
        } else {
            // Non-existent key
            Err(TreeStatus::NotFound)
        }
    }

    /*
    pub fn insert_key(&self, key: &[u8], value: &[u8]) -> Result<(), TreeStatus> {
        let mut foster_page = self.traverse_to_leaf_for_write(key)?; // Hold X latch on the page
        let rec = foster_page.lower_bound_rec(key).ok_or(TreeStatus::NotInPageRange)?;
        if rec.key == key {
            // Exact match
            if !rec.is_ghost {
                return Err(TreeStatus::Duplicate)
            } else {
                // Replace ghost record
                foster_page.replace_ghost(key, value);
            }
        } else {
            foster_page = {
                // System transaction
                // * Tries to insert the key-value pair into the page
                // * If the page is full, then split the page and insert the key-value pair
                // * If the page is split, then the parent page is also split
                let inserted = foster_page.insert(key, value, true);
                if !inserted {
                    // Split the page
                    let (new_page, new_key) = foster_page.split();
                    // Insert the new key into the parent page
                    foster_page.insert_into_parent(new_key, new_page)
                }
                foster_page
            };
            {
                foster_page.replace_ghost(key, value);
            }
        }
        Ok(())
    }

    // Logical deletion of a key
    pub fn delete_key(&self, key: &[u8]) -> Result<(), TreeStatus> {
        let mut leaf = self.traverse_to_leaf_for_write(key)?; // Hold X latch on the page
        let mut foster_page = FosterBtreePage::new(&mut *leaf);
        let rec = foster_page.lower_bound_rec(key).ok_or(TreeStatus::NotInPageRange)?;
        if rec.key == key {
            // Exact match
            if rec.is_ghost {
                Err(TreeStatus::NotFound)
            } else {
                // Logical deletion
                foster_page.mark_ghost(key);
                Ok(())
            }
        } else {
            // Non-existent key
            Err(TreeStatus::NotFound)
        }
    }

    pub fn update_key(&self, key: &[u8], value: &[u8]) -> Result<(), TreeStatus> {
        let mut leaf = self.traverse_to_leaf_for_write(key)?; // Hold X latch on the page
        let mut foster_page = FosterBtreePage::new(&mut *leaf);
        let rec = foster_page.lower_bound_rec(key).ok_or(TreeStatus::NotInPageRange)?;
        if rec.key == key {
            // Exact match
            if rec.is_ghost {
                Err(TreeStatus::NotFound)
            } else {
                // Update the value
                foster_page.update(key, value);
                Ok(())
            }
        } else {
            // Non-existent key
            Err(TreeStatus::NotFound)
        }
    }

    pub fn physical_delete_key(&self, key: &[u8]) -> Result<(), TreeStatus> {
        let mut leaf = self.traverse_to_leaf_for_write(key)?; // Hold X latch on the page
        let mut foster_page = FosterBtreePage::new(&mut *leaf);
        let rec = foster_page.lower_bound_rec(key).ok_or(TreeStatus::NotInPageRange)?;
        if rec.key == key {
            // Exact match
            if rec.is_ghost {
                // Check lock conflicts and if there is no physical conflict, then delete the key
                {
                    // System transaction
                    foster_page.remove(key);
                }
                Ok(())
            } else {
                Err(TreeStatus::NotReadyForPhysicalDelete)
            }
        } else {
            // Non-existent key
            Err(TreeStatus::NotFound)
        }
    }
    */
}

mod tests {
    use core::num;
    use std::collections::btree_map::Keys;

    use tempfile::TempDir;

    use crate::{buffer_pool::CacheEvictionPolicy, page::Page};

    use super::{BufferPool, BufferPoolRef, ContainerKey, FosterBtree, FosterBtreePage};

    fn get_buffer_pool(db_id: u16) -> (TempDir, BufferPoolRef) {
        let temp_dir = TempDir::new().unwrap();
        // create a directory for the database
        std::fs::create_dir(temp_dir.path().join(db_id.to_string())).unwrap();
        let num_frames = 10;
        let ep = CacheEvictionPolicy::new(num_frames);
        let bp = BufferPoolRef::new(BufferPool::new(temp_dir.path(), num_frames, ep).unwrap());
        (temp_dir, bp)
    }

    fn get_btree(db_id: u16) -> (TempDir, FosterBtree) {
        let (temp_dir, bp) = get_buffer_pool(db_id);
        let c_key = ContainerKey::new(db_id, 0);
        let txn_id = 0;
        let btree = FosterBtree::create_new(txn_id, c_key, bp.clone());
        (temp_dir, btree)
    }

    fn to_bytes(num: usize) -> Vec<u8> {
        num.to_be_bytes().to_vec()
    }

    fn get_kvs(range: std::ops::Range<usize>) -> Vec<(Vec<u8>, Vec<u8>)> {
        range.map(|i| (to_bytes(i), to_bytes(i))).collect()
    }

    fn print_page(p: &Page) {
        println!(
            "----------------- Page ID: {} -----------------",
            p.get_id()
        );
        println!("Low fence: {:?}", p.get_low_fence());
        println!("High fence: {:?}", p.get_high_fence());
        println!("Slot count: {}", p.slot_count());
        println!("Active slot count: {}", p.active_slot_count());
        println!("Foster child: {}", p.has_foster_child());
        for i in 0..p.active_slot_count() {
            let key = p.get_raw_key((i + 1) as u16);
            let val = p.get_val((i + 1) as u16);
            let key = usize::from_be_bytes(key.try_into().unwrap());
            let val = if p.has_foster_child() && i == p.active_slot_count() - 1 {
                let page_id = u32::from_be_bytes(val.try_into().unwrap()).to_string();
                format!("PageID({})", page_id)
            } else {
                usize::from_be_bytes(val.try_into().unwrap()).to_string()
            };
            println!("Slot {}: Key: {}, Value: {}", i + 1, key, val);
        }
        println!("----------------------------------------------");
    }

    #[test]
    fn test_page_setup() {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let (temp_dir, bp) = get_buffer_pool(db_id);
        {
            let p_key = bp.create_new_page_for_write(c_key).unwrap().key().unwrap();
            let mut p = bp.get_page_for_write(p_key).unwrap();
            p.init();
            let low_fence = to_bytes(0);
            let high_fence = to_bytes(20);
            p.set_low_fence(&low_fence);
            p.set_high_fence(&high_fence);
            assert_eq!(p.get_low_fence().as_ref(), low_fence);
            assert_eq!(p.get_high_fence().as_ref(), high_fence);
            print_page(&p);
        }
        drop(temp_dir);
    }

    #[test]
    fn test_page_split() {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);

        let low_fence = to_bytes(0);
        let high_fence = to_bytes(20);
        let kvs = get_kvs(0..10);

        let (temp_dir, bp) = get_buffer_pool(db_id);
        let mut p0 = bp.create_new_page_for_write(c_key).unwrap();
        p0.init();
        p0.set_low_fence(&low_fence);
        p0.set_high_fence(&high_fence);

        let mut p1 = bp.create_new_page_for_write(c_key).unwrap();
        p1.init();

        let mut p2 = bp.create_new_page_for_write(c_key).unwrap();
        p2.init();

        // Insert 10 keys into p0
        p0.insert_sorted(kvs.clone());
        assert_eq!(p0.active_slot_count(), 10);

        {
            // Split p0 into p0 and p1
            FosterBtree::split(&mut p0, &mut p1);

            // Check the consistency of p0 and p1
            p0.run_consistency_checks(true);
            p1.run_consistency_checks(true);
            // Check the contents of p0
            // p0 has 0, 1, 2, 3, 4, and foster key 5
            assert_eq!(p0.active_slot_count(), 6); // 5 real slots + 1 foster key
            assert!(p0.has_foster_child());
            assert_eq!(p0.get_foster_page_id(), p1.get_id());
            for i in 0..5 as usize {
                let key = p0.get_raw_key((i + 1) as u16);
                assert_eq!(key, kvs[i].0);
            }
            // Check the contents of p1
            // p1 has 5, 6, 7, 8, 9
            assert_eq!(p1.active_slot_count(), 5); // 5 real slots
            assert!(!p1.has_foster_child());
            for i in 0..5 as usize {
                let key = p1.get_raw_key((i + 1) as u16);
                assert_eq!(key, kvs[i + 5].0);
            }
            // p1's low fence should be the mid key
            assert_eq!(p1.get_low_fence().as_ref(), kvs[5].0);
            // p1's high fence should be the same as p0's high fence
            assert_eq!(p0.get_high_fence(), p1.get_high_fence());

            print_page(&p0);
            print_page(&p1);
        }

        {
            // Split p0 into p0 and p2
            FosterBtree::split(&mut p0, &mut p2);

            // Check the consistency of p0 and p2
            p0.run_consistency_checks(true);
            p2.run_consistency_checks(true);
            // Check the contents of p0
            // p0 has 0, 1, 2, and foster key 3
            assert_eq!(p0.active_slot_count(), 4); // 3 real slots + 1 foster key
            assert!(p0.has_foster_child());
            assert_eq!(p0.get_foster_page_id(), p2.get_id());
            for i in 0..3 as usize {
                let key = p0.get_raw_key((i + 1) as u16);
                assert_eq!(key, kvs[i].0);
            }
            // Check the contents of p2
            // p2 has 3, 4, and foster key 5
            assert_eq!(p2.active_slot_count(), 3); // 2 real slots + 1 foster key
            assert!(p2.has_foster_child());
            assert_eq!(p2.get_foster_page_id(), p1.get_id());
            for i in 0..2 as usize {
                let key = p2.get_raw_key((i + 1) as u16);
                assert_eq!(key, kvs[i + 3].0);
            }
            // p2's low fence should be the mid key
            assert_eq!(p2.get_low_fence().as_ref(), kvs[3].0);
            // p2's high fence should be the same as p0's high fence
            assert_eq!(p0.get_high_fence(), p2.get_high_fence());

            print_page(&p0);
            print_page(&p2);
            print_page(&p1);
        }

        drop(temp_dir);
    }

    #[test]
    fn test_page_adopt() {
        // One parent page. One child page with foster child.
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let (temp_dir, bp) = get_buffer_pool(db_id);
        let mut parent = bp.create_new_page_for_write(c_key).unwrap();
        let mut child0 = bp.create_new_page_for_write(c_key).unwrap();
        let mut child1 = bp.create_new_page_for_write(c_key).unwrap();

        // Before:
        //   parent [k0, k2)
        //    |
        //    v
        //   child0 [k0, k2) --> child1 [k1, k2)
        //
        // After:
        //   parent [k0, k2)
        //    +-------------------+
        //    |                   |
        //    v                   v
        //   child0 [k0, k1)    child1 [k1, k2)

        let k0 = to_bytes(10);
        let k1 = to_bytes(20);
        let k2 = to_bytes(30);

        parent.init();
        parent.set_low_fence(&k0);
        parent.set_high_fence(&k2);
        parent.insert(&k0, &child0.get_id().to_be_bytes(), false);
        parent.set_leaf(false);

        child0.init();
        child0.set_low_fence(&k0);
        child0.set_high_fence(&k2);
        // Insert 10 slots
        for i in 0..10 {
            let key = to_bytes(i + 10);
            child0.insert(&key, &key, false);
        }
        child0.set_foster_child(true);
        child0.insert(&k1, &child1.get_id().to_be_bytes(), false);

        child1.init();
        child1.set_low_fence(&k1);
        child1.set_high_fence(&k2);
        // Insert 10 slots
        for i in 0..10 {
            let key = to_bytes(i + 20);
            child1.insert(&key, &key, false);
        }

        // Run consistency checks
        parent.run_consistency_checks(false);
        child0.run_consistency_checks(false);
        child1.run_consistency_checks(false);
        assert_eq!(parent.active_slot_count(), 1);
        assert_eq!(child0.active_slot_count(), 11);
        assert_eq!(child1.active_slot_count(), 10);
        assert_eq!(parent.get_val(1), child0.get_id().to_be_bytes());
        assert!(child0.has_foster_child());
        assert_eq!(child0.get_foster_key(), k1);
        assert_eq!(child0.get_foster_page_id(), child1.get_id());

        FosterBtree::adopt(&mut parent, &mut child0);

        // Run consistency checks
        parent.run_consistency_checks(false);
        child0.run_consistency_checks(false);
        child1.run_consistency_checks(false);
        assert_eq!(parent.active_slot_count(), 2);
        assert_eq!(child0.active_slot_count(), 10);
        assert_eq!(child1.active_slot_count(), 10);
        assert_eq!(parent.get_val(1), child0.get_id().to_be_bytes());
        assert_eq!(parent.get_val(2), child1.get_id().to_be_bytes());
        assert_eq!(child0.has_foster_child(), false);

        drop(temp_dir);
    }
}
