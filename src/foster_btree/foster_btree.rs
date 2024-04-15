use std::sync::Arc;

use crate::{
    buffer_pool::prelude::*,
    page::{Page, PageId},
    write_ahead_log::{prelude::LogRecord, LogBufferRef},
};

use super::foster_btree_page::FosterBtreePage;

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
    pub wal_buffer: LogBufferRef,
}

impl FosterBtree {
    pub fn create_new(
        txn_id: u64,
        c_key: ContainerKey,
        bp: BufferPoolRef,
        wal_buffer: LogBufferRef,
    ) -> Self {
        // Create a root page
        let root_key = {
            let mut root_page = bp.create_new_page_for_write(c_key).unwrap();
            root_page.init_as_root();
            let root_key = root_page.key().unwrap();
            // Write log
            {
                let log_record = LogRecord::SysTxnAllocPage {
                    txn_id,
                    page_id: root_key.page_id,
                };
                let lsn = wal_buffer.append_log(&log_record.to_bytes());
                root_page.set_lsn(lsn);
            }
            root_key
        };
        FosterBtree {
            c_key,
            root_key,
            bp: bp.clone(),
            wal_buffer,
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
                let page_id_bytes = foster_page
                    .lower_bound(key)
                    .ok_or(TreeStatus::NotInPageRange)?;
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
                let page_id_bytes = foster_page
                    .lower_bound(key)
                    .ok_or(TreeStatus::NotInPageRange)?;
                let page_id = PageId::from_be_bytes(page_id_bytes.try_into().unwrap());
                PageKey::new(self.c_key, page_id)
            };

            let next_page = self.bp.get_page_for_write(page_key)?;
            // Now we have two locks. We need to release the lock of the current page.

            current_page = next_page;
        }
        Ok(current_page)
    }

    pub fn get_key(&self, key: &[u8]) -> Result<Vec<u8>, TreeStatus> {
        let leaf = self.traverse_to_leaf_for_read(key)?;
        let foster_page = leaf;
        let rec = foster_page
            .lower_bound_rec(key)
            .ok_or(TreeStatus::NotInPageRange)?;
        if rec.key == key {
            // Exact match
            if rec.is_ghost {
                Err(TreeStatus::NotFound)
            } else {
                Ok(rec.value.to_vec())
            }
        } else {
            // Non-existent key
            Err(TreeStatus::NotFound)
        }
    }

    /*
    pub fn insert_key(&self, key: &[u8], value: &[u8]) -> Result<(), TreeStatus> {
        let mut leaf = self.traverse_to_leaf_for_write(key)?; // Hold X latch on the page
        let mut foster_page = FosterBtreePage::new(&mut *leaf);
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

mod tests {}
