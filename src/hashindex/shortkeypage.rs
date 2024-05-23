use std::mem::size_of;

use crate::page::{Page, PageId, AVAILABLE_PAGE_SIZE};
const PAGE_HEADER_SIZE: usize = 0;

pub const SHORT_KEY_PAGE_HEADER_SIZE: usize = size_of::<ShortKeyHeader>(); // 4 + 2 + 2 = 8
pub const SHORT_KEY_SLOT_SIZE: usize = size_of::<ShortKeySlot>(); // 2 + 8 + 2 = 12

pub trait ShortKeyPage {
    fn new() -> Self;
    fn init(&mut self);

    fn insert(&mut self, key: &[u8], val: &[u8]) -> (bool, Option<Vec<u8>>); // return (success, old_value)
    fn insert_with_function<F>(
        &mut self,
        key: &[u8],
        value: &[u8],
        update_fn: F,
    ) -> (bool, Option<Vec<u8>>)
    where
        F: Fn(&[u8], &[u8]) -> Vec<u8>;
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn remove(&mut self, key: &[u8]) -> Option<Vec<u8>>;

    fn encode_shortkey_header(&mut self, header: &ShortKeyHeader);
    fn decode_shortkey_header(&self) -> ShortKeyHeader;
    fn get_next_page_id(&self) -> PageId;
    fn set_next_page_id(&mut self, next_page_id: PageId);

    fn encode_shortkey_slot(&mut self, index: u16, slot: &ShortKeySlot);
    fn decode_shortkey_slot(&self, index: u16) -> ShortKeySlot;
    fn remove_shortkey_slot(&mut self, index: u16);

    fn encode_shortkey_value(&mut self, offset: usize, entry: &ShortKeyValue);
    fn decode_shortkey_value(&self, offset: usize, remain_key_len: u16) -> ShortKeyValue;

    fn compare_key(&self, key: &[u8], slot_id: u16) -> std::cmp::Ordering;
    fn binary_search<F>(&self, f: F) -> (bool, u16)
    // return (found, index)
    where
        F: Fn(u16) -> std::cmp::Ordering;
    fn slot_end_offset(&self) -> usize;
}

#[derive(Debug, Clone)]
pub struct ShortKeyHeader {
    next_page_id: PageId,  // u16 -> u32
    slot_num: u16,         // u16
    val_start_offset: u16, // u16
}

#[derive(Debug, Clone)]
pub struct ShortKeySlot {
    key_len: u16,        // u16 (key length)
    key_prefix: [u8; 8], // 8 bytes (fixed size for the key prefix)
    val_offset: u16,     // u16
}

#[derive(Debug, Clone)]
pub struct ShortKeyValue {
    remain_key: Vec<u8>, // dynamic size (remain part of actual key), if key_len > 8
    vals_len: u16,       // u16
    vals: Vec<u8>, // vector of vals (variable size), decode in the top of the page (HashTable)
}

impl ShortKeyPage for Page {
    fn new() -> Self {
        let mut page = Page::new_empty();
        let header = ShortKeyHeader {
            next_page_id: 0,
            slot_num: 0,
            val_start_offset: AVAILABLE_PAGE_SIZE as u16,
        };
        Self::encode_shortkey_header(&mut page, &header);
        page
    }

    fn init(&mut self) {
        let header = ShortKeyHeader {
            next_page_id: 0,
            slot_num: 0,
            val_start_offset: AVAILABLE_PAGE_SIZE as u16,
        };
        Self::encode_shortkey_header(self, &header);
    }

    fn slot_end_offset(&self) -> usize {
        PAGE_HEADER_SIZE
            + SHORT_KEY_PAGE_HEADER_SIZE
            + SHORT_KEY_SLOT_SIZE * self.decode_shortkey_header().slot_num as usize
    }

    fn compare_key(&self, key: &[u8], slot_id: u16) -> std::cmp::Ordering {
        let slot = self.decode_shortkey_slot(slot_id);
        let mut key_prefix = [0u8; 8];
        let copy_len = std::cmp::min(8, key.len());
        key_prefix[..copy_len].copy_from_slice(&key[..copy_len]);

        let prefix_compare = slot.key_prefix.as_slice().cmp(&key_prefix);

        match prefix_compare {
            std::cmp::Ordering::Equal => {
                if key.len() <= 8 {
                    return std::cmp::Ordering::Equal;
                }
                let value_entry =
                    self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);
                value_entry.remain_key.as_slice().cmp(&key[8..])
            }
            _ => prefix_compare,
        }
    }

    // Find the left-most key where f(key) = gte.
    // Assumes that f(key, search_key) is lt for all keys to the left of the returned index.
    // [lt, lt, lt, lt, lt, gte, gte, gte]
    //                       ^
    //                       |
    //                       return this index
    // If all keys are lt, then return the len (i.e. slot_count)
    fn binary_search<F>(&self, f: F) -> (bool, u16)
    where
        F: Fn(u16) -> std::cmp::Ordering,
    {
        let header = self.decode_shortkey_header();
        if header.slot_num == 0 {
            return (false, 0);
        }

        let mut low = 0;
        let mut high = header.slot_num - 1;

        match f(low) {
            std::cmp::Ordering::Equal => return (true, low),
            std::cmp::Ordering::Greater => return (false, 0),
            std::cmp::Ordering::Less => {}
        }

        match f(high) {
            std::cmp::Ordering::Equal => return (true, high),
            std::cmp::Ordering::Less => return (false, high + 1),
            std::cmp::Ordering::Greater => {}
        }

        // Invairant: f(high) = Gte
        while low < high {
            let mid = low + (high - low) / 2;
            match f(mid) {
                std::cmp::Ordering::Equal => return (true, mid),
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
            }
        }

        (false, low)
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> (bool, Option<Vec<u8>>) {
        let mut header = self.decode_shortkey_header();
        let (found, index) = self.binary_search(|slot_id| self.compare_key(key, slot_id));

        if found {
            let mut slot = self.decode_shortkey_slot(index);
            let mut old_value_entry =
                self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);

            if value.len() == old_value_entry.vals.len() {
                let old_vals = std::mem::replace(&mut old_value_entry.vals, value.to_vec());
                old_value_entry.vals_len = value.len() as u16;
                self.encode_shortkey_value(slot.val_offset as usize, &old_value_entry);
                return (true, Some(old_vals));
            } else {
                let remain_key_len = key.len().saturating_sub(8);
                let required_space = remain_key_len + value.len() + 2;
                if required_space > header.val_start_offset as usize - self.slot_end_offset() {
                    self.remove_shortkey_slot(index);
                    return (false, Some(old_value_entry.vals.to_vec()));
                }

                let new_val_offset =
                    header.val_start_offset as usize - (value.len() + 2 + remain_key_len);
                let new_value_entry = ShortKeyValue {
                    remain_key: old_value_entry.remain_key,
                    vals_len: value.len() as u16,
                    vals: value.to_vec(),
                };

                slot.val_offset = new_val_offset as u16;
                self.encode_shortkey_slot(index, &slot);

                self.encode_shortkey_value(new_val_offset, &new_value_entry);
                header.val_start_offset = new_val_offset as u16;
                self.encode_shortkey_header(&header);

                return (true, Some(old_value_entry.vals.to_vec()));
            }
        } else {
            let remain_key_len = key.len().saturating_sub(8);
            let required_space = SHORT_KEY_SLOT_SIZE + remain_key_len + value.len() + 2;
            if required_space > header.val_start_offset as usize - self.slot_end_offset() {
                return (false, None);
            }

            if index < header.slot_num {
                let start_pos = PAGE_HEADER_SIZE
                    + SHORT_KEY_PAGE_HEADER_SIZE
                    + index as usize * SHORT_KEY_SLOT_SIZE;
                let end_pos = PAGE_HEADER_SIZE
                    + SHORT_KEY_PAGE_HEADER_SIZE
                    + header.slot_num as usize * SHORT_KEY_SLOT_SIZE;
                self.copy_within(start_pos..end_pos, start_pos + SHORT_KEY_SLOT_SIZE);
            }

            let new_val_offset =
                header.val_start_offset as usize - (value.len() + 2 + remain_key_len);
            let new_slot = ShortKeySlot {
                key_len: key.len() as u16,
                key_prefix: {
                    let mut prefix = [0u8; 8];
                    let copy_len = std::cmp::min(8, key.len());
                    prefix[..copy_len].copy_from_slice(&key[..copy_len]);
                    prefix
                },
                val_offset: new_val_offset as u16,
            };
            self.encode_shortkey_slot(index, &new_slot);

            let remain_key = if key.len() > 8 {
                key[8..].to_vec()
            } else {
                Vec::new()
            };
            let new_value_entry = ShortKeyValue {
                remain_key,
                vals_len: value.len() as u16,
                vals: value.to_vec(),
            };
            self.encode_shortkey_value(new_val_offset, &new_value_entry);

            header.slot_num += 1;
            header.val_start_offset = new_val_offset as u16;
            self.encode_shortkey_header(&header);

            return (true, None);
        }
    }

    fn insert_with_function<F>(
        &mut self,
        key: &[u8],
        value: &[u8],
        update_fn: F,
    ) -> (bool, Option<Vec<u8>>)
    where
        F: Fn(&[u8], &[u8]) -> Vec<u8>,
    {
        let mut header = self.decode_shortkey_header();
        let (found, index) = self.binary_search(|slot_id| self.compare_key(key, slot_id));

        if found {
            let mut slot = self.decode_shortkey_slot(index);
            let mut old_value_entry =
                self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);

            let new_value = update_fn(&old_value_entry.vals, value);

            if new_value.len() == old_value_entry.vals.len() {
                let old_vals = std::mem::replace(&mut old_value_entry.vals, new_value);
                self.encode_shortkey_value(slot.val_offset as usize, &old_value_entry);
                return (true, Some(old_vals));
            } else {
                let remain_key_len = key.len().saturating_sub(8);
                let required_space = remain_key_len + new_value.len() + 2;
                if required_space > header.val_start_offset as usize - self.slot_end_offset() {
                    self.remove_shortkey_slot(index);
                    return (false, Some(old_value_entry.vals.to_vec()));
                }

                let new_val_offset =
                    header.val_start_offset as usize - (new_value.len() + 2 + remain_key_len);
                let new_value_entry = ShortKeyValue {
                    remain_key: old_value_entry.remain_key,
                    vals_len: new_value.len() as u16,
                    vals: new_value,
                };

                slot.val_offset = new_val_offset as u16;
                self.encode_shortkey_slot(index, &slot);

                self.encode_shortkey_value(new_val_offset, &new_value_entry);
                header.val_start_offset = new_val_offset as u16;
                self.encode_shortkey_header(&header);

                return (true, Some(old_value_entry.vals.to_vec()));
            }
        } else {
            let remain_key_len = key.len().saturating_sub(8);
            let required_space = SHORT_KEY_SLOT_SIZE + remain_key_len + value.len() + 2;
            if required_space > header.val_start_offset as usize - self.slot_end_offset() {
                return (false, None);
            }

            if index < header.slot_num {
                let start_pos = PAGE_HEADER_SIZE
                    + SHORT_KEY_PAGE_HEADER_SIZE
                    + index as usize * SHORT_KEY_SLOT_SIZE;
                let end_pos = PAGE_HEADER_SIZE
                    + SHORT_KEY_PAGE_HEADER_SIZE
                    + header.slot_num as usize * SHORT_KEY_SLOT_SIZE;
                self.copy_within(start_pos..end_pos, start_pos + SHORT_KEY_SLOT_SIZE);
            }

            let new_val_offset =
                header.val_start_offset as usize - (value.len() + 2 + remain_key_len);
            let new_slot = ShortKeySlot {
                key_len: key.len() as u16,
                key_prefix: {
                    let mut prefix = [0u8; 8];
                    let copy_len = std::cmp::min(8, key.len());
                    prefix[..copy_len].copy_from_slice(&key[..copy_len]);
                    prefix
                },
                val_offset: new_val_offset as u16,
            };
            self.encode_shortkey_slot(index, &new_slot);

            let remain_key = if key.len() > 8 {
                key[8..].to_vec()
            } else {
                Vec::new()
            };
            let new_value_entry = ShortKeyValue {
                remain_key,
                vals_len: value.len() as u16,
                vals: value.to_vec(),
            };
            self.encode_shortkey_value(new_val_offset, &new_value_entry);

            header.slot_num += 1;
            header.val_start_offset = new_val_offset as u16;
            self.encode_shortkey_header(&header);

            return (true, None);
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let (found, index) = self.binary_search(|slot_id| self.compare_key(key, slot_id));
        if found {
            let slot = self.decode_shortkey_slot(index);
            let value_entry = self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);
            return Some(value_entry.vals.to_vec());
        }
        None
    }

    fn remove(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let (found, index) = self.binary_search(|slot_id| self.compare_key(key, slot_id));

        if found {
            let slot = self.decode_shortkey_slot(index);
            let value_entry = self.decode_shortkey_value(slot.val_offset as usize, slot.key_len);
            let old_values = value_entry.vals.clone();

            self.remove_shortkey_slot(index);
            Some(old_values)
        } else {
            None
        }
    }

    fn encode_shortkey_header(&mut self, header: &ShortKeyHeader) {
        let offset = PAGE_HEADER_SIZE;
        self[offset..offset + 4].copy_from_slice(&header.next_page_id.to_le_bytes());
        self[offset + 4..offset + 6].copy_from_slice(&header.slot_num.to_le_bytes());
        self[offset + 6..offset + 8].copy_from_slice(&header.val_start_offset.to_le_bytes());
    }

    fn decode_shortkey_header(&self) -> ShortKeyHeader {
        let offset = PAGE_HEADER_SIZE;
        let next_page_id = u32::from_le_bytes(
            self[offset..offset + 4]
                .try_into()
                .expect("Invalid slice length"),
        );
        let slot_num = u16::from_le_bytes(
            self[offset + 4..offset + 6]
                .try_into()
                .expect("Invalid slice length"),
        );
        let val_start_offset = u16::from_le_bytes(
            self[offset + 6..offset + 8]
                .try_into()
                .expect("Invalid slice length"),
        );

        ShortKeyHeader {
            next_page_id,
            slot_num,
            val_start_offset,
        }
    }

    fn get_next_page_id(&self) -> PageId {
        self.decode_shortkey_header().next_page_id
    }

    fn set_next_page_id(&mut self, next_page_id: PageId) {
        let mut header = self.decode_shortkey_header();
        header.next_page_id = next_page_id;
        self.encode_shortkey_header(&header);
    }

    fn encode_shortkey_slot(&mut self, index: u16, slot: &ShortKeySlot) {
        let offset =
            PAGE_HEADER_SIZE + SHORT_KEY_PAGE_HEADER_SIZE + index as usize * SHORT_KEY_SLOT_SIZE;

        self[offset..offset + 2].copy_from_slice(&slot.key_len.to_le_bytes());
        self[offset + 2..offset + 10].copy_from_slice(&slot.key_prefix);
        self[offset + 10..offset + 12].copy_from_slice(&slot.val_offset.to_le_bytes());
    }

    fn decode_shortkey_slot(&self, index: u16) -> ShortKeySlot {
        let offset =
            PAGE_HEADER_SIZE + SHORT_KEY_PAGE_HEADER_SIZE + index as usize * SHORT_KEY_SLOT_SIZE;

        let key_len = u16::from_le_bytes([self[offset], self[offset + 1]]);
        let mut key_prefix = [0u8; 8];
        key_prefix.copy_from_slice(&self[offset + 2..offset + 10]);
        let val_offset = u16::from_le_bytes([self[offset + 10], self[offset + 11]]);

        ShortKeySlot {
            key_len,
            key_prefix,
            val_offset,
        }
    }

    fn remove_shortkey_slot(&mut self, index: u16) {
        let mut header = self.decode_shortkey_header();
        if index < header.slot_num - 1 {
            let start = PAGE_HEADER_SIZE
                + SHORT_KEY_PAGE_HEADER_SIZE
                + (index + 1) as usize * SHORT_KEY_SLOT_SIZE;
            let end = PAGE_HEADER_SIZE
                + SHORT_KEY_PAGE_HEADER_SIZE
                + header.slot_num as usize * SHORT_KEY_SLOT_SIZE;
            let dest = PAGE_HEADER_SIZE
                + SHORT_KEY_PAGE_HEADER_SIZE
                + index as usize * SHORT_KEY_SLOT_SIZE;

            self.copy_within(start..end, dest);
        }

        header.slot_num -= 1;
        self.encode_shortkey_header(&header);
    }

    fn encode_shortkey_value(&mut self, offset: usize, entry: &ShortKeyValue) {
        self[offset..offset + entry.remain_key.len()].copy_from_slice(&entry.remain_key);
        self[offset + entry.remain_key.len()..offset + entry.remain_key.len() + 2]
            .copy_from_slice(&entry.vals_len.to_le_bytes());
        self[offset + entry.remain_key.len() + 2
            ..offset + entry.remain_key.len() + 2 + entry.vals.len()]
            .copy_from_slice(&entry.vals);
    }

    fn decode_shortkey_value(&self, offset: usize, key_len: u16) -> ShortKeyValue {
        let remain_key_len = if key_len > 8 { key_len as usize - 8 } else { 0 };

        let remain_key = if remain_key_len > 0 {
            self[offset..offset + remain_key_len].to_vec()
        } else {
            Vec::new()
        };

        let vals_len_start = offset + remain_key_len;

        let vals_len =
            u16::from_le_bytes([self[vals_len_start], self[vals_len_start + 1]]) as usize;

        let vals_start = vals_len_start + 2;
        let vals = self[vals_start..vals_start + vals_len].to_vec();

        ShortKeyValue {
            remain_key,
            vals_len: vals_len as u16,
            vals,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    fn update_fn(old_value: &[u8], new_value: &[u8]) -> Vec<u8> {
        old_value.iter().chain(new_value.iter()).copied().collect()
    }

    #[test]
    fn test_insert_new_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key_123";
        let value = b"value_123";

        // Test insertion of a new key
        assert_eq!(page.insert(key, value), (true, None));

        // Verify the inserted key and value
        let retrieved_value = page.get(key);
        assert_eq!(retrieved_value, Some(value.to_vec()));
    }

    #[test]
    fn test_insert_duplicate_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key_456";
        let value1 = b"value_456";
        let value2 = b"new_value_456";

        // Insert the key initially
        // assert!(page.insert(key, value1).is_none());
        assert_eq!(page.insert(key, value1), (true, None));

        // Insert the same key with a new value
        let old_value = page.insert(key, value2);
        assert_eq!(old_value, (true, Some(value1.to_vec())));

        // Verify the updated value
        let retrieved_value = page.get(key);
        assert_eq!(retrieved_value, Some(value2.to_vec()));
    }

    #[test]
    fn test_insert_out_of_space() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key_789";
        let value = vec![0u8; AVAILABLE_PAGE_SIZE]; // Unrealistically large value to simulate out-of-space

        // Attempt to insert a value that's too large for the page
        assert_eq!(page.insert(key, &value), (false, None));

        // Ensure the key was not inserted
        let retrieved_value = page.get(key);
        assert!(retrieved_value.is_none());
    }

    #[test]
    fn test_insert_multiple_keys_ordering() {
        let mut page = <Page as ShortKeyPage>::new();
        let keys = vec![
            (b"alpha", b"value_alpha"),
            (b"gamma", b"value_gamma"),
            (b"betaa", b"value_betaa"),
        ];

        // Insert keys in a specific order
        for (key, value) in keys.iter() {
            page.insert(*key, *value);
        }

        // Check if keys are retrieved in sorted order
        assert_eq!(page.get(b"alpha"), Some(b"value_alpha".to_vec()));
        assert_eq!(page.get(b"betaa"), Some(b"value_betaa".to_vec()));
        assert_eq!(page.get(b"gamma"), Some(b"value_gamma".to_vec()));
    }

    #[test]
    fn test_boundary_key_inserts() {
        let mut page = <Page as ShortKeyPage>::new();
        let min_key = b"aaaaaaa";
        let max_key = b"zzzzzzz";
        let value = b"value";

        assert_eq!(page.insert(min_key, value), (true, None));
        assert_eq!(page.insert(max_key, value), (true, None));

        assert_eq!(page.get(min_key), Some(value.to_vec()));
        assert_eq!(page.get(max_key), Some(value.to_vec()));
    }

    #[test]
    fn test_consecutive_inserts_deletes() {
        let mut page = <Page as ShortKeyPage>::new();
        let keys: Vec<&[u8]> = vec![b"key1", b"key2", b"key3"];

        // Insert keys
        for key in &keys {
            assert_eq!(page.insert(key, b"value"), (true, None));
        }

        // Delete keys
        for key in &keys {
            assert!(page.remove(key).is_some());
        }

        // Check deletions
        for key in &keys {
            assert!(page.get(key).is_none());
        }

        // Re-insert same keys
        for key in &keys {
            assert_eq!(page.insert(key, b"new_value"), (true, None));
        }

        // Verify re-insertions
        for key in &keys {
            assert_eq!(page.get(key), Some(b"new_value".to_vec()));
        }
    }

    #[test]
    fn test_updates_with_varying_value_sizes() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"key";
        let small_value = b"small";
        let large_value = b"this_is_a_much_larger_value_than_before";

        assert_eq!(page.insert(key, small_value), (true, None));
        assert_eq!(
            page.insert(key, large_value),
            (true, Some(small_value.to_vec()))
        );
        assert_eq!(page.get(key), Some(large_value.to_vec()));

        // Update back to a smaller value
        assert_eq!(
            page.insert(key, small_value),
            (true, Some(large_value.to_vec()))
        );
        assert_eq!(page.get(key), Some(small_value.to_vec()));
    }

    #[test]
    fn stress_test_random_keys_and_values() {
        let mut page = <Page as ShortKeyPage>::new();
        let mut rng = rand::thread_rng();
        let mut keys_and_values = vec![];

        for _ in 0..40 {
            // Adjust the number for more intense testing
            let key: Vec<u8> = (0..8).map(|_| rng.gen_range(0x00..0xFF)).collect();
            let value: Vec<u8> = (0..50).map(|_| rng.gen_range(0x00..0xFF)).collect(); // Random length values
            keys_and_values.push((key.clone(), value.clone()));
            assert_eq!(page.insert(&key, &value), (true, None));
        }

        // Verify all keys and values
        for (key, value) in keys_and_values {
            assert_eq!(page.get(&key), Some(value));
        }
    }

    #[test]
    fn stress_test_random_keys_and_values_with_order() {
        let mut page = <Page as ShortKeyPage>::new();
        let mut rng = rand::thread_rng();
        let mut keys_and_values = vec![];

        for _ in 0..40 {
            // Adjust the number for more intense testing
            let key: Vec<u8> = (0..8).map(|_| rng.gen_range(0x00..0xFF)).collect();
            let value: Vec<u8> = (0..50).map(|_| rng.gen_range(0x00..0xFF)).collect(); // Random length values
            keys_and_values.push((key.clone(), value.clone()));
            assert_eq!(page.insert(&key, &value), (true, None));
        }

        // Sort keys_and_values by keys to check the order after insertion
        keys_and_values.sort_by(|a, b| a.0.cmp(&b.0));

        // Verify all keys and values in sorted order
        for (key, expected_value) in keys_and_values.iter() {
            assert_eq!(page.get(key), Some(expected_value.clone()));
        }

        // Verify the order of keys directly from the page
        let mut last_key = vec![];
        for i in 0..page.decode_shortkey_header().slot_num {
            let slot = page.decode_shortkey_slot(i);
            let current_key = [
                &slot.key_prefix[..],
                &page
                    .decode_shortkey_value(slot.val_offset as usize, slot.key_len)
                    .remain_key[..],
            ]
            .concat();
            if !last_key.is_empty() {
                assert!(last_key <= current_key, "Keys are not sorted correctly.");
            }
            last_key = current_key;
        }
    }

    #[test]
    fn test_order_preservation() {
        let mut page = <Page as ShortKeyPage>::new();
        let keys: Vec<&[u8]> = vec![b"delta", b"alpha", b"echo", b"bravo", b"charlie"];

        for key in keys.iter() {
            page.insert(*key, b"value");
        }

        let mut retrieved_keys = vec![];
        for i in 0..keys.len() {
            let key = page.decode_shortkey_slot(i as u16);
            // Convert the key_prefix to a String and trim null characters
            let key_string = String::from_utf8_lossy(&key.key_prefix)
                .trim_end_matches('\0')
                .to_string();
            retrieved_keys.push(key_string);
        }

        // Assert that the keys are retrieved in the expected order
        assert_eq!(
            retrieved_keys,
            vec!["alpha", "bravo", "charlie", "delta", "echo"]
        );
    }

    #[test]
    fn test_insert_with_function_new_key() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"new_key";
        let value = b"value";

        let (success, old_value) = page.insert_with_function(key, value, update_fn);
        assert!(success);
        assert_eq!(old_value, None);
        assert_eq!(page.get(key), Some(value.to_vec()));
    }

    #[test]
    fn test_insert_with_function_update_key1() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"update_key";
        let value1 = b"value1";
        let value2 = b"value2";

        page.insert(key, value1);
        let (success, old_value) = page.insert_with_function(key, value2, update_fn);
        assert!(success);
        assert_eq!(old_value, Some(value1.to_vec()));
        assert_eq!(page.get(key), Some(update_fn(value1, value2)));
    }

    #[test]
    fn test_insert_with_function_update_key2() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"update_key_diff_size";
        let value1 = b"value1";
        let value2 = b"value2_longer";

        page.insert(key, value1);
        let (success, old_value) = page.insert_with_function(key, value2, update_fn);
        assert!(success);
        assert_eq!(old_value, Some(value1.to_vec()));
        assert_eq!(page.get(key), Some(update_fn(value1, value2)));
    }

    #[test]
    fn test_insert_with_function_out_of_space() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"key";
        // key.len() <= 8, so only - 2 for the value length
        let large_value =
            vec![0u8; AVAILABLE_PAGE_SIZE - SHORT_KEY_PAGE_HEADER_SIZE - SHORT_KEY_SLOT_SIZE - 2];

        // Fill the page almost completely
        let (success, _) = page.insert(key, &large_value);
        assert!(success);

        // Try to insert another value which should fail due to lack of space
        let key2 = b"key2";
        let value2 = b"value2";
        let (success, _) = page.insert_with_function(key2, value2, update_fn);
        assert!(!success);
        assert_eq!(page.get(key2), None);
    }

    #[test]
    fn test_insert_with_function_update_existing_value() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"existing_key";
        let value1 = b"value1";
        let value2 = b"value2";

        page.insert(key, value1);
        let (success, old_value) = page.insert_with_function(key, value2, |_, new| new.to_vec());
        assert!(success);
        assert_eq!(old_value, Some(value1.to_vec()));
        assert_eq!(page.get(key), Some(value2.to_vec()));
    }

    #[test]
    fn test_insert_with_function_key_too_large() {
        let mut page = <Page as ShortKeyPage>::new();
        let large_key = vec![0u8; AVAILABLE_PAGE_SIZE];
        let value = b"value";

        let (success, _) = page.insert_with_function(&large_key, value, update_fn);
        assert!(!success);
        assert_eq!(page.get(&large_key), None);
    }

    #[test]
    fn test_insert_with_function_value_too_large() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"key";
        let large_value = vec![0u8; AVAILABLE_PAGE_SIZE];

        let (success, _) = page.insert_with_function(key, &large_value, update_fn);
        assert!(!success);
        assert_eq!(page.get(key), None);
    }

    #[test]
    fn test_insert_with_function_value_update_replaces_old_value() {
        let mut page = <Page as ShortKeyPage>::new();
        let key = b"test_key";
        let value1 = b"old_value";
        let value2 = b"new_value";

        page.insert(key, value1);
        let (success, old_value) = page.insert_with_function(key, value2, |_, new| new.to_vec());
        assert!(success);
        assert_eq!(old_value, Some(value1.to_vec()));
        assert_eq!(page.get(key), Some(value2.to_vec()));
    }

    #[test]
    fn stress_test_random_keys_and_values_with_function() {
        let mut page = <Page as ShortKeyPage>::new();
        let mut rng = rand::thread_rng();
        let mut keys_and_values = vec![];

        for _ in 0..40 {
            // Adjust the number for more intense testing
            let key: Vec<u8> = (0..8).map(|_| rng.gen_range(0x00..0xFF)).collect();
            let value: Vec<u8> = (0..50).map(|_| rng.gen_range(0x00..0xFF)).collect(); // Random length values
            keys_and_values.push((key.clone(), value.clone()));
            assert_eq!(
                page.insert_with_function(&key, &value, update_fn),
                (true, None)
            );
        }

        // Verify all keys and values
        for (key, value) in keys_and_values {
            assert_eq!(page.get(&key), Some(value));
        }
    }

    #[test]
    fn stress_test_random_keys_and_values_with_function_and_order() {
        let mut page = <Page as ShortKeyPage>::new();
        let mut rng = rand::thread_rng();
        let mut keys_and_values = vec![];

        for _ in 0..40 {
            // Adjust the number for more intense testing
            let key: Vec<u8> = (0..8).map(|_| rng.gen_range(0x00..0xFF)).collect();
            let value: Vec<u8> = (0..50).map(|_| rng.gen_range(0x00..0xFF)).collect(); // Random length values
            keys_and_values.push((key.clone(), value.clone()));
            assert_eq!(
                page.insert_with_function(&key, &value, update_fn),
                (true, None)
            );
        }

        // Sort keys_and_values by keys to check the order after insertion
        keys_and_values.sort_by(|a, b| a.0.cmp(&b.0));

        // Verify all keys and values in sorted order
        for (key, expected_value) in keys_and_values.iter() {
            assert_eq!(page.get(key), Some(expected_value.clone()));
        }

        // Verify the order of keys directly from the page
        let mut last_key = vec![];
        for i in 0..page.decode_shortkey_header().slot_num {
            let slot = page.decode_shortkey_slot(i);
            let current_key = [
                &slot.key_prefix[..],
                &page
                    .decode_shortkey_value(slot.val_offset as usize, slot.key_len)
                    .remain_key[..],
            ]
            .concat();
            if !last_key.is_empty() {
                assert!(last_key <= current_key, "Keys are not sorted correctly.");
            }
            last_key = current_key;
        }
    }

    #[test]
    fn test_order_preservation_with_function() {
        let mut page = <Page as ShortKeyPage>::new();
        let keys: Vec<&[u8]> = vec![b"delta", b"alpha", b"echo", b"bravo", b"charlie"];

        for key in keys.iter() {
            page.insert_with_function(*key, b"value", update_fn);
        }

        let mut retrieved_keys = vec![];
        for i in 0..keys.len() {
            let key = page.decode_shortkey_slot(i as u16);
            // Convert the key_prefix to a String and trim null characters
            let key_string = String::from_utf8_lossy(&key.key_prefix)
                .trim_end_matches('\0')
                .to_string();
            retrieved_keys.push(key_string);
        }

        // Assert that the keys are retrieved in the expected order
        assert_eq!(
            retrieved_keys,
            vec!["alpha", "bravo", "charlie", "delta", "echo"]
        );
    }
}
