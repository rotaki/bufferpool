use crate::page::Page;

// Page layout:
// 1 byte: flags (is_root, is_leaf, leftmost, rightmost, has_foster_children)
// 2 byte: active slot count (generally >=2  because of low and high fences)
// 2 byte: free space
// Slotted page layout:
// * slot_metadata [offset: u16, key_size: u16, value_size: u16]. The first bit of the offset is used to indicate if the slot is a ghost slot.
//  The slot metadata should be sorted based on the key.
// * slot_data [key: [u8], value: [u8]] // value should be a page id if the page is a non-leaf page, otherwise it should be a value.
// The first slot is the low fence and the last slot is the high fence.

// Assumptions
// * Keys are unique

// [slot0] -- low_fence. If the page is the leftmost page, then the low_fence is offset 0, size 0. Should not be referenced.
// [slotN] -- high_fence. If the page is the rightmost page, then the high_fence is offset 0, size 0. Should not be referenced.

mod page_header {
    pub const PAGE_HEADER_SIZE: usize = 5;

    pub struct PageHeader {
        flags: u8,
        active_slot_count: u16,
        slot_data_start_offset: u16,
    }

    impl PageHeader {
        pub fn from_bytes(bytes: &[u8; 5]) -> Self {
            let flags = bytes[0];
            let active_slot_count = u16::from_be_bytes([bytes[1], bytes[2]]);
            let slot_data_start_offset = u16::from_be_bytes([bytes[3], bytes[4]]);
            PageHeader {
                flags,
                active_slot_count,
                slot_data_start_offset,
            }
        }

        pub fn to_bytes(&self) -> [u8; 5] {
            let active_slot_count_bytes = self.active_slot_count.to_be_bytes();
            let slot_data_start_offset_bytes = self.slot_data_start_offset.to_be_bytes();
            [
                self.flags,
                active_slot_count_bytes[0],
                active_slot_count_bytes[1],
                slot_data_start_offset_bytes[0],
                slot_data_start_offset_bytes[1],
            ]
        }

        pub fn new(slot_data_start_offset: u16) -> Self {
            PageHeader {
                flags: 0,
                active_slot_count: 0,
                slot_data_start_offset,
            }
        }

        pub fn is_root(&self) -> bool {
            self.flags & 0b1000_0000 != 0
        }

        pub fn set_root(&mut self, is_root: bool) {
            if is_root {
                self.flags |= 0b1000_0000;
            } else {
                self.flags &= 0b0111_1111;
            }
        }

        pub fn is_leaf(&self) -> bool {
            self.flags & 0b0100_0000 != 0
        }

        pub fn set_leaf(&mut self, is_leaf: bool) {
            if is_leaf {
                self.flags |= 0b0100_0000;
            } else {
                self.flags &= 0b1011_1111;
            }
        }

        pub fn is_left_most(&self) -> bool {
            self.flags & 0b0010_0000 != 0
        }

        pub fn set_left_most(&mut self, is_left_most: bool) {
            if is_left_most {
                self.flags |= 0b0010_0000;
            } else {
                self.flags &= 0b1101_1111;
            }
        }

        pub fn is_right_most(&self) -> bool {
            self.flags & 0b0001_0000 != 0
        }

        pub fn set_right_most(&mut self, is_right_most: bool) {
            if is_right_most {
                self.flags |= 0b0001_0000;
            } else {
                self.flags &= 0b1110_1111;
            }
        }

        pub fn has_foster_children(&self) -> bool {
            self.flags & 0b0000_1000 != 0
        }

        pub fn set_foster_children(&mut self, has_foster_children: bool) {
            if has_foster_children {
                self.flags |= 0b0000_1000;
            } else {
                self.flags &= 0b1111_0111;
            }
        }

        pub fn active_slot_count(&self) -> u16 {
            self.active_slot_count
        }

        pub fn increment_active_slots(&mut self) {
            self.active_slot_count += 1;
        }

        pub fn decrement_active_slots(&mut self) {
            self.active_slot_count -= 1;
        }

        pub fn slot_data_start_offset(&self) -> u16 {
            self.slot_data_start_offset
        }

        pub fn set_slot_data_start_offset(&mut self, slot_data_start_offset: u16) {
            self.slot_data_start_offset = slot_data_start_offset;
        }
    }
}

mod slot_metadata {
    pub const SLOT_METADATA_SIZE: usize = 6;

    pub struct SlotMetadata {
        offset: u16, // The first bit of the offset is used to indicate if the slot is a ghost slot.
        key_size: u16, // The size of the key
        value_size: u16, // The size of the value
    }

    impl SlotMetadata {
        pub fn from_bytes(bytes: [u8; SLOT_METADATA_SIZE]) -> Self {
            let offset = u16::from_be_bytes([bytes[0], bytes[1]]);
            let key_size = u16::from_be_bytes([bytes[2], bytes[3]]);
            let value_size = u16::from_be_bytes([bytes[4], bytes[5]]);
            SlotMetadata {
                offset,
                key_size,
                value_size,
            }
        }

        pub fn to_bytes(&self) -> [u8; SLOT_METADATA_SIZE] {
            let offset_bytes = self.offset.to_be_bytes();
            let key_size_bytes = self.key_size.to_be_bytes();
            let value_size_bytes = self.value_size.to_be_bytes();
            [
                offset_bytes[0],
                offset_bytes[1],
                key_size_bytes[0],
                key_size_bytes[1],
                value_size_bytes[0],
                value_size_bytes[1],
            ]
        }

        pub fn new(offset: u16, key_size: u16, value_size: u16) -> Self {
            // Always clear the first bit of the offset.
            // i.e. Always assume that the slot is not a ghost slot on new.
            let offset = offset & 0b0111_1111_1111_1111;
            SlotMetadata {
                offset,
                key_size,
                value_size,
            }
        }

        pub fn is_ghost(&self) -> bool {
            self.offset & 0b1000_0000_0000_0000 != 0
        }

        pub fn set_ghost(&mut self, is_ghost: bool) {
            if is_ghost {
                self.offset |= 0b1000_0000_0000_0000;
            } else {
                self.offset &= 0b0111_1111_1111_1111;
            }
        }

        pub fn offset(&self) -> u16 {
            self.offset & 0b0111_1111_1111_1111
        }

        pub fn set_offset(&mut self, offset: u16) {
            self.offset = (self.offset & 0b1000_0000_0000_0000) | (offset & 0b0111_1111_1111_1111);
        }

        pub fn key_size(&self) -> u16 {
            self.key_size
        }

        pub fn set_key_size(&mut self, key_size: u16) {
            self.key_size = key_size;
        }

        pub fn value_size(&self) -> u16 {
            self.value_size
        }

        pub fn set_value_size(&mut self, value_size: u16) {
            self.value_size = value_size;
        }
    }
}

use log::trace;
use page_header::{PageHeader, PAGE_HEADER_SIZE};
use slot_metadata::{SlotMetadata, SLOT_METADATA_SIZE};

pub struct FosterBtreePage<'a> {
    page: &'a mut Page,
}

// Private methods
impl<'a> FosterBtreePage<'a> {
    fn header(&self) -> PageHeader {
        let header_bytes: &[u8; PAGE_HEADER_SIZE] =
            &self.page[0..PAGE_HEADER_SIZE].try_into().unwrap();
        PageHeader::from_bytes(header_bytes)
    }

    fn update_header(&mut self, header: PageHeader) {
        self.page[0..PAGE_HEADER_SIZE].copy_from_slice(&header.to_bytes());
    }

    fn slot_metadata_offset(slot_id: u16) -> usize {
        PAGE_HEADER_SIZE + slot_id as usize * SLOT_METADATA_SIZE
    }

    fn slot_metadata(&self, slot_id: u16) -> Option<SlotMetadata> {
        if slot_id < self.header().active_slot_count() {
            let offset = FosterBtreePage::slot_metadata_offset(slot_id);
            let slot_metadata_bytes: [u8; SLOT_METADATA_SIZE] = self.page
                [offset..offset + SLOT_METADATA_SIZE]
                .try_into()
                .unwrap();
            Some(SlotMetadata::from_bytes(slot_metadata_bytes))
        } else {
            None
        }
    }

    fn insert_slot_metadata(&mut self, key_size: u16, value_size: u16) -> (u16, SlotMetadata) {
        let mut page_header = self.header();
        let slot_id = page_header.active_slot_count();
        page_header.increment_active_slots();
        let slot_offset = page_header.slot_data_start_offset() - key_size - value_size;
        page_header.set_slot_data_start_offset(slot_offset);
        self.update_header(page_header);

        let slot_metadata = SlotMetadata::new(slot_offset, key_size, value_size);
        let res = self.update_slot_metadata(slot_id, &slot_metadata);
        assert!(res);
        (slot_id, slot_metadata)
    }

    fn update_slot_metadata(&mut self, slot_id: u16, slot_metadata: &SlotMetadata) -> bool {
        if slot_id < self.header().active_slot_count() {
            let offset = FosterBtreePage::slot_metadata_offset(slot_id);
            self.page[offset..offset + SLOT_METADATA_SIZE]
                .copy_from_slice(&slot_metadata.to_bytes());
            true
        } else {
            false
        }
    }

    // Returns the first ghost slot if it exists.
    fn ghost_slot(&self) -> Option<(u16, SlotMetadata)> {
        for i in 0..self.header().active_slot_count() {
            let slot_metadata = self.slot_metadata(i).unwrap();
            if slot_metadata.is_ghost() {
                return Some((i, slot_metadata));
            }
        }
        None
    }

    fn free_space(&self) -> usize {
        let next_slot_metadata_offset =
            FosterBtreePage::slot_metadata_offset(self.header().active_slot_count());
        let slot_data_start_offset = self.header().slot_data_start_offset();
        slot_data_start_offset as usize - next_slot_metadata_offset
    }

    // [ [slot4][slot3][slot2][slot1] ]
    //   ^             ^     ^
    //   |             |     |
    //   slot_data_start_offset
    //                 |     |
    //                 shift_start_offset
    //                       |
    //                  <----> shift_size
    //    <------------> data to be shifted
    //
    // Delete slot2. Shift [slot4][slot3] to the right by slot2.size
    //
    // [        [slot4][slot3][slot1] ]
    //
    // The left offset of slot4 is `slot_data_start_offset`.
    // The left offset of slot2 is `shift_start_offset`.
    // The size of slot2 is `shift_size`.
    fn shift_slot_data(&mut self, shift_start_offset: u16, shift_size: u16) {
        // Chunks of data to be shifted is in the range of [start..end)
        // The new range is [new_start..new_end)
        trace!(
            "Shifting slot data. Start offset: {}, size: {}",
            shift_start_offset,
            shift_size
        );
        let start = self.header().slot_data_start_offset() as usize;
        let end = shift_start_offset as usize;
        // No need to shift if start >= end OR shift_size == 0
        if start >= end || shift_size == 0 {
            return;
        }
        let data = self.page[start..end].to_vec();

        let new_start = start + shift_size as usize;
        let new_end = end + shift_size as usize;
        self.page[new_start..new_end].copy_from_slice(&data);

        // For each slot shifted, update the slot metadata
        // Shifting includes the ghost slots.
        for slot_id in 0..self.header().active_slot_count() {
            if let Some(mut slot_metadata) = self.slot_metadata(slot_id) {
                let current_offset = slot_metadata.offset();
                if current_offset < shift_start_offset as u16 {
                    // Update slot metadata
                    let new_offset = current_offset + shift_size;
                    slot_metadata.set_offset(new_offset);
                    self.update_slot_metadata(slot_id, &slot_metadata);
                }
            } else {
                panic!("Slot metadata should be available");
            }
        }

        // Update the slot_data_start_offset of the page
        let mut page_header = self.header();
        page_header.set_slot_data_start_offset(new_start as u16);
        self.update_header(page_header);
    }

    // Shift the slot metadata to the right by shift_size.
    // [ [slotmeta1][slotmeta2][slotmeta3] ]
    //
    //
    // Want to insert a new slot at slotmeta2.
    // Need to shift slotmeta2 and slotmeta3 to the right by 1 SLOT_METADATA_SIZE.
    //
    // [ [slotmeta1][slotmeta2'][slotmeta2][slotmeta3] ]
    //
    // This function implicitly increments the active_slot_count of the page.
    fn shift_slot_meta(&mut self, slot_id: u16) {
        let start = FosterBtreePage::slot_metadata_offset(slot_id);
        let end = FosterBtreePage::slot_metadata_offset(self.header().active_slot_count());
        if start > end {
            panic!("Slot metadata does not exist at the given slot_id");
        } else if start == end {
            // No need to shift if start == end. Just add a new slot metadata at the end.
            let (new_slot_id, _) = self.insert_slot_metadata(0, 0);
            assert!(new_slot_id == slot_id);
        } else {
            let data = self.page[start..end].to_vec();

            let new_start = start + SLOT_METADATA_SIZE as usize;
            let new_end = end + SLOT_METADATA_SIZE as usize;

            self.page[new_start..new_end].copy_from_slice(&data);

            // Update the active_slot_count of the page
            let mut header = self.header();
            header.increment_active_slots();
            self.update_header(header);
        }
    }

    // Find the left-most key where f(key) = true.
    // Assumes that f(key, search_key) is false for all keys to the left of the returned index.
    // [false, false, false, true, true, true]
    //                        ^
    //                        |
    //                        return this index
    // If all keys are false, then return the len (i.e. active_slot_count)
    fn linear_search<F>(&self, mut f: F) -> u16
    where
        F: Fn(KeyInternal) -> bool,
    {
        for i in 0..self.header().active_slot_count() {
            let slot_key = self.get_slot_key(i).unwrap();
            if f(slot_key) {
                return i;
            }
        }
        self.header().active_slot_count()
    }

    // Find the left-most key where f(key) = true.
    // Assumes that f(key, search_key) is false for all keys to the left of the returned index.
    // [false, false, false, true, true, true]
    //                        ^
    //                        |
    //                        return this index
    // If all keys are false, then return the len (i.e. active_slot_count)
    fn binary_search<F>(&self, f: F) -> u16
    where
        F: Fn(KeyInternal) -> bool,
    {
        let low_fence = self.get_slot_key(0).unwrap();
        let mut ng = if !f(low_fence) {
            0
        } else {
            return 0;
        };
        let high_fence = self
            .get_slot_key(self.header().active_slot_count() - 1)
            .unwrap();
        let mut ok = if f(high_fence) {
            self.header().active_slot_count() - 1
        } else {
            return self.header().active_slot_count();
        };

        // Invariant: f(ng) = false, f(ok) = true
        while ok - ng > 1 {
            let mid = ng + (ok - ng) / 2;
            let slot_key = self.get_slot_key(mid).unwrap();
            if f(slot_key) {
                ok = mid;
            } else {
                ng = mid;
            }
        }
        ok
    }
}

enum KeyInternal<'a> {
    MinusInfty,
    Normal(&'a [u8]),
    PlusInfty,
}

impl<'a> PartialEq for KeyInternal<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (KeyInternal::MinusInfty, KeyInternal::MinusInfty) => true,
            (KeyInternal::MinusInfty, KeyInternal::Normal(_)) => false,
            (KeyInternal::MinusInfty, KeyInternal::PlusInfty) => false,
            (KeyInternal::Normal(key1), KeyInternal::Normal(key2)) => key1 == key2,
            (KeyInternal::Normal(_), KeyInternal::MinusInfty) => false,
            (KeyInternal::Normal(_), KeyInternal::PlusInfty) => false,
            (KeyInternal::PlusInfty, KeyInternal::MinusInfty) => false,
            (KeyInternal::PlusInfty, KeyInternal::Normal(_)) => false,
            (KeyInternal::PlusInfty, KeyInternal::PlusInfty) => true,
        }
    }
}

impl Eq for KeyInternal<'_> {}

impl<'a> PartialOrd for KeyInternal<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (KeyInternal::MinusInfty, KeyInternal::MinusInfty) => Some(std::cmp::Ordering::Equal),
            (KeyInternal::MinusInfty, KeyInternal::Normal(_)) => Some(std::cmp::Ordering::Less),
            (KeyInternal::MinusInfty, KeyInternal::PlusInfty) => Some(std::cmp::Ordering::Less),
            (KeyInternal::Normal(_), KeyInternal::MinusInfty) => Some(std::cmp::Ordering::Greater),
            (KeyInternal::Normal(key1), KeyInternal::Normal(key2)) => Some(key1.cmp(key2)),
            (KeyInternal::Normal(_), KeyInternal::PlusInfty) => Some(std::cmp::Ordering::Less),
            (KeyInternal::PlusInfty, KeyInternal::MinusInfty) => Some(std::cmp::Ordering::Greater),
            (KeyInternal::PlusInfty, KeyInternal::Normal(_)) => Some(std::cmp::Ordering::Greater),
            (KeyInternal::PlusInfty, KeyInternal::PlusInfty) => Some(std::cmp::Ordering::Equal),
        }
    }
}

impl Ord for KeyInternal<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<'a> FosterBtreePage<'a> {
    fn get_low_fence(&self) -> KeyInternal {
        if self.header().is_left_most() {
            KeyInternal::MinusInfty
        } else {
            let slot_metadata = self.slot_metadata(0).unwrap();
            let offset = slot_metadata.offset() as usize;
            let key_size = slot_metadata.key_size() as usize;
            KeyInternal::Normal(&self.page[offset..offset + key_size])
        }
    }

    fn get_high_fence(&self) -> KeyInternal {
        if self.header().is_right_most() {
            KeyInternal::PlusInfty
        } else {
            let slot_metadata = self
                .slot_metadata(self.header().active_slot_count() - 1)
                .unwrap();
            let offset = slot_metadata.offset() as usize;
            let key_size = slot_metadata.key_size() as usize;
            KeyInternal::Normal(&self.page[offset..offset + key_size])
        }
    }

    fn get_slot_key(&self, slot_id: u16) -> Option<KeyInternal> {
        if slot_id == 0 {
            Some(self.get_low_fence())
        } else if slot_id == self.header().active_slot_count() - 1 {
            Some(self.get_high_fence())
        } else if slot_id < self.header().active_slot_count() - 1 {
            let slot_metadata = self.slot_metadata(slot_id).unwrap();
            let offset = slot_metadata.offset() as usize;
            let key_size = slot_metadata.key_size() as usize;
            Some(KeyInternal::Normal(&self.page[offset..offset + key_size]))
        } else {
            None
        }
    }

    fn range(&self) -> (KeyInternal, KeyInternal) {
        let low_fence = self.get_low_fence();
        let high_fence = self.get_high_fence();
        (low_fence, high_fence)
    }

    fn is_in_range(&self, key: &[u8]) -> bool {
        let (low_fence, high_fence) = self.range();
        low_fence <= KeyInternal::Normal(key) && KeyInternal::Normal(key) < high_fence
    }

    fn is_in_page(&self, key: &[u8]) -> bool {
        // TODO: optimize
        if self.is_in_range(key) {
            let slot_id = self.binary_search(|slot_key| KeyInternal::Normal(key) < slot_key);
            assert!(1 <= slot_id && slot_id <= self.header().active_slot_count() - 1);
            let slot_metadata = self.slot_metadata(slot_id).unwrap();
            let key_offset = slot_metadata.offset() as usize;
            let key_size = slot_metadata.key_size() as usize;
            let slot_key = &self.page[key_offset..key_offset + key_size];
            slot_key == key
        } else {
            false
        }
    }
}

// Public methods
impl<'a> FosterBtreePage<'a> {
    pub fn init(page: &'a mut Page) {
        let header = PageHeader::new(page.len() as u16);
        page[0..PAGE_HEADER_SIZE].copy_from_slice(&header.to_bytes());
    }

    pub fn init_as_root(page: &'a mut Page) {
        let mut header = PageHeader::new(page.len() as u16);
        header.set_root(true);
        header.set_leaf(true);
        header.set_left_most(true);
        header.set_right_most(true);
        page[0..PAGE_HEADER_SIZE].copy_from_slice(&header.to_bytes());

        let mut fbt_page = FosterBtreePage::new(page);
        fbt_page.insert_low_fence(&[]);
        fbt_page.insert_high_fence(&[]);
    }

    pub fn new(page: &'a mut Page) -> Self {
        FosterBtreePage { page }
    }

    pub fn insert_low_fence(&mut self, key: &[u8]) {
        // Low fence is always at slot_id 0
        if self.header().active_slot_count() != 0 {
            panic!("Cannot insert low fence when active_slot_count != 0");
        }

        let (slot_id, mut slot_meta) = self.insert_slot_metadata(key.len() as u16, 0);
        assert!(slot_id == 0);
        slot_meta.set_ghost(true);
        self.update_slot_metadata(0, &slot_meta);
        let offset = slot_meta.offset();
        self.page[offset as usize..offset as usize + key.len()].copy_from_slice(key);
    }

    pub fn insert_high_fence(&mut self, key: &[u8]) {
        // High fence is initially inserted at slot_id 1 (slot_id will change if other slots are inserted).
        if self.header().active_slot_count() != 1 {
            // Assumes that the low fence is already inserted but no other slots are inserted.
            panic!("Cannot insert high fence when active_slot_count != 1");
        }
        let (slot_id, mut slot_meta) = self.insert_slot_metadata(key.len() as u16, 0);
        assert!(slot_id == 1);
        slot_meta.set_ghost(true);
        self.update_slot_metadata(1, &slot_meta);
        let offset = slot_meta.offset();
        self.page[offset as usize..offset as usize + key.len()].copy_from_slice(key);
    }

    /// Insert a key-value pair into the page.
    /// Need to check the existence of the key before inserting.
    /// Otherwise two keys with the same value will be inserted.
    pub fn insert(&mut self, key: &[u8], value: &[u8], make_ghost: bool) -> bool {
        let slot_size = key.len() + value.len();
        if SLOT_METADATA_SIZE + slot_size > self.free_space() {
            false
        } else {
            if !self.is_in_range(key) {
                false
            } else {
                let slot_id = self.binary_search(|slot_key| KeyInternal::Normal(key) < slot_key);
                assert!(1 <= slot_id && slot_id <= self.header().active_slot_count() - 1);

                // We want to shift [slot_id..] to [slot_id+1..] and overwrite the slot at slot_id.
                // Note that low_fence will never be shifted.
                self.shift_slot_meta(slot_id);

                // Place the slot data
                let start_offset = self.header().slot_data_start_offset();
                let offset = start_offset - slot_size as u16;
                self.page[offset as usize..offset as usize + key.len()].copy_from_slice(key);
                self.page[offset as usize + key.len()..offset as usize + slot_size]
                    .copy_from_slice(value);

                // Update the slot metadata
                let mut slot_metadata =
                    SlotMetadata::new(offset, key.len() as u16, value.len() as u16);
                slot_metadata.set_ghost(make_ghost);
                let res = self.update_slot_metadata(slot_id, &slot_metadata);
                assert!(res);

                // Update the header
                let mut header = self.header();
                header.set_slot_data_start_offset(offset);
                self.update_header(header);

                true
            }
        }
    }

    /// Returns the right-most key that is less than or equal to the given key.
    pub fn lower_bound(&self, key: &[u8]) -> Option<&[u8]> {
        if !self.is_in_range(key) {
            None
        } else {
            let slot_id = self.binary_search(|slot_key| KeyInternal::Normal(key) < slot_key);
            // If the key is in range, that means binary search will done on array with [false, ..., true]
            // Therefore, the first true is at 1 <= slot_id <= active_slot_count-1
            assert!(1 <= slot_id && slot_id <= self.header().active_slot_count() - 1);
            let slot_id = slot_id - 1;
            let slot_metadata = self.slot_metadata(slot_id).unwrap();
            let key_offset = slot_metadata.offset() as usize;
            let key_size = slot_metadata.key_size() as usize;
            let key = &self.page[key_offset..key_offset + key_size];
            Some(key)
        }
    }

    /// Returns the value associated with the given key.
    pub fn find(&self, key: &[u8]) -> Option<&[u8]> {
        if !self.is_in_range(key) {
            None
        } else {
            let slot_id = self.binary_search(|slot_key| KeyInternal::Normal(key) < slot_key);
            assert!(1 <= slot_id && slot_id <= self.header().active_slot_count() - 1);
            let slot_id = slot_id - 1;
            if slot_id == 0 {
                // The key is the low fence
                None
            } else {
                let slot_metadata = self.slot_metadata(slot_id).unwrap();
                let key_offset = slot_metadata.offset() as usize;
                let key_size = slot_metadata.key_size() as usize;
                let slot_key = &self.page[key_offset..key_offset + key_size];
                if slot_key == key {
                    let value_offset =
                        slot_metadata.offset() as usize + slot_metadata.key_size() as usize;
                    let value_size = slot_metadata.value_size() as usize;
                    let value = &self.page[value_offset..value_offset + value_size];
                    Some(value)
                } else {
                    None
                }
            }
        }
    }

    /// Mark the slot with the given key as a ghost slot.
    pub fn mark_ghost(&mut self, key: &[u8]) {
        if self.is_in_range(key) {
            let equal_or_greater =
                self.binary_search(|slot_key| KeyInternal::Normal(key) <= slot_key);
            let greater = self.binary_search(|slot_key| KeyInternal::Normal(key) < slot_key);
            assert!(greater - equal_or_greater <= 1); // There should be at most 1 slot with the same key.
            for i in equal_or_greater..greater {
                if i == 0 || i == self.header().active_slot_count() {
                    // Skip the low and high fences
                    continue;
                }
                let mut slot_metadata = self.slot_metadata(i).unwrap();
                // Set the slot to be a ghost slot
                slot_metadata.set_ghost(true);
                self.update_slot_metadata(i, &slot_metadata);
            }
        }
    }
}

#[cfg(test)]
impl FosterBtreePage<'_> {
    pub fn check_keys_are_sorted(&self) {
        // debug print all the keys
        // for i in 0..self.header().active_slot_count() {
        //     let key = self.get_slot_key(i).unwrap();
        //     println!("{:?}", key);
        // }
        for i in 1..self.header().active_slot_count() {
            let key1 = self.get_slot_key(i - 1).unwrap();
            let key2 = self.get_slot_key(i).unwrap();
            assert!(key1 < key2);
        }
    }

    pub fn check_slot_data_start(&self) {
        let mut slot_data_start = u16::MAX;
        for i in 0..self.header().active_slot_count() {
            let slot_metadata = self.slot_metadata(i).unwrap();
            let offset = slot_metadata.offset();
            if offset < slot_data_start {
                slot_data_start = offset;
            }
        }
        assert_eq!(slot_data_start, self.header().slot_data_start_offset());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::Page;

    /*
    #[test]
    fn test_init() {
        let mut page = Page::new();
        let low_fence = "a".as_bytes();
        let high_fence = "d".as_bytes();
        FosterBtreePage::init(&mut page);
        let mut fbt_page = FosterBtreePage::new(&mut page);
        fbt_page.insert_low_fence(low_fence);
        fbt_page.insert_high_fence(high_fence);

        fbt_page.check_keys_are_sorted();
        fbt_page.check_slot_data_start();

        assert_eq!(fbt_page.header().active_slot_count(), 2);
        assert_eq!(fbt_page.get_slot_key(0).unwrap(), low_fence);
        assert_eq!(fbt_page.get_slot_key(1).unwrap(), high_fence);
    }
    */

    #[test]
    fn test_lower_bound_and_find_for_normal_fence_page() {
        let mut page = Page::new();
        let low_fence = "b".as_bytes();
        let high_fence = "g".as_bytes();
        FosterBtreePage::init(&mut page);
        let mut fbt_page = FosterBtreePage::new(&mut page);
        fbt_page.insert_low_fence(low_fence);
        fbt_page.insert_high_fence(high_fence);

        assert_eq!(fbt_page.lower_bound("a".as_bytes()), None);
        assert_eq!(fbt_page.lower_bound("b".as_bytes()), Some("b".as_bytes()));
        assert_eq!(fbt_page.lower_bound("c".as_bytes()), Some("b".as_bytes()));
        assert_eq!(fbt_page.lower_bound("d".as_bytes()), Some("b".as_bytes()));
        assert_eq!(fbt_page.lower_bound("e".as_bytes()), Some("b".as_bytes()));
        assert_eq!(fbt_page.lower_bound("f".as_bytes()), Some("b".as_bytes()));
        assert_eq!(fbt_page.lower_bound("g".as_bytes()), None);
        assert_eq!(fbt_page.lower_bound("h".as_bytes()), None);
        assert_eq!(fbt_page.lower_bound("i".as_bytes()), None);
        assert_eq!(fbt_page.find("a".as_bytes()), None);
        assert_eq!(fbt_page.find("b".as_bytes()), None);
        assert_eq!(fbt_page.find("c".as_bytes()), None);
        assert_eq!(fbt_page.find("d".as_bytes()), None);
        assert_eq!(fbt_page.find("e".as_bytes()), None);
        assert_eq!(fbt_page.find("f".as_bytes()), None);
        assert_eq!(fbt_page.find("g".as_bytes()), None);
        assert_eq!(fbt_page.find("h".as_bytes()), None);
        assert_eq!(fbt_page.find("i".as_bytes()), None);

        // Push two keys "c" and "e"
        assert!(fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false));
        fbt_page.check_keys_are_sorted();
        fbt_page.check_slot_data_start();
        assert!(fbt_page.header().active_slot_count() == 3);

        assert!(fbt_page.insert("e".as_bytes(), "ee".as_bytes(), false));
        fbt_page.check_keys_are_sorted();
        fbt_page.check_slot_data_start();
        assert!(fbt_page.header().active_slot_count() == 4);

        assert_eq!(fbt_page.lower_bound("a".as_bytes()), None);
        assert_eq!(fbt_page.lower_bound("b".as_bytes()), Some("b".as_bytes()));
        assert_eq!(fbt_page.lower_bound("c".as_bytes()), Some("c".as_bytes()));
        assert_eq!(fbt_page.lower_bound("d".as_bytes()), Some("c".as_bytes()));
        assert_eq!(fbt_page.lower_bound("e".as_bytes()), Some("e".as_bytes()));
        assert_eq!(fbt_page.lower_bound("f".as_bytes()), Some("e".as_bytes()));
        assert_eq!(fbt_page.lower_bound("g".as_bytes()), None);
        assert_eq!(fbt_page.lower_bound("h".as_bytes()), None);
        assert_eq!(fbt_page.lower_bound("i".as_bytes()), None);
        assert_eq!(fbt_page.find("a".as_bytes()), None);
        assert_eq!(fbt_page.find("b".as_bytes()), None);
        assert_eq!(fbt_page.find("c".as_bytes()), Some("cc".as_bytes()));
        assert_eq!(fbt_page.find("d".as_bytes()), None);
        assert_eq!(fbt_page.find("e".as_bytes()), Some("ee".as_bytes()));
        assert_eq!(fbt_page.find("f".as_bytes()), None);
        assert_eq!(fbt_page.find("g".as_bytes()), None);
        assert_eq!(fbt_page.find("h".as_bytes()), None);
        assert_eq!(fbt_page.find("i".as_bytes()), None);
    }

    #[test]
    fn test_lower_bound_and_find_for_root_page() {
        let mut page = Page::new();
        FosterBtreePage::init_as_root(&mut page);
        let mut fbt_page = FosterBtreePage::new(&mut page);

        assert_eq!(fbt_page.lower_bound("a".as_bytes()), Some("".as_bytes()));
        assert_eq!(fbt_page.lower_bound("b".as_bytes()), Some("".as_bytes()));
        assert_eq!(fbt_page.lower_bound("c".as_bytes()), Some("".as_bytes()));
        assert_eq!(fbt_page.lower_bound("d".as_bytes()), Some("".as_bytes()));
        assert_eq!(fbt_page.lower_bound("e".as_bytes()), Some("".as_bytes()));
        assert_eq!(fbt_page.lower_bound("f".as_bytes()), Some("".as_bytes()));
        assert_eq!(fbt_page.lower_bound("g".as_bytes()), Some("".as_bytes()));
        assert_eq!(fbt_page.lower_bound("h".as_bytes()), Some("".as_bytes()));
        assert_eq!(fbt_page.lower_bound("i".as_bytes()), Some("".as_bytes()));
        assert_eq!(fbt_page.find("a".as_bytes()), None);
        assert_eq!(fbt_page.find("b".as_bytes()), None);
        assert_eq!(fbt_page.find("c".as_bytes()), None);
        assert_eq!(fbt_page.find("d".as_bytes()), None);
        assert_eq!(fbt_page.find("e".as_bytes()), None);
        assert_eq!(fbt_page.find("f".as_bytes()), None);
        assert_eq!(fbt_page.find("g".as_bytes()), None);
        assert_eq!(fbt_page.find("h".as_bytes()), None);
        assert_eq!(fbt_page.find("i".as_bytes()), None);

        fbt_page.insert("c".as_bytes(), "cc".as_bytes(), false);
        fbt_page.check_keys_are_sorted();
        fbt_page.check_slot_data_start();
        assert!(fbt_page.header().active_slot_count() == 3);
        fbt_page.insert("e".as_bytes(), "ee".as_bytes(), false);
        fbt_page.check_keys_are_sorted();
        fbt_page.check_slot_data_start();
        assert!(fbt_page.header().active_slot_count() == 4);

        assert_eq!(fbt_page.lower_bound("a".as_bytes()), Some("".as_bytes()));
        assert_eq!(fbt_page.lower_bound("b".as_bytes()), Some("".as_bytes()));
        assert_eq!(fbt_page.lower_bound("c".as_bytes()), Some("c".as_bytes()));
        assert_eq!(fbt_page.lower_bound("d".as_bytes()), Some("c".as_bytes()));
        assert_eq!(fbt_page.lower_bound("e".as_bytes()), Some("e".as_bytes()));
        assert_eq!(fbt_page.lower_bound("f".as_bytes()), Some("e".as_bytes()));
        assert_eq!(fbt_page.lower_bound("g".as_bytes()), Some("e".as_bytes()));
        assert_eq!(fbt_page.lower_bound("h".as_bytes()), Some("e".as_bytes()));
        assert_eq!(fbt_page.lower_bound("i".as_bytes()), Some("e".as_bytes()));
        assert_eq!(fbt_page.find("a".as_bytes()), None);
        assert_eq!(fbt_page.find("b".as_bytes()), None);
        assert_eq!(fbt_page.find("c".as_bytes()), Some("cc".as_bytes()));
        assert_eq!(fbt_page.find("d".as_bytes()), None);
        assert_eq!(fbt_page.find("e".as_bytes()), Some("ee".as_bytes()));
        assert_eq!(fbt_page.find("f".as_bytes()), None);
        assert_eq!(fbt_page.find("g".as_bytes()), None);
        assert_eq!(fbt_page.find("h".as_bytes()), None);
        assert_eq!(fbt_page.find("i".as_bytes()), None);
    }
}
