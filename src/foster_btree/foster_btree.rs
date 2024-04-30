use std::{
    cell::UnsafeCell,
    collections::{HashMap, HashSet, VecDeque},
    sync::{atomic::Ordering, Arc, Mutex},
    thread,
    time::Duration,
};

use lazy_static::lazy_static;
use tempfile::TempDir;

use crate::{
    buffer_pool::prelude::*,
    foster_btree::foster_btree_page::PAGE_HEADER_SIZE,
    log, log_trace,
    page::{Page, PageId, BASE_PAGE_HEADER_SIZE, PAGE_SIZE},
    write_ahead_log::{prelude::LogRecord, LogBufferRef},
};

use super::foster_btree_page::{BTreeKey, FosterBtreePage};

pub enum TreeStatus {
    Ok,
    NotFound,
    NotInPageRange,
    Duplicate,
    NotReadyForPhysicalDelete,
    WriteLockFailed,
    MemPoolStatus(MemPoolStatus),
}

impl From<MemPoolStatus> for TreeStatus {
    fn from(status: MemPoolStatus) -> Self {
        TreeStatus::MemPoolStatus(status)
    }
}

impl std::fmt::Debug for TreeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TreeStatus::Ok => write!(f, "Ok"),
            TreeStatus::NotFound => write!(f, "NotFound"),
            TreeStatus::NotInPageRange => write!(f, "NotInPageRange"),
            TreeStatus::Duplicate => write!(f, "Duplicate"),
            TreeStatus::NotReadyForPhysicalDelete => write!(f, "NotReadyForPhysicalDelete"),
            TreeStatus::WriteLockFailed => write!(f, "WriteLockFailed"),
            TreeStatus::MemPoolStatus(status) => write!(f, "{:?}", status),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum OpType {
    SPLIT,
    MERGE,
    LOADBALANCE,
    ADOPT,
    ANTIADOPT,
    ASCENDROOT,
    DESCENDROOT,
}

struct OpByte(pub u8);

impl OpByte {
    pub fn new() -> Self {
        OpByte(0)
    }
    pub fn is_merge_done(&self) -> bool {
        self.0 & 0b0000_0001 != 0
    }
    pub fn merge_done(&mut self) {
        self.0 |= 0b0000_0001;
    }
    pub fn is_load_balance_done(&self) -> bool {
        self.0 & 0b0000_0010 != 0
    }
    pub fn load_balance_done(&mut self) {
        self.0 |= 0b0000_0010;
    }
    pub fn is_adopt_done(&self) -> bool {
        self.0 & 0b0000_0100 != 0
    }
    pub fn adopt_done(&mut self) {
        self.0 |= 0b0000_0100;
    }
    pub fn is_antiadopt_done(&self) -> bool {
        self.0 & 0b0000_1000 != 0
    }
    pub fn antiadopt_done(&mut self) {
        self.0 |= 0b0000_1000;
    }
    pub fn is_ascend_root_done(&self) -> bool {
        self.0 & 0b0001_0000 != 0
    }
    pub fn ascend_root_done(&mut self) {
        self.0 |= 0b0001_0000;
    }
    pub fn is_descend_root_done(&self) -> bool {
        self.0 & 0b0010_0000 != 0
    }
    pub fn descend_root_done(&mut self) {
        self.0 |= 0b0010_0000;
    }
    pub fn reset(&mut self) {
        self.0 = 0;
    }
}

struct OpStat {
    stats: UnsafeCell<[usize; 7]>,
}

impl OpStat {
    pub fn new() -> Self {
        OpStat {
            stats: UnsafeCell::new([0; 7]),
        }
    }
    pub fn inc(&self, op_type: OpType) {
        let guard = unsafe { &mut *self.stats.get() };
        guard[op_type as usize] += 1;
    }

    pub fn merge(&self, other: &OpStat) {
        let guard = unsafe { &mut *self.stats.get() };
        let other_guard = unsafe { &*other.stats.get() };
        for i in 0..6 {
            guard[i] += other_guard[i];
        }
    }

    pub fn to_string(&self) -> String {
        let guard = unsafe { &*self.stats.get() };
        format!(
            "SPLIT: {}, MERGE: {}, LOADBALANCE: {}, ADOPT: {}, ANTIADOPT: {}, ASCENDROOT: {}, DESCENDROOT: {}",
            guard[OpType::SPLIT as usize],
            guard[OpType::MERGE as usize],
            guard[OpType::LOADBALANCE as usize],
            guard[OpType::ADOPT as usize],
            guard[OpType::ANTIADOPT as usize],
            guard[OpType::ASCENDROOT as usize],
            guard[OpType::DESCENDROOT as usize],
        )
    }
}

struct LocalStat {
    pub stat: OpStat,
}

impl Drop for LocalStat {
    fn drop(&mut self) {
        GLOBAL_STAT.lock().unwrap().merge(&self.stat);
    }
}

lazy_static! {
    static ref GLOBAL_STAT: Mutex<OpStat> = Mutex::new(OpStat::new());
}

thread_local! {
    static LOCAL_STAT: LocalStat = LocalStat {
        stat: OpStat::new(),
    };
}

fn inc_local_stat(op_type: OpType) {
    LOCAL_STAT.with(|stat| {
        stat.stat.inc(op_type);
    });
}

// The threshold for page modification.
// [0, MIN_BYTES_USED) is small.
// [MIN_BYTES_USED, MAX_BYTES_USED) is normal.
// [MAX_BYTES_USED, PAGE_SIZE) is large.
// If the page has less than MIN_BYTES_USED, then we need to MERGE or LOADBALANCE.
pub const MIN_BYTES_USED: usize = (PAGE_SIZE - BASE_PAGE_HEADER_SIZE) / 5;
// If the page has more than MAX_BYTES_USED, then we need to LOADBALANCE.
pub const MAX_BYTES_USED: usize = (PAGE_SIZE - BASE_PAGE_HEADER_SIZE) * 4 / 5;

#[inline]
fn is_small(page: &Page) -> bool {
    (page.total_bytes_used() as usize) < MIN_BYTES_USED
}

#[inline]
fn is_large(page: &Page) -> bool {
    (page.total_bytes_used() as usize) >= MAX_BYTES_USED
}

/// Check if the parent page is the parent of the child page.
/// This includes the foster child relationship.
fn is_parent_and_child(parent: &Page, child: &Page) -> bool {
    let low_key = BTreeKey::new(&child.get_raw_key(child.low_fence_slot_id()));
    // Find the slot id of the child page in the parent page.
    let slot_id = parent.lower_bound_slot_id(&low_key);
    // Check if the value of the slot id is the same as the child page id.
    parent.get_val(slot_id) == child.get_id().to_be_bytes()
}

/// Check if the parent page is the foster parent of the child page.
fn is_foster_relationship(parent: &Page, child: &Page) -> bool {
    parent.level() == child.level()
        && parent.has_foster_child()
        && parent.get_foster_page_id() == child.get_id()
}

fn should_modify_structure_for_foster_relationship(
    this: &Page,
    child: &Page,
    op_byte: &mut OpByte,
) -> Option<OpType> {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(is_foster_relationship(this, child));
    if !op_byte.is_merge_done() && should_merge(this, child) {
        op_byte.merge_done();
        return Some(OpType::MERGE);
    }
    if !op_byte.is_load_balance_done() && should_load_balance(this, child) {
        op_byte.load_balance_done();
        return Some(OpType::LOADBALANCE);
    }
    if !op_byte.is_ascend_root_done() && should_root_ascend(this, child) {
        op_byte.ascend_root_done();
        return Some(OpType::ASCENDROOT);
    }
    None
}

fn should_modify_structure_for_parent_child_relationship(
    this: &Page,
    child: &Page,
    op_byte: &mut OpByte,
) -> Option<OpType> {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(!is_foster_relationship(this, child));
    if !op_byte.is_adopt_done() && should_adopt(this, child) {
        op_byte.adopt_done();
        return Some(OpType::ADOPT);
    }
    if !op_byte.is_antiadopt_done() && should_antiadopt(this, child) {
        op_byte.antiadopt_done();
        return Some(OpType::ANTIADOPT);
    }
    if !op_byte.is_descend_root_done() && should_root_descend(this, child) {
        op_byte.descend_root_done();
        return Some(OpType::DESCENDROOT);
    }
    None
}

/// There are 3 * 3 = 9 possible size
/// S: Small, N: Normal, L: Large
/// SS, NS, SN: Merge
/// SL, LS: Load balance
/// NN, LN, NL, LL: Do nothing
fn should_merge(this: &Page, child: &Page) -> bool {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(is_foster_relationship(this, child));
    // Check if this is small and child is not too large
    if is_small(this) && !is_large(child) {
        return true;
    }

    if !is_large(this) && is_small(child) {
        return true;
    }
    false
}

/// There are 3 * 3 = 9 possible size
/// S: Small, N: Normal, L: Large
/// SS, NS, SN: Merge
/// SL, LS: Load balance
/// NN, LN, NL, LL: Do nothing
fn should_load_balance(this: &Page, child: &Page) -> bool {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(is_foster_relationship(this, child));
    // Check if this is not too small and child is too large
    if (is_small(this) && is_large(child)) || (is_large(this) && is_small(child)) {
        return true;
    }
    false
}

fn should_adopt(this: &Page, child: &Page) -> bool {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(!is_foster_relationship(this, child));
    // Check if the parent page has enough space to adopt the foster child of the child page.
    if !child.has_foster_child() {
        return false;
    }
    // We want to do anti-adoption as less as possible.
    // Therefore, if child will need merge or load balance, we prioritize them over adoption.
    // If this is large, there is a high chance that the parent will need to split when adopting the foster child.
    // If child is small, there is a high chance that the parent will need to merge or load balance.
    if is_large(this) || is_small(child) {
        return false;
    }
    if this.total_free_space()
        < this.bytes_needed(
            &child.get_foster_key(),
            PageId::default().to_be_bytes().as_ref(),
        )
    {
        // If the parent page does not have enough space to adopt the foster child, then we do not adopt.
        return false;
    }
    true
}

fn should_antiadopt(this: &Page, child: &Page) -> bool {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(!is_foster_relationship(this, child));
    if child.has_foster_child() {
        return false;
    }
    if !is_small(child) {
        // If child is not small, there is no need to antiadopt because the child will not need to merge or load balance.
        return false;
    }
    let low_key = BTreeKey::new(&child.get_raw_key(child.low_fence_slot_id()));
    let slot_id = this.lower_bound_slot_id(&low_key);
    if this.has_foster_child() {
        if slot_id + 1 >= this.foster_child_slot_id() {
            return false;
        }
    } else {
        if slot_id + 1 >= this.high_fence_slot_id() {
            return false;
        }
    }

    true
}

fn should_root_ascend(this: &Page, child: &Page) -> bool {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(is_foster_relationship(this, child));
    if !this.is_root() {
        return false;
    }
    true
}

fn should_root_descend(this: &Page, child: &Page) -> bool {
    debug_assert!(is_parent_and_child(this, child));
    debug_assert!(!is_foster_relationship(this, child));
    if !this.is_root() {
        return false;
    }
    if this.active_slot_count() != 1 {
        return false;
    }
    if child.has_foster_child() {
        return false;
    }
    // Same low fence and high fence
    debug_assert_eq!(this.get_low_fence(), child.get_low_fence());
    debug_assert_eq!(this.get_high_fence(), child.get_high_fence());
    true
}

/// Splitting the page and insert the key-value pair in the appropriate page.
///
/// We want to insert the key-value pair into the page.
/// However, the page does not have enough space to insert the key-value pair.
///
/// Some assumptions:
/// 1. Key must be in the range of the page.
/// 2. The foster child is a unused page. It will be initialized in this function.
fn split_insert(this: &mut Page, foster_child: &mut Page, key: &[u8], value: &[u8]) {
    #[cfg(any(feature = "stat"))]
    inc_local_stat(OpType::SPLIT);

    let slot_id = this.lower_bound_slot_id(&BTreeKey::new(key)) + 1;
    if slot_id == this.high_fence_slot_id() {
        if this.has_foster_child() {
            panic!("Not at the right page to insert the key-value pair. Go to the foster child.");
        }
        // If this has space to insert the foster key, then we don't need to split the page.
        if this.total_free_space() >= this.bytes_needed(key, &foster_child.get_id().to_be_bytes()) {
            // We need new page to insert the key-value pair. No movement of slots is required.
            foster_child.init();
            foster_child.set_level(this.level());
            foster_child.set_low_fence(key);
            foster_child.set_high_fence(this.get_raw_key(this.high_fence_slot_id()));
            foster_child.set_right_most(this.is_right_most());
            let res = foster_child.insert(key, value, false);
            assert!(res);
            // Insert the foster key into this page.
            this.set_has_foster_child(true);
            let res = this.insert(key, &foster_child.get_id().to_be_bytes(), false);
            assert!(res);
            return;
        } else {
            // We need to move some slots to the foster child from the parent page.
            let mut moving_kvs = Vec::new();
            for i in (1..this.high_fence_slot_id()).rev() {
                let key = this.get_raw_key(i).to_owned();
                let val = this.get_val(i).to_owned();
                moving_kvs.push((key, val));
                this.remove_at(i);
                if this.total_free_space()
                    >= this.bytes_needed(
                        &moving_kvs.last().unwrap().0,
                        &foster_child.get_id().to_be_bytes(),
                    )
                {
                    break;
                }
            }
            if moving_kvs.is_empty() {
                panic!("Page is full but cannot split because the slots are too large")
            }
            moving_kvs.reverse();
            foster_child.init();
            foster_child.set_level(this.level());
            foster_child.set_low_fence(&moving_kvs[0].0);
            foster_child.set_high_fence(this.get_raw_key(this.high_fence_slot_id()));
            foster_child.set_right_most(this.is_right_most());
            let res = foster_child.append_sorted(&moving_kvs);
            assert!(res);
            foster_child.set_has_foster_child(this.has_foster_child());
            let res = foster_child.insert(key, value, false);
            assert!(res);
            this.set_has_foster_child(true);
            let res = this.insert(
                &moving_kvs[0].0,
                &foster_child.get_id().to_be_bytes(),
                false,
            );
            assert!(res);
            return;
        }
    } else {
        // The page is full and we need to split the page.
        // First, we split the page into two pages with (almost) equal sizes.
        let total_size = this.total_bytes_used();
        let mut half_bytes = total_size / 2;
        let mut moving_slot_ids = Vec::new();
        let mut moving_kvs = Vec::new();
        for i in (1..this.high_fence_slot_id()).rev() {
            let key = this.get_raw_key(i);
            let val = this.get_val(i);
            let bytes_needed = this.bytes_needed(key, val);
            if half_bytes >= bytes_needed {
                half_bytes -= bytes_needed;
                moving_slot_ids.push(i);
                moving_kvs.push((key, val));
            } else {
                break;
            }
        }
        if moving_kvs.is_empty() {
            panic!("Page is full but cannot split because the slots are too large");
        }

        // Reverse the moving slots
        moving_kvs.reverse();
        moving_slot_ids.reverse();

        let foster_key = moving_kvs[0].0.to_vec();

        foster_child.init();
        foster_child.set_level(this.level());
        foster_child.set_low_fence(&foster_key);
        foster_child.set_high_fence(this.get_raw_key(this.high_fence_slot_id()));
        foster_child.set_right_most(this.is_right_most());
        let res = foster_child.append_sorted(&moving_kvs);
        assert!(res);
        foster_child.set_has_foster_child(this.has_foster_child());
        // Remove the moved slots from this
        this.remove_range(moving_slot_ids[0], this.high_fence_slot_id());
        let res = this.insert(&foster_key, &foster_child.get_id().to_be_bytes(), false);
        assert!(res);
        this.set_has_foster_child(true);

        // Now, we have two pages: this and foster_child.
        // We need to decide which page to insert the key-value pair.
        // If the key is less than the foster key, we insert the key into this.
        // Otherwise, we insert the key into the foster child.
        let res = if key < &foster_key {
            this.insert(key, value, false)
        } else {
            foster_child.insert(key, value, false)
        };

        if res {
            return;
        } else {
            // Need to split the page into three
            // There are some tricky cases where insertion causes the page to split into three pages.
            // E.g. Page has capacity of 6 and the current page looks like this:
            // [[xxx],[zzz]]
            // We want to insert [yyyy]
            // None of [[xxx],[yyyy]] or [[zzz],[yyyy]] can work.
            // We need to split the page into three pages.
            // [[xxx]], [[yyyy]], [[zzz]]

            // If we cannot insert the key into one of the pages, then we need to split the page into three.
            // This: [l, r1, r2, ..., rk, (f)x h]
            // Foster1: [l(x), x, (f)rk+1, h]
            // Foster2: [l(rk+1), rk+1, rk+2, ..., h]
            unimplemented!("Need to split the page into three")
        }
    }
}
/// Merge the foster child page into this page.
/// The foster child page will be deleted.
// Before:
//   this [k0, k2) --> sibling [k1, k2)
//
// After:
//   this [k0, k2)
fn merge(this: &mut Page, foster_child: &mut Page) {
    #[cfg(any(feature = "stat"))]
    inc_local_stat(OpType::MERGE);

    debug_assert!(is_parent_and_child(this, foster_child));
    debug_assert!(is_foster_relationship(this, foster_child));

    let mut kvs = Vec::new();
    for i in 1..=foster_child.active_slot_count() {
        let key = foster_child.get_raw_key(i);
        let val = foster_child.get_val(i);
        kvs.push((key, val));
    }
    this.remove_at(this.foster_child_slot_id());
    let res = this.append_sorted(&kvs);
    assert!(res);
    this.set_has_foster_child(foster_child.has_foster_child());
    foster_child.remove_range(1, foster_child.high_fence_slot_id());
    foster_child.set_has_foster_child(false);

    #[cfg(debug_assertions)]
    {
        this.run_consistency_checks(false);
        foster_child.run_consistency_checks(false);
    }
}

/// Move some slots from one page to another page so that the two pages approximately
/// have the same number of bytes.
/// Assumptions:
/// 1. this page is the foster parent of the foster child.
/// 2. The two pages are initialized. (have low fence and high fence)
/// 3. Balancing does not move the foster key from one page to another.
/// 4. Balancing does not move the low fence and high fence.
fn balance(this: &mut Page, foster_child: &mut Page) {
    #[cfg(any(feature = "stat"))]
    inc_local_stat(OpType::LOADBALANCE);

    debug_assert!(is_parent_and_child(this, foster_child));
    debug_assert!(is_foster_relationship(this, foster_child));

    // Calculate the total bytes of the two pages.
    let this_total = this.total_bytes_used();
    let foster_child_total = foster_child.total_bytes_used();
    if this_total == foster_child_total {
        // The two pages are balanced.
        return;
    } else {
        if this_total > foster_child_total {
            // Move some slots from this to foster child.
            let mut diff = (this_total - foster_child_total) / 2;
            let mut moving_slot_ids = Vec::new();
            let mut moving_kvs: Vec<(&[u8], &[u8])> = Vec::new();
            for i in (1..this.foster_child_slot_id()).rev() {
                let key = this.get_raw_key(i);
                let val = this.get_val(i);
                let bytes_needed = this.bytes_needed(key, val);
                if diff >= bytes_needed {
                    diff -= bytes_needed;
                    moving_slot_ids.push(i);
                    moving_kvs.push((key, val));
                } else {
                    break;
                }
            }
            if moving_kvs.is_empty() {
                // No slots to move
                return;
            }
            // Reverse the moving slots
            moving_kvs.reverse();
            moving_slot_ids.reverse();
            // Before:
            // this [l, k0, k1, k2, ..., f(kN), h) --> foster_child [l(kN), kN, kN+1, ..., h)
            // After:
            // this [l, k0, k1, ..., f(kN-m), h) --> foster_child [l(kN-m), kN-m, kN-m+1, ..., h)
            foster_child.set_low_fence(moving_kvs[0].0);
            for (key, val) in moving_kvs {
                let res = foster_child.insert(key, val, false);
                assert!(res);
            }
            // Remove the moved slots from this
            this.remove_range(moving_slot_ids[0], this.high_fence_slot_id());
            let res = this.insert(
                &foster_child.get_raw_key(0),
                &foster_child.get_id().to_be_bytes(),
                false,
            );
            assert!(res);
        } else {
            // Move some slots from foster child to this.
            let mut diff = (foster_child_total - this_total) / 2;
            let mut moving_slot_ids = Vec::new();
            let mut moving_kvs = Vec::new();
            let end = if foster_child.has_foster_child() {
                // We do not move the foster key
                foster_child.foster_child_slot_id()
            } else {
                foster_child.high_fence_slot_id()
            };
            for i in 1..end {
                let key = foster_child.get_raw_key(i);
                let val = foster_child.get_val(i);
                let bytes_needed = foster_child.bytes_needed(key, val);
                if diff >= bytes_needed {
                    diff -= bytes_needed;
                    moving_slot_ids.push(i);
                    moving_kvs.push((key, val));
                } else {
                    break;
                }
            }
            if moving_kvs.is_empty() {
                // No slots to move
                return;
            }
            // Before:
            // this [l, k0, k1, k2, ..., f(kN), h) --> foster_child [l(kN), kN, kN+1, ..., h)
            // After:
            // this [l, k0, k1, ..., f(kN+m), h) --> foster_child [l(kN+m), kN+m, kN+m+1, ..., h)
            this.remove_at(this.foster_child_slot_id());
            for (key, val) in moving_kvs {
                let res = this.insert(key, val, false);
                assert!(res);
            }
            // Remove the moved slots from foster child
            foster_child.remove_range(
                moving_slot_ids[0],
                moving_slot_ids[moving_slot_ids.len() - 1] + 1,
            );
            let foster_key = foster_child.get_raw_key(1).to_vec();
            foster_child.set_low_fence(&foster_key);
            let res = this.insert(&foster_key, &foster_child.get_id().to_be_bytes(), false);
            assert!(res);
        }
    }
}

/// Adopt the foster child of the child page.
/// The foster child of the child page is removed and becomes the child of the parent page.
///
/// Parent page
/// * insert the foster key and the foster child page id.
///
/// Child page
/// * remove the foster key from the child page.
/// * remove the foster child flag and set the high fence to the foster key.
///
/// Before:
///   parent [k0, k2)
///    |
///    v
///   child0 [k0, k2) --> child1 [k1, k2)
//
/// After:
///   parent [k0, k2)
///    +-------------------+
///    |                   |
///    v                   v
///   child0 [k0, k1)    child1 [k1, k2)
fn adopt(parent: &mut Page, child: &mut Page) {
    #[cfg(any(feature = "stat"))]
    inc_local_stat(OpType::ADOPT);

    debug_assert!(is_parent_and_child(parent, child));
    debug_assert!(!is_foster_relationship(parent, child));
    debug_assert!(child.has_foster_child());

    let foster_child_slot_id = child.foster_child_slot_id();
    let foster_key = child.get_foster_key().to_owned();
    let foster_child_page_id = child.get_foster_page_id();

    // Try insert into parent page.
    let res = parent.insert(&foster_key, &foster_child_page_id.to_be_bytes(), false);
    assert!(res);
    // Remove the foster key from the child page.
    child.remove_at(foster_child_slot_id);
    child.set_has_foster_child(false);
    child.set_high_fence(foster_key.as_ref());
}

/// Anti-adopt.
/// Make a child a foster parent. This operation is needed when we do load balancing between siblings.
///
/// Before:
/// parent [..., k0, k1, k2, ..., kN)
///  +-------------------+
///  |                   |
///  v                   v
/// child1 [k0, k1)   child2 [k1, k2)
///
/// After:
/// parent [..., k0, k2, ..., kN)
///  |
///  v
/// child1 [k0, k2) --> foster_child [k1, k2)
fn anti_adopt(parent: &mut Page, child1: &mut Page) {
    #[cfg(any(feature = "stat"))]
    inc_local_stat(OpType::ANTIADOPT);

    debug_assert!(is_parent_and_child(parent, child1));
    debug_assert!(!is_foster_relationship(parent, child1));
    debug_assert!(!child1.has_foster_child());

    // Identify child1 slot
    let low_key = BTreeKey::new(&child1.get_raw_key(child1.low_fence_slot_id()));
    let slot_id = parent.lower_bound_slot_id(&low_key);
    if parent.has_foster_child() {
        debug_assert!(slot_id + 1 < parent.foster_child_slot_id());
    } else {
        debug_assert!(slot_id + 1 < parent.high_fence_slot_id());
    }
    let k1 = parent.get_raw_key(slot_id + 1);
    let child2_ptr = parent.get_val(slot_id + 1);
    let k2 = parent.get_raw_key(slot_id + 2);
    child1.set_high_fence(k2);
    child1.set_has_foster_child(true);
    let res = child1.insert(k1, child2_ptr, false);
    if !res {
        panic!("Cannot insert the slot into the child page");
    }
    parent.remove_at(slot_id + 1);
}

/// Descend the root page to the child page if the root page
/// contains only the page id of the child page.
/// Note that the root page is never deleted.
/// The child page will be deleted.
///
/// Before:
/// root [-inf, +inf)
///  |
///  v
/// child [-inf, +inf)
///
/// After:
/// root [-inf, +inf)
fn descend_root(root: &mut Page, child: &mut Page) {
    #[cfg(any(feature = "stat"))]
    inc_local_stat(OpType::DESCENDROOT);

    assert!(root.is_root());
    // If the root contains only the page id of the child,
    // then we can push the child's content to the root and
    // descend the root level to the child level.
    assert_eq!(root.active_slot_count(), 1);
    assert_eq!(root.get_val(1), child.get_id().to_be_bytes());
    assert_eq!(child.get_low_fence(), BTreeKey::MinusInfty);
    assert_eq!(child.get_high_fence(), BTreeKey::PlusInfty);

    root.remove_at(1);
    root.decrement_level();

    let mut kvs = Vec::new();
    for i in 1..=child.active_slot_count() {
        let key = child.get_raw_key(i);
        let val = child.get_val(i);
        kvs.push((key, val));
    }
    let res = root.append_sorted(&kvs);
    assert!(res);

    child.remove_range(1, child.high_fence_slot_id());
}

/// Ascend the root page to the parent page if the root page
/// contains a foster child.
/// Note that the root page will still be the root page after
/// the ascend operation.
///
/// Before:
/// root [-inf, +inf) --> foster_child [k0, +inf)
///
/// After:
/// root [-inf, +inf)
///  +-------------------+
///  |                   |
///  v                   v
/// child [-inf, k0)    foster_child [k0, +inf)
fn ascend_root(root: &mut Page, child: &mut Page) {
    #[cfg(any(feature = "stat"))]
    inc_local_stat(OpType::ASCENDROOT);

    assert!(root.is_root());
    assert!(root.has_foster_child());
    assert!(!root.empty());
    assert!(child.empty());

    // Copy the foster slot data to local variables.
    let foster_key = root.get_foster_key().to_owned();

    // Set level, has_foster_child flag, and fence keys
    child.init();
    child.set_level(root.level());
    child.set_low_fence(root.get_raw_key(root.low_fence_slot_id()));
    child.set_left_most(root.is_left_most());
    child.set_high_fence(&foster_key);
    root.increment_level();
    root.set_has_foster_child(false);

    // Move the root's data to the child
    let mut kvs = Vec::new();
    for i in 1..root.foster_child_slot_id() {
        let key = root.get_raw_key(i);
        let val = root.get_val(i);
        kvs.push((key, val));
    }
    let res = child.append_sorted(&kvs);
    assert!(res);

    // Remove the moved slots from the root
    root.remove_range(1, root.foster_child_slot_id());
    root.insert(&[], &child.get_id().to_be_bytes(), false);

    #[cfg(debug_assertions)]
    {
        root.run_consistency_checks(false);
        child.run_consistency_checks(false);
    }
}

fn to_dot_string(page: &Page) -> String {
    let mut result = format!("\t{} [label=\"[P{}](", page.get_id(), page.get_id());
    // If page is a leaf, add the key-value pairs
    let low_fence = page.get_raw_key(page.low_fence_slot_id());
    if low_fence == &[] {
        result.push_str("L[-∞], ");
    } else {
        let low_fence_usize = usize::from_be_bytes(low_fence.try_into().unwrap());
        result.push_str(&format!("L[{}], ", low_fence_usize));
    }
    for i in 1..=page.active_slot_count() {
        let key = page.get_raw_key(i);
        let key = if key == &[] {
            "-∞".to_string()
        } else {
            let key_usize = usize::from_be_bytes(key.try_into().unwrap());
            key_usize.to_string()
        };
        result.push_str(&format!("{}, ", key));
    }
    let high_fence = page.get_raw_key(page.high_fence_slot_id());
    if high_fence == &[] {
        result.push_str("H[+∞])\"];");
    } else {
        let high_fence_usize = usize::from_be_bytes(high_fence.try_into().unwrap());
        result.push_str(&format!("H[{}])\"];", high_fence_usize));
    }
    result.push_str("\n");
    result
}

fn print_page(p: &Page) {
    println!(
        "----------------- Page ID: {} -----------------",
        p.get_id()
    );
    println!("Level: {}", p.level());
    println!("Size: {}", p.total_bytes_used());
    println!("Available space: {}", p.total_free_space());
    println!("Low fence: {:?}", p.get_low_fence());
    println!("High fence: {:?}", p.get_high_fence());
    println!("Slot count: {}", p.slot_count());
    println!("Active slot count: {}", p.active_slot_count());
    println!("Foster child: {}", p.has_foster_child());
    for i in 1..=p.active_slot_count() {
        let key = p.get_raw_key(i as u16);
        let val = p.get_val(i as u16);
        let key = if key.is_empty() {
            "[]".to_owned()
        } else {
            usize::from_be_bytes(key.try_into().unwrap()).to_string()
        };
        let val = if p.has_foster_child() && i == p.active_slot_count() {
            let page_id = u32::from_be_bytes(val.try_into().unwrap()).to_string();
            format!("PageID({})", page_id)
        } else {
            if p.is_leaf() {
                let size = val.len();
                let min_size = 8.min(size);
                let val = &val[..min_size];
                format!("{:?}...len={}", val, size)
            } else {
                let page_id = u32::from_be_bytes(val.try_into().unwrap()).to_string();
                format!("PageID({})", page_id)
            }
        };
        println!("Slot {}: Key: {}, Value: {}", i, key, val);
    }
    println!("----------------------------------------------");
}

pub fn create_new(
    txn_id: u64,
    c_key: ContainerKey,
    bp: BufferPoolRef,
    // wal_buffer: LogBufferRef,
) -> FosterBtree<BufferPool> {
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
        mem_pool: bp.clone(),
        // wal_buffer,
    }
}

pub struct FosterBtree<T: MemPool> {
    pub c_key: ContainerKey,
    pub root_key: PageKey,
    pub mem_pool: Arc<T>,
    // pub wal_buffer: LogBufferRef,
}

impl<T: MemPool> FosterBtree<T> {
    pub fn generate_dot(&self) -> String {
        let mut result = "digraph G {\n".to_string();
        // rectangle nodes
        result.push_str("\tnode [shape=rectangle];\n");
        let current_page = self.read_page(self.root_key);
        let mut levels = HashMap::new();
        let mut stack = VecDeque::new();
        stack.push_back(current_page);
        while let Some(page) = stack.pop_front() {
            result.push_str(&to_dot_string(&page));
            levels
                .entry(page.level())
                .or_insert(HashSet::new())
                .insert(page.get_id());
            if page.has_foster_child() {
                // Push the foster child to front of the stack
                let foster_child_page_id = page.get_foster_page_id();
                let foster_child_page = self
                    .mem_pool
                    .get_page_for_read(PageKey::new(self.c_key, foster_child_page_id))
                    .unwrap();
                result.push_str(&format!(
                    "\t{} -> {};\n",
                    page.get_id(),
                    foster_child_page_id
                ));
                stack.push_front(foster_child_page);
            }
            if page.is_leaf() {
                continue;
            }
            for i in 1..=page.active_slot_count() {
                let child_page_id = PageId::from_be_bytes(page.get_val(i).try_into().unwrap());
                let child_page = self
                    .mem_pool
                    .get_page_for_read(PageKey::new(self.c_key, child_page_id))
                    .unwrap();
                result.push_str(&format!("\t{} -> {};\n", page.get_id(), child_page_id));
                stack.push_back(child_page);
            }
        }
        // Map the same level pages to the same rank
        for (_, page_ids) in levels {
            result.push_str(&format!("\t{{rank=same; {:?}}}\n", page_ids));
        }

        result.push_str("}\n");

        result
    }

    pub fn stats(&self) -> String {
        #[cfg(any(feature = "stat"))]
        {
            let stats = GLOBAL_STAT.lock().unwrap();
            stats.to_string()
        }
        #[cfg(not(feature = "stat"))]
        {
            "Stat is disabled".to_string()
        }
    }

    /// System transaction that allocates a new page.
    fn allocate_page(&self) -> FrameWriteGuard {
        let mut foster_page = loop {
            let page = self.mem_pool.create_new_page_for_write(self.c_key);
            match page {
                Ok(page) => break page,
                Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                    std::hint::spin_loop();
                }
                _ => {
                    panic!("Unexpected error");
                }
            }
        };
        foster_page.init();
        // Write log
        // {
        //     let log_record = LogRecord::SysTxnAllocPage {
        //         txn_id: 0,
        //         page_id: page_key.page_id,
        //     };
        //     let lsn = self.wal_buffer.append_log(&log_record.to_bytes());
        //     foster_page.set_lsn(lsn);
        // }
        foster_page
    }

    fn read_page(&self, page_key: PageKey) -> FrameReadGuard {
        let page = loop {
            let page = self.mem_pool.get_page_for_read(page_key);
            match page {
                Ok(page) => break page,
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    std::hint::spin_loop();
                }
                _ => {
                    panic!("Unexpected error");
                }
            }
        };
        page
    }

    fn insert_inner(&self, this: &mut Page, key: &[u8], value: &[u8]) {
        if !this.insert(key, value, false) {
            // Split the page
            let mut foster_child = self.allocate_page();
            split_insert(this, &mut foster_child, key, value);
        }
    }

    fn modify_structure_if_needed_for_read<'a>(
        &self,
        is_foster_relationship: bool,
        this: FrameReadGuard<'a>,
        child: FrameReadGuard<'a>,
        op_byte: &mut OpByte,
    ) -> (Option<OpType>, FrameReadGuard<'a>, FrameReadGuard<'a>) {
        let op = if is_foster_relationship {
            should_modify_structure_for_foster_relationship(&this, &child, op_byte)
        } else {
            should_modify_structure_for_parent_child_relationship(&this, &child, op_byte)
        };
        if let Some(op) = op {
            log_trace!(
                "Should modify structure: {:?}, This: {}, Child: {}",
                op,
                this.get_id(),
                child.get_id()
            );
            let (mut this, mut child) = match this.try_upgrade(false) {
                Ok(this) => {
                    let child = child.try_upgrade(false);
                    match child {
                        Ok(child) => (this, child),
                        Err(child) => {
                            return (None, this.downgrade(), child);
                        }
                    }
                }
                Err(this) => {
                    return (None, this, child);
                }
            };
            log_trace!(
                "Ready to modify structure: {:?}, This: {}, Child: {}",
                op,
                this.get_id(),
                child.get_id()
            );
            // make both pages dirty
            this.dirty().store(true, Ordering::Relaxed);
            child.dirty().store(true, Ordering::Relaxed);
            self.modify_structure(op.clone(), &mut this, &mut child);
            (Some(op), this.downgrade(), child.downgrade())
        } else {
            (None, this, child)
        }
    }

    fn modify_structure(&self, op: OpType, this: &mut Page, child: &mut Page) {
        match op {
            OpType::MERGE => {
                // Merge the foster child into this page
                merge(this, child);
            }
            OpType::LOADBALANCE => {
                // Load balance between this and the foster child
                balance(this, child);
            }
            OpType::ADOPT => {
                // Adopt the foster child of the child page
                adopt(this, child);
            }
            OpType::ANTIADOPT => {
                // Anti-adopt the foster child of the child page
                anti_adopt(this, child);
            }
            OpType::ASCENDROOT => {
                let mut new_page = self.allocate_page();
                new_page.init();
                // Ascend the root page to the parent page
                ascend_root(this, &mut new_page);
            }
            OpType::DESCENDROOT => {
                // Descend the root page to the child page
                descend_root(this, child);
            }
            _ => {
                panic!("Unexpected operation");
            }
        }
    }

    fn traverse_to_leaf_for_read(&self, key: &[u8]) -> Result<FrameReadGuard, TreeStatus> {
        let mut current_page = self.read_page(self.root_key);
        let mut op_byte = OpByte::new();
        loop {
            let this_page = current_page;
            log_trace!("Traversal for read, page: {}", this_page.get_id());
            if this_page.is_leaf() {
                if this_page.has_foster_child() && this_page.get_foster_key() <= key {
                    // Check whether the foster child should be traversed.
                    let foster_page_key = PageKey::new(self.c_key, this_page.get_foster_page_id());
                    let foster_page = self.read_page(foster_page_key);
                    // Now we have two locks. We need to release the lock of the current page.
                    let (op, this_page, foster_page) = self.modify_structure_if_needed_for_read(
                        true,
                        this_page,
                        foster_page,
                        &mut op_byte,
                    );
                    match op {
                        Some(OpType::MERGE) | Some(OpType::LOADBALANCE) => {
                            current_page = this_page;
                            continue;
                        }
                        None | Some(OpType::ASCENDROOT) => {
                            // Start from the child
                            op_byte.reset();
                            current_page = foster_page;
                            continue;
                        }
                        _ => {
                            panic!("Unexpected operation");
                        }
                    }
                } else {
                    return Ok(this_page);
                }
            }
            let (is_foster_relationship, page_key) = {
                let slot_id = this_page.lower_bound_slot_id(&BTreeKey::new(key));
                let is_foster_relationship =
                    this_page.has_foster_child() && slot_id == this_page.foster_child_slot_id();
                let page_id_bytes = this_page.get_val(slot_id);
                let page_id = PageId::from_be_bytes(page_id_bytes.try_into().unwrap());
                (is_foster_relationship, PageKey::new(self.c_key, page_id))
            };

            let next_page = self.read_page(page_key);
            // Now we have two locks. We need to release the lock of the current page.
            let (op, this_page, next_page) = self.modify_structure_if_needed_for_read(
                is_foster_relationship,
                this_page,
                next_page,
                &mut op_byte,
            );
            match op {
                Some(OpType::MERGE)
                | Some(OpType::LOADBALANCE)
                | Some(OpType::DESCENDROOT)
                | Some(OpType::ADOPT) => {
                    current_page = this_page;
                    continue;
                }
                None | Some(OpType::ASCENDROOT) | Some(OpType::ANTIADOPT) => {
                    // Start from the child
                    op_byte.reset();
                    current_page = next_page;
                    continue;
                }
                _ => {
                    panic!("Unexpected operation");
                }
            }
        }
    }

    fn traverse_to_leaf_for_write(&self, key: &[u8]) -> Result<FrameWriteGuard, TreeStatus> {
        let mut current_page = self.read_page(self.root_key);
        let mut op_byte = OpByte::new();
        loop {
            let this_page = current_page;
            log_trace!("Traversal for write, page: {}", this_page.get_id());
            if this_page.is_leaf() {
                if this_page.has_foster_child() && this_page.get_foster_key() <= key {
                    // Check whether the foster child should be traversed.
                    let foster_page_key = PageKey::new(self.c_key, this_page.get_foster_page_id());
                    let foster_page = self.read_page(foster_page_key);
                    // Now we have two locks. We need to release the lock of the current page.
                    let (op, this_page, foster_page) = self.modify_structure_if_needed_for_read(
                        true,
                        this_page,
                        foster_page,
                        &mut op_byte,
                    );
                    match op {
                        Some(OpType::MERGE) | Some(OpType::LOADBALANCE) => {
                            current_page = this_page;
                            continue;
                        }
                        None | Some(OpType::ASCENDROOT) => {
                            // Start from the child
                            op_byte.reset();
                            current_page = foster_page;
                            continue;
                        }
                        _ => {
                            panic!("Unexpected operation");
                        }
                    }
                } else {
                    match this_page.try_upgrade(true) {
                        Ok(upgraded) => {
                            return Ok(upgraded);
                        }
                        Err(_) => {
                            // If upgrade fails, it means that the page is read by another transaction.
                            // Release everything and retry.
                            return Err(TreeStatus::WriteLockFailed);
                        }
                    }
                }
            }
            let (is_foster_relation_ship, page_key) = {
                let slot_id = this_page.lower_bound_slot_id(&BTreeKey::new(key));
                let is_foster_relationship =
                    this_page.has_foster_child() && slot_id == this_page.foster_child_slot_id();
                let page_id_bytes = this_page.get_val(slot_id);
                let page_id = PageId::from_be_bytes(page_id_bytes.try_into().unwrap());
                (is_foster_relationship, PageKey::new(self.c_key, page_id))
            };

            let next_page = self.read_page(page_key);
            // Now we have two locks. We need to release the lock of the current page.
            let (op, this_page, next_page) = self.modify_structure_if_needed_for_read(
                is_foster_relation_ship,
                this_page,
                next_page,
                &mut op_byte,
            );
            match op {
                Some(OpType::MERGE)
                | Some(OpType::LOADBALANCE)
                | Some(OpType::DESCENDROOT)
                | Some(OpType::ADOPT) => {
                    current_page = this_page;
                    continue;
                }
                None | Some(OpType::ASCENDROOT) | Some(OpType::ANTIADOPT) => {
                    // Start from the child
                    op_byte.reset();
                    current_page = next_page;
                    continue;
                }
                _ => {
                    panic!("Unexpected operation");
                }
            }
        }
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

    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), TreeStatus> {
        let base = Duration::from_millis(1);
        let mut attmepts = 0;
        let mut leaf_page = {
            loop {
                match self.traverse_to_leaf_for_write(key) {
                    Ok(leaf_page) => {
                        break leaf_page;
                    }
                    Err(TreeStatus::WriteLockFailed) => {
                        attmepts += 1;
                        log_trace!(
                            "Failed to acquire write lock (#attempt {}). Sleeping for {:?}",
                            attmepts,
                            base * attmepts
                        );
                        std::thread::sleep(base * attmepts);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        };
        log_trace!("Acquired write lock for page {}", leaf_page.get_id());

        let slot_id = leaf_page.lower_bound_slot_id(&BTreeKey::new(key));
        if slot_id == 0 {
            // Lower fence so insert is ok
            self.insert_inner(&mut leaf_page, key, value);
            Ok(())
        } else {
            if leaf_page.get_raw_key(slot_id) == key {
                // Exact match
                Err(TreeStatus::Duplicate)
            } else {
                self.insert_inner(&mut leaf_page, key, value);
                Ok(())
            }
        }
    }

    fn debug_traverse_to_leaf_for_read(&self, key: &[u8]) -> Result<FrameReadGuard, TreeStatus> {
        let mut current_page = self.read_page(self.root_key);
        let mut op_byte = OpByte::new();
        loop {
            let this_page = current_page;
            print_page(&this_page);
            if this_page.is_leaf() {
                if this_page.has_foster_child() && this_page.get_foster_key() <= key {
                    // Check whether the foster child should be traversed.
                    let foster_page_key = PageKey::new(self.c_key, this_page.get_foster_page_id());
                    let foster_page = self.read_page(foster_page_key);
                    // Now we have two locks. We need to release the lock of the current page.
                    let (op, this_page, foster_page) = self.modify_structure_if_needed_for_read(
                        true,
                        this_page,
                        foster_page,
                        &mut op_byte,
                    );
                    match op {
                        Some(OpType::MERGE) | Some(OpType::LOADBALANCE) => {
                            current_page = this_page;
                            continue;
                        }
                        None | Some(OpType::ASCENDROOT) => {
                            // Start from the child
                            op_byte.reset();
                            current_page = foster_page;
                            continue;
                        }
                        _ => {
                            panic!("Unexpected operation");
                        }
                    }
                } else {
                    return Ok(this_page);
                }
            }
            let (is_foster_relationship, page_key) = {
                let slot_id = this_page.lower_bound_slot_id(&BTreeKey::new(key));
                let is_foster_relationship =
                    this_page.has_foster_child() && slot_id == this_page.foster_child_slot_id();
                let page_id_bytes = this_page.get_val(slot_id);
                let page_id = PageId::from_be_bytes(page_id_bytes.try_into().unwrap());
                (is_foster_relationship, PageKey::new(self.c_key, page_id))
            };

            let next_page = self.read_page(page_key);
            // Now we have two locks. We need to release the lock of the current page.
            let (op, this_page, next_page) = self.modify_structure_if_needed_for_read(
                is_foster_relationship,
                this_page,
                next_page,
                &mut op_byte,
            );
            match op {
                Some(OpType::MERGE)
                | Some(OpType::LOADBALANCE)
                | Some(OpType::DESCENDROOT)
                | Some(OpType::ADOPT) => {
                    current_page = this_page;
                    continue;
                }
                None | Some(OpType::ASCENDROOT) | Some(OpType::ANTIADOPT) => {
                    // Start from the child
                    op_byte.reset();
                    current_page = next_page;
                    continue;
                }
                _ => {
                    panic!("Unexpected operation");
                }
            }
        }
    }

    fn debug_traverse_to_leaf_for_write(&self, key: &[u8]) -> Result<FrameWriteGuard, TreeStatus> {
        let mut current_page = self.read_page(self.root_key);
        let mut op_byte = OpByte::new();
        loop {
            let this_page = current_page;
            print_page(&this_page);
            if this_page.is_leaf() {
                if this_page.has_foster_child() && this_page.get_foster_key() <= key {
                    // Check whether the foster child should be traversed.
                    let foster_page_key = PageKey::new(self.c_key, this_page.get_foster_page_id());
                    let foster_page = self.read_page(foster_page_key);
                    // Now we have two locks. We need to release the lock of the current page.
                    let (op, this_page, foster_page) = self.modify_structure_if_needed_for_read(
                        true,
                        this_page,
                        foster_page,
                        &mut op_byte,
                    );
                    match op {
                        Some(OpType::MERGE) | Some(OpType::LOADBALANCE) => {
                            current_page = this_page;
                            continue;
                        }
                        None | Some(OpType::ASCENDROOT) => {
                            // Start from the child
                            op_byte.reset();
                            current_page = foster_page;
                            continue;
                        }
                        _ => {
                            panic!("Unexpected operation");
                        }
                    }
                } else {
                    match this_page.try_upgrade(true) {
                        Ok(upgraded) => {
                            return Ok(upgraded);
                        }
                        Err(_) => {
                            // If upgrade fails, it means that the page is read by another transaction.
                            // Release everything and retry.
                            return Err(TreeStatus::WriteLockFailed);
                        }
                    }
                }
            }
            let (is_foster_relationship, page_key) = {
                let slot_id = this_page.lower_bound_slot_id(&BTreeKey::new(key));
                let is_foster_relationship =
                    this_page.has_foster_child() && slot_id == this_page.foster_child_slot_id();
                let page_id_bytes = this_page.get_val(slot_id);
                let page_id = PageId::from_be_bytes(page_id_bytes.try_into().unwrap());
                (is_foster_relationship, PageKey::new(self.c_key, page_id))
            };

            let next_page = self.read_page(page_key);
            // Now we have two locks. We need to release the lock of the current page.
            let (op, this_page, next_page) = self.modify_structure_if_needed_for_read(
                is_foster_relationship,
                this_page,
                next_page,
                &mut op_byte,
            );
            match op {
                Some(OpType::MERGE)
                | Some(OpType::LOADBALANCE)
                | Some(OpType::DESCENDROOT)
                | Some(OpType::ADOPT) => {
                    current_page = this_page;
                    continue;
                }
                None | Some(OpType::ASCENDROOT) | Some(OpType::ANTIADOPT) => {
                    // Start from the child
                    op_byte.reset();
                    current_page = next_page;
                    continue;
                }
                _ => {
                    panic!("Unexpected operation");
                }
            }
        }
    }

    pub fn debug_insert(&self, key: &[u8], value: &[u8]) -> Result<(), TreeStatus> {
        let base = Duration::from_millis(1);
        let mut attmepts = 0;
        let mut leaf_page = {
            loop {
                match self.debug_traverse_to_leaf_for_write(key) {
                    Ok(leaf_page) => {
                        break leaf_page;
                    }
                    Err(TreeStatus::WriteLockFailed) => {
                        attmepts += 1;
                        std::thread::sleep(base * attmepts);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        };

        let slot_id = leaf_page.lower_bound_slot_id(&BTreeKey::new(key));
        if slot_id == 0 {
            // Lower fence so insert is ok
            self.insert_inner(&mut leaf_page, key, value);
            Ok(())
        } else {
            if leaf_page.get_raw_key(slot_id) == key {
                // Exact match
                Err(TreeStatus::Duplicate)
            } else {
                self.insert_inner(&mut leaf_page, key, value);
                Ok(())
            }
        }
    }

    fn debug_get_key(&self, key: &[u8]) -> Result<Vec<u8>, TreeStatus> {
        let foster_page = self.debug_traverse_to_leaf_for_read(key)?;
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

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::Write,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        thread,
    };

    use tempfile::TempDir;

    use crate::{
        buffer_pool::CacheEvictionPolicy,
        foster_btree::foster_btree::{
            adopt, anti_adopt, ascend_root, balance, descend_root, is_large, is_small, merge,
            print_page, should_adopt, should_antiadopt, should_load_balance, should_merge,
            should_root_ascend, should_root_descend, MIN_BYTES_USED,
        },
        log, log_trace,
        page::Page,
        random::{gen_random_byte_vec, gen_random_permutation, RandomKVs},
    };

    use super::{
        create_new, BufferPool, BufferPoolRef, ContainerKey, FosterBtree, FosterBtreePage,
        InMemPool, PageKey, MAX_BYTES_USED,
    };

    fn get_buffer_pool(db_id: u16) -> (TempDir, BufferPoolRef) {
        let temp_dir = TempDir::new().unwrap();
        // create a directory for the database
        std::fs::create_dir(temp_dir.path().join(db_id.to_string())).unwrap();
        let num_frames = 10;
        let ep = CacheEvictionPolicy::new(num_frames);
        let bp = BufferPoolRef::new(BufferPool::new(temp_dir.path(), num_frames, ep).unwrap());
        (temp_dir, bp)
    }

    fn get_btree(db_id: u16) -> (TempDir, FosterBtree<BufferPool>) {
        let (temp_dir, bp) = get_buffer_pool(db_id);
        let c_key = ContainerKey::new(db_id, 0);
        let txn_id = 0;
        let btree = create_new(txn_id, c_key, bp.clone());
        (temp_dir, btree)
    }

    fn to_bytes(num: usize) -> Vec<u8> {
        num.to_be_bytes().to_vec()
    }

    fn get_kvs(range: std::ops::Range<usize>) -> Vec<(Vec<u8>, Vec<u8>)> {
        range.map(|i| (to_bytes(i), to_bytes(i))).collect()
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

    fn test_page_merge_detail(
        k0: usize,
        k1: usize,
        k2: usize,
        left: Vec<usize>,
        right: Vec<usize>,
    ) {
        // check left and right are sorted and in the correct range
        assert!(left.iter().all(|&x| k0 <= x && x < k1));
        if left.len() > 0 {
            for i in 0..left.len() - 1 {
                assert!(left[i] < left[i + 1]);
            }
        }
        assert!(right.iter().all(|&x| k1 <= x && x < k2));
        if right.len() > 0 {
            for i in 0..right.len() - 1 {
                assert!(right[i] < right[i + 1]);
            }
        }
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let (temp_dir, bp) = get_buffer_pool(db_id);
        let mut p0 = bp.create_new_page_for_write(c_key).unwrap();
        let mut p1 = bp.create_new_page_for_write(c_key).unwrap();

        let k0 = to_bytes(k0);
        let k1 = to_bytes(k1);
        let k2 = to_bytes(k2);

        p0.init();
        p0.set_level(0);
        p0.set_low_fence(&k0);
        p0.set_high_fence(&k2);
        p0.set_has_foster_child(true);
        p0.append_sorted(&left.iter().map(|&x| (to_bytes(x), to_bytes(x))).collect());
        p0.insert(&k1, &p1.get_id().to_be_bytes(), false);

        p1.init();
        p1.set_level(0);
        p1.set_low_fence(&k1);
        p1.set_high_fence(&k2);
        p1.append_sorted(&right.iter().map(|&x| (to_bytes(x), to_bytes(x))).collect());

        // Run consistency checks
        p0.run_consistency_checks(true);
        p1.run_consistency_checks(true);

        // Merge p1 into p0
        merge(&mut p0, &mut p1);

        // Run consistency checks
        p0.run_consistency_checks(false);
        p1.run_consistency_checks(false);
        // Check the contents of p0
        assert_eq!(p0.active_slot_count() as usize, left.len() + right.len());
        for i in 0..left.len() {
            let key = p0.get_raw_key((i + 1) as u16);
            assert_eq!(key, to_bytes(left[i]));
            let val = p0.get_val((i + 1) as u16);
            assert_eq!(val, to_bytes(left[i]));
        }
        for i in 0..right.len() {
            let key = p0.get_raw_key((i + 1 + left.len()) as u16);
            assert_eq!(key, to_bytes(right[i]));
            let val = p0.get_val((i + 1 + left.len()) as u16);
            assert_eq!(val, to_bytes(right[i]));
        }
        assert_eq!(p0.get_low_fence().as_ref(), k0);
        assert_eq!(p0.get_high_fence().as_ref(), k2);
        assert!(!p0.has_foster_child());

        assert!(p1.empty());

        // print_page(&p0);
        // print_page(&p1);

        drop(temp_dir);
    }

    #[test]
    fn test_page_merge() {
        test_page_merge_detail(10, 20, 30, vec![], vec![]);
        test_page_merge_detail(10, 20, 30, vec![10, 15], vec![20, 25, 29]);
        test_page_merge_detail(10, 20, 30, vec![], vec![20, 25]);
        test_page_merge_detail(10, 20, 30, vec![10, 15], vec![]);
    }

    fn test_page_balance_detail(
        k0: usize,
        k1: usize,
        k2: usize,
        left: Vec<usize>,
        right: Vec<usize>,
    ) {
        // check left and right are sorted and in the correct range
        assert!(left.iter().all(|&x| k0 <= x && x < k1));
        if left.len() > 0 {
            for i in 0..left.len() - 1 {
                assert!(left[i] < left[i + 1]);
            }
        }
        assert!(right.iter().all(|&x| k1 <= x && x < k2));
        if right.len() > 0 {
            for i in 0..right.len() - 1 {
                assert!(right[i] < right[i + 1]);
            }
        }
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let (temp_dir, bp) = get_buffer_pool(db_id);
        let mut p0 = bp.create_new_page_for_write(c_key).unwrap();
        let mut p1 = bp.create_new_page_for_write(c_key).unwrap();

        let k0 = to_bytes(k0);
        let k1 = to_bytes(k1);
        let k2 = to_bytes(k2);

        p0.init();
        p0.set_level(0);
        p0.set_low_fence(&k0);
        p0.set_high_fence(&k2);
        p0.set_has_foster_child(true);
        p0.append_sorted(&left.iter().map(|&x| (to_bytes(x), to_bytes(x))).collect());
        p0.insert(&k1, &p1.get_id().to_be_bytes(), false);

        p1.init();
        p1.set_level(0);
        p1.set_low_fence(&k1);
        p1.set_high_fence(&k2);
        p1.append_sorted(&right.iter().map(|&x| (to_bytes(x), to_bytes(x))).collect());

        // Run consistency checks
        p0.run_consistency_checks(true);
        p1.run_consistency_checks(true);

        let prev_diff = p0.total_bytes_used().abs_diff(p1.total_bytes_used());

        // Balance p0 and p1
        balance(&mut p0, &mut p1);

        // Run consistency checks
        p0.run_consistency_checks(false);
        p1.run_consistency_checks(false);

        // If balanced, half of the total keys should be in p0 and the other half should be in p1
        let total_keys = left.len() + right.len();
        let total_keys_in_pages = p0.active_slot_count() + p1.active_slot_count();
        assert_eq!(total_keys + 1, total_keys_in_pages as usize); // +1 because of the foster key

        let all = left.iter().chain(right.iter()).collect::<Vec<_>>();
        for i in 0..p0.active_slot_count() as usize - 1 {
            let key = p0.get_raw_key((i + 1) as u16);
            assert_eq!(key, to_bytes(*all[i]));
        }
        for i in 0..p1.active_slot_count() as usize {
            let key = p1.get_raw_key((i + 1) as u16);
            assert_eq!(key, to_bytes(*all[i + p0.active_slot_count() as usize - 1]));
        }
        assert_eq!(p0.get_low_fence().as_ref(), k0);
        assert_eq!(p0.get_high_fence().as_ref(), k2);
        assert!(p0.has_foster_child());
        assert_eq!(p0.get_foster_page_id(), p1.get_id());
        assert_eq!(p0.get_foster_key(), p1.get_raw_key(0));
        assert_eq!(p1.get_high_fence().as_ref(), k2);

        let new_diff = p0.total_bytes_used().abs_diff(p1.total_bytes_used());
        assert!(new_diff <= prev_diff);

        // print_page(&p0);
        // print_page(&p1);
        // println!("Prev diff: {}, New diff: {}", prev_diff, new_diff);

        drop(temp_dir);
    }

    #[test]
    fn test_page_balance() {
        test_page_balance_detail(10, 20, 30, vec![], vec![]);
        test_page_balance_detail(10, 20, 30, vec![10, 15], vec![]);
        test_page_balance_detail(10, 20, 30, vec![], vec![20, 25, 29]);
        test_page_balance_detail(10, 20, 30, vec![10, 11, 12, 13, 14, 15], vec![20, 21]);
    }

    fn test_page_adopt_detail(
        k0: usize,
        k1: usize,
        k2: usize,
        left: Vec<usize>,
        right: Vec<usize>,
    ) {
        // check left and right are sorted and in the correct range
        assert!(left.iter().all(|&x| k0 <= x && x < k1));
        if left.len() > 0 {
            for i in 0..left.len() - 1 {
                assert!(left[i] < left[i + 1]);
            }
        }
        assert!(right.iter().all(|&x| k1 <= x && x < k2));
        if right.len() > 0 {
            for i in 0..right.len() - 1 {
                assert!(right[i] < right[i + 1]);
            }
        }
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let (temp_dir, bp) = get_buffer_pool(db_id);
        let mut parent = bp.create_new_page_for_write(c_key).unwrap();
        let mut child0 = bp.create_new_page_for_write(c_key).unwrap();
        let mut child1 = bp.create_new_page_for_write(c_key).unwrap();

        let k0 = to_bytes(k0);
        let k1 = to_bytes(k1);
        let k2 = to_bytes(k2);

        parent.init();
        parent.set_low_fence(&k0);
        parent.set_high_fence(&k2);
        parent.set_level(1);
        parent.insert(&k0, &child0.get_id().to_be_bytes(), false);

        child0.init();
        child0.set_low_fence(&k0);
        child0.set_high_fence(&k2);
        child0.set_level(0);
        child0.append_sorted(&left.iter().map(|&x| (to_bytes(x), to_bytes(x))).collect());
        child0.set_has_foster_child(true);
        child0.insert(&k1, &child1.get_id().to_be_bytes(), false);

        child1.init();
        child1.set_low_fence(&k1);
        child1.set_high_fence(&k2);
        child1.set_level(0);
        child1.append_sorted(&right.iter().map(|&x| (to_bytes(x), to_bytes(x))).collect());

        // Run consistency checks
        parent.run_consistency_checks(true);
        child0.run_consistency_checks(true);
        child1.run_consistency_checks(true);
        assert_eq!(parent.active_slot_count(), 1);
        assert_eq!(child0.active_slot_count() as usize, left.len() + 1);
        assert_eq!(child1.active_slot_count() as usize, right.len());
        assert_eq!(parent.get_val(1), child0.get_id().to_be_bytes());
        assert!(child0.has_foster_child());
        assert_eq!(child0.get_foster_key(), child1.get_raw_key(0));
        assert_eq!(child0.get_foster_page_id(), child1.get_id());

        // Adopt
        adopt(&mut parent, &mut child0);

        // Run consistency checks
        parent.run_consistency_checks(false);
        child0.run_consistency_checks(false);
        child1.run_consistency_checks(false);

        // Check the contents of parent
        assert_eq!(parent.active_slot_count(), 2);
        assert_eq!(parent.get_val(1), child0.get_id().to_be_bytes());
        assert_eq!(parent.get_val(2), child1.get_id().to_be_bytes());
        assert_eq!(child0.active_slot_count() as usize, left.len());
        assert!(!child0.has_foster_child());
        for i in 0..left.len() {
            let key = child0.get_raw_key((i + 1) as u16);
            assert_eq!(key, to_bytes(left[i]));
            let val = child0.get_val((i + 1) as u16);
            assert_eq!(val, to_bytes(left[i]));
        }
        assert_eq!(child1.active_slot_count() as usize, right.len());
        for i in 0..right.len() {
            let key = child1.get_raw_key((i + 1) as u16);
            assert_eq!(key, to_bytes(right[i]));
            let val = child1.get_val((i + 1) as u16);
            assert_eq!(val, to_bytes(right[i]));
        }

        drop(temp_dir);
    }

    #[test]
    fn test_page_adopt() {
        test_page_adopt_detail(10, 20, 30, vec![], vec![]);
        test_page_adopt_detail(10, 20, 30, vec![10, 15], vec![20, 25, 29]);
        test_page_adopt_detail(10, 20, 30, vec![], vec![20, 25]);
        test_page_adopt_detail(10, 20, 30, vec![10, 15], vec![]);
    }

    fn test_page_anti_adopt_detail(
        k0: usize,
        k1: usize,
        k2: usize,
        left: Vec<usize>,
        right: Vec<usize>,
    ) {
        // check left and right are sorted and in the correct range
        assert!(left.iter().all(|&x| k0 <= x && x < k1));
        if left.len() > 0 {
            for i in 0..left.len() - 1 {
                assert!(left[i] < left[i + 1]);
            }
        }
        assert!(right.iter().all(|&x| k1 <= x && x < k2));
        if right.len() > 0 {
            for i in 0..right.len() - 1 {
                assert!(right[i] < right[i + 1]);
            }
        }
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let (temp_dir, bp) = get_buffer_pool(db_id);
        let mut parent = bp.create_new_page_for_write(c_key).unwrap();
        let mut child0 = bp.create_new_page_for_write(c_key).unwrap();
        let mut child1 = bp.create_new_page_for_write(c_key).unwrap();

        let k0 = to_bytes(k0);
        let k1 = to_bytes(k1);
        let k2 = to_bytes(k2);

        parent.init();
        parent.set_low_fence(&k0);
        parent.set_high_fence(&k2);
        parent.set_level(1);
        parent.insert(&k0, &child0.get_id().to_be_bytes(), false);
        parent.insert(&k1, &child1.get_id().to_be_bytes(), false);

        child0.init();
        child0.set_low_fence(&k0);
        child0.set_high_fence(&k1);
        child0.set_level(0);
        child0.append_sorted(&left.iter().map(|&x| (to_bytes(x), to_bytes(x))).collect());

        child1.init();
        child1.set_low_fence(&k1);
        child1.set_high_fence(&k2);
        child1.set_level(0);
        child1.append_sorted(&right.iter().map(|&x| (to_bytes(x), to_bytes(x))).collect());

        // Run consistency checks
        parent.run_consistency_checks(true);
        child0.run_consistency_checks(true);
        child1.run_consistency_checks(true);
        assert_eq!(parent.active_slot_count(), 2);
        assert_eq!(child0.active_slot_count() as usize, left.len());
        assert_eq!(child1.active_slot_count() as usize, right.len());

        // print_page(&parent);
        // print_page(&child0);
        // print_page(&child1);

        // Anti-adopt
        anti_adopt(&mut parent, &mut child0);

        // Run consistency checks
        parent.run_consistency_checks(false);
        child0.run_consistency_checks(false);
        child1.run_consistency_checks(false);

        // Check the contents of parent
        assert_eq!(parent.active_slot_count(), 1);
        assert_eq!(parent.get_val(1), child0.get_id().to_be_bytes());

        assert_eq!(child0.active_slot_count() as usize, left.len() + 1);
        assert_eq!(child0.get_low_fence().as_ref(), k0);
        assert_eq!(child0.get_high_fence().as_ref(), k2);
        assert!(child0.has_foster_child());
        assert_eq!(child0.get_foster_key(), child1.get_raw_key(0));
        assert_eq!(child0.get_foster_page_id(), child1.get_id());
        for i in 0..left.len() {
            let key = child0.get_raw_key((i + 1) as u16);
            assert_eq!(key, to_bytes(left[i]));
            let val = child0.get_val((i + 1) as u16);
            assert_eq!(val, to_bytes(left[i]));
        }

        assert_eq!(child1.active_slot_count() as usize, right.len());
        assert_eq!(child1.get_low_fence().as_ref(), k1);
        assert_eq!(child1.get_high_fence().as_ref(), k2);
        for i in 0..right.len() {
            let key = child1.get_raw_key((i + 1) as u16);
            assert_eq!(key, to_bytes(right[i]));
            let val = child1.get_val((i + 1) as u16);
            assert_eq!(val, to_bytes(right[i]));
        }

        // print_page(&parent);
        // print_page(&child0);
        // print_page(&child1);

        drop(temp_dir);
    }

    #[test]
    fn test_page_anti_adopt() {
        test_page_anti_adopt_detail(10, 20, 30, vec![], vec![]);
        test_page_anti_adopt_detail(10, 20, 30, vec![10, 15], vec![20, 25, 29]);
        test_page_anti_adopt_detail(10, 20, 30, vec![], vec![20, 25]);
        test_page_anti_adopt_detail(10, 20, 30, vec![10, 15], vec![]);
    }

    #[test]
    fn test_root_page_ascend() {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let (temp_dir, bp) = get_buffer_pool(db_id);
        let mut root = bp.create_new_page_for_write(c_key).unwrap();
        let mut foster_child = bp.create_new_page_for_write(c_key).unwrap();
        let mut child = bp.create_new_page_for_write(c_key).unwrap();

        // Before:
        // root [-inf, +inf) --> foster_child [k0, +inf)
        //
        // After:
        // root [-inf, +inf)
        //  +-------------------+
        //  |                   |
        //  v                   v
        // child [-inf, k0)    foster_child [k0, +inf)

        root.init_as_root();
        // Insert 10 slots into the root. Add foster child at the end.
        for i in 1..=10 {
            let key = to_bytes(i);
            root.insert(&key, &key, false);
        }
        let foster_key = to_bytes(11);
        root.insert(&foster_key, &foster_child.get_id().to_be_bytes(), false);
        root.set_has_foster_child(true);

        foster_child.init();
        foster_child.set_low_fence(&foster_key);
        foster_child.set_high_fence(&[]);

        // Insert 10 slots into the foster child
        for i in 11..=20 {
            let key = to_bytes(i);
            foster_child.insert(&key, &key, false);
        }

        child.init();

        // Run consistency checks
        root.run_consistency_checks(true);
        foster_child.run_consistency_checks(true);
        child.run_consistency_checks(true);

        print_page(&root);
        print_page(&foster_child);
        print_page(&child);

        ascend_root(&mut root, &mut child);

        // Run consistency checks
        root.run_consistency_checks(false);
        foster_child.run_consistency_checks(true);
        child.run_consistency_checks(true);
        assert_eq!(root.active_slot_count(), 2);
        assert_eq!(root.level(), 1);
        assert_eq!(root.has_foster_child(), false);
        assert_eq!(root.get_raw_key(1), &[]);
        assert_eq!(root.get_val(1), child.get_id().to_be_bytes());
        assert_eq!(root.get_raw_key(2), &foster_key);
        assert_eq!(root.get_val(2), foster_child.get_id().to_be_bytes());
        assert_eq!(child.active_slot_count(), 10);
        assert_eq!(child.level(), 0);
        assert_eq!(child.has_foster_child(), false);
        assert_eq!(child.is_left_most(), true);
        for i in 1..=child.active_slot_count() {
            let key = child.get_raw_key(i);
            let val = child.get_val(i);
            assert_eq!(key, to_bytes(i as usize));
            assert_eq!(val, to_bytes(i as usize));
        }
        assert_eq!(foster_child.active_slot_count(), 10);
        assert_eq!(foster_child.level(), 0);
        assert_eq!(foster_child.has_foster_child(), false);
        for i in 1..=foster_child.active_slot_count() {
            let key = foster_child.get_raw_key(i);
            let val = foster_child.get_val(i);
            assert_eq!(key, to_bytes(i as usize + 10));
            assert_eq!(val, to_bytes(i as usize + 10));
        }

        print_page(&root);
        print_page(&child);
        print_page(&foster_child);

        drop(temp_dir);
    }

    #[test]
    fn test_root_page_descend() {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let (temp_dir, bp) = get_buffer_pool(db_id);
        let mut root = bp.create_new_page_for_write(c_key).unwrap();
        let mut child = bp.create_new_page_for_write(c_key).unwrap();

        // Before:
        //   root [k0, k1)
        //    |
        //    v
        //   child [k0, k1)
        //
        //
        // After:
        //   root [k0, k1)

        root.init_as_root();
        root.increment_level();
        root.insert(&[], &child.get_id().to_be_bytes(), false);

        child.init();
        child.set_low_fence(&[]);
        child.set_high_fence(&[]);
        // Insert 10 slots
        for i in 1..=10 {
            let key = to_bytes(i);
            child.insert(&key, &key, false);
        }
        child.set_level(0);

        // Run consistency checks
        root.run_consistency_checks(false);
        child.run_consistency_checks(false);

        descend_root(&mut root, &mut child);

        // Run consistency checks
        root.run_consistency_checks(false);
        child.run_consistency_checks(false);
        assert_eq!(root.active_slot_count(), 10);
        assert_eq!(root.level(), 0);
        assert_eq!(root.has_foster_child(), false);
        assert!(child.empty());
        assert_eq!(child.active_slot_count(), 0);
        for i in 1..=root.active_slot_count() {
            let key = root.get_raw_key(i);
            let val = root.get_val(i);
            assert_eq!(key, to_bytes(i as usize));
            assert_eq!(val, to_bytes(i as usize));
        }

        print_page(&root);
        print_page(&child);

        drop(temp_dir);
    }

    // Foster relationship between two pages.
    // Returns (temp_dir, bp, this_page_id, foster_page_id)
    // If is_root is true, then this_page is a root page.
    fn build_foster_relationship(
        is_root: bool,
        this_size: usize,
        foster_size: usize,
    ) -> (TempDir, BufferPoolRef, PageKey, PageKey) {
        let this_size = this_size as u16;
        let foster_size = foster_size as u16;
        // Create a foster relationship between two pages.
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let (temp_dir, bp) = get_buffer_pool(db_id);
        let mut this = bp.create_new_page_for_write(c_key).unwrap();
        let mut foster = bp.create_new_page_for_write(c_key).unwrap();

        // This [k0, k2) --> Foster [k1, k2)

        let k0 = to_bytes(10);
        let k1 = to_bytes(20);
        let k2 = to_bytes(30);

        if is_root {
            this.init_as_root();
        } else {
            this.init();
        }
        this.set_low_fence(&k0);
        this.set_high_fence(&k2);
        this.set_has_foster_child(true);
        this.insert(&k1, &foster.get_id().to_be_bytes(), false);
        {
            // Insert a slot into this page so that the total size of the page is this_size
            let current_size = this.total_bytes_used();
            let val_size = this_size - current_size - this.bytes_needed(&k0, &[]);
            let val = vec![2_u8; val_size as usize];
            this.insert(&k0, &val, false);
        }
        assert_eq!(this.total_bytes_used(), this_size);

        foster.init();
        foster.set_low_fence(&k1);
        foster.set_high_fence(&k2);
        foster.set_right_most(this.is_right_most());
        foster.set_has_foster_child(false);
        {
            // Insert a slot into foster page so that the total size of the page is foster_size
            let current_size = foster.total_bytes_used();
            let val_size = foster_size - current_size - this.bytes_needed(&k1, &[]);
            let val = vec![2_u8; val_size as usize];
            foster.insert(&k1, &val, false);
        }

        // Run consistency checks
        this.run_consistency_checks(true);
        foster.run_consistency_checks(true);

        let this_id = PageKey::new(c_key, this.get_id());
        let foster_id = PageKey::new(c_key, foster.get_id());

        drop(this);
        drop(foster);

        (temp_dir, bp, this_id, foster_id)
    }

    // In a foster relationship, MERGE, BALANCE, ROOT_ASCEND is allowed.
    // [0, MIN_BYTES_USED) is small
    // [MIN_BYTES_USED, MAX_BYTES_USED) is normal.
    // [MAX_BYTES_USED, PAGE_SIZE) is large
    // There are 3 * 3 = 9 possible size
    // S: Small, N: Normal, L: Large
    // SS, NS, SN: Merge
    // SL, LS: Load balance
    // NN, LN, NL, LL: Do nothing
    // Root ascend is allowed only when the foster page is the root page.
    #[test]
    fn test_foster_relationship_structure_modification_criteria() {
        {
            // Should merge, root_ascend
            // Should not balance
            {
                let (temp_dir, bp, this_id, foster_id) =
                    build_foster_relationship(true, MIN_BYTES_USED - 1, MIN_BYTES_USED);
                let this = bp.get_page_for_write(this_id).unwrap();
                let foster = bp.get_page_for_write(foster_id).unwrap();
                assert!(is_small(&this));
                assert!(!is_large(&foster));
                assert!(should_merge(&this, &foster));
                assert!(should_root_ascend(&this, &foster));
                assert!(!should_load_balance(&this, &foster));
            }
            {
                let (temp_dir, bp, this_id, foster_id) =
                    build_foster_relationship(true, MIN_BYTES_USED, MIN_BYTES_USED - 1);
                let this = bp.get_page_for_write(this_id).unwrap();
                let foster = bp.get_page_for_write(foster_id).unwrap();
                assert!(!is_large(&this));
                assert!(is_small(&foster));
                assert!(should_merge(&this, &foster));
                assert!(should_root_ascend(&this, &foster));
                assert!(!should_load_balance(&this, &foster));
            }
            {
                let (temp_dir, bp, this_id, foster_id) =
                    build_foster_relationship(true, MIN_BYTES_USED - 1, MIN_BYTES_USED - 1);
                let this = bp.get_page_for_write(this_id).unwrap();
                let foster = bp.get_page_for_write(foster_id).unwrap();
                assert!(is_small(&this));
                assert!(is_small(&foster));
                assert!(should_merge(&this, &foster));
                assert!(should_root_ascend(&this, &foster));
                assert!(!should_load_balance(&this, &foster));
            }
        }
        {
            // Should load balance
            // Should not merge, root_ascend
            {
                let (temp_dir, bp, this_id, foster_id) =
                    build_foster_relationship(false, MIN_BYTES_USED - 1, MAX_BYTES_USED);
                let this = bp.get_page_for_write(this_id).unwrap();
                let foster = bp.get_page_for_write(foster_id).unwrap();
                assert!(is_small(&this));
                assert!(is_large(&foster));
                assert!(!should_merge(&this, &foster));
                assert!(!should_root_ascend(&this, &foster));
                assert!(should_load_balance(&this, &foster));
            }
            {
                let (temp_dir, bp, this_id, foster_id) =
                    build_foster_relationship(false, MAX_BYTES_USED, MIN_BYTES_USED - 1);
                let this = bp.get_page_for_write(this_id).unwrap();
                let foster = bp.get_page_for_write(foster_id).unwrap();
                assert!(is_large(&this));
                assert!(is_small(&foster));
                assert!(!should_merge(&this, &foster));
                assert!(!should_root_ascend(&this, &foster));
                assert!(should_load_balance(&this, &foster));
            }
        }
        {
            // Should not merge, balance, root_ascend
            {
                let (temp_dir, bp, this_id, foster_id) =
                    build_foster_relationship(false, MAX_BYTES_USED, MAX_BYTES_USED);
                let this = bp.get_page_for_write(this_id).unwrap();
                let foster = bp.get_page_for_write(foster_id).unwrap();
                assert!(is_large(&this));
                assert!(is_large(&foster));
                assert!(!should_merge(&this, &foster));
                assert!(!should_root_ascend(&this, &foster));
                assert!(!should_load_balance(&this, &foster));
            }
            {
                let (temp_dir, bp, this_id, foster_id) =
                    build_foster_relationship(false, MIN_BYTES_USED, MIN_BYTES_USED);
                let this = bp.get_page_for_write(this_id).unwrap();
                let foster = bp.get_page_for_write(foster_id).unwrap();
                assert!(!is_small(&this));
                assert!(!is_small(&foster));
                assert!(!should_merge(&this, &foster));
                assert!(!should_root_ascend(&this, &foster));
                assert!(!should_load_balance(&this, &foster));
            }
        }
    }

    fn build_two_children_tree(child0_size: usize) -> (TempDir, BufferPoolRef, PageKey, PageKey) {
        let child0_size = child0_size as u16;

        // Create a parent with two children.
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let (temp_dir, bp) = get_buffer_pool(db_id);
        let mut parent = bp.create_new_page_for_write(c_key).unwrap();
        let mut child0 = bp.create_new_page_for_write(c_key).unwrap();
        let mut child1 = bp.create_new_page_for_write(c_key).unwrap();

        // Parent [k0, k2)
        //  +-------------------+
        //  |                   |
        //  v                   v
        // Child0 [k0, k1)     Child1 [k1, k2)

        let k0 = to_bytes(10);
        let k1 = to_bytes(20);
        let k2 = to_bytes(30);

        parent.init();
        parent.set_low_fence(&k0);
        parent.set_high_fence(&k2);
        parent.set_level(1);
        parent.insert(&k0, &child0.get_id().to_be_bytes(), false);
        parent.insert(&k1, &child1.get_id().to_be_bytes(), false);

        child0.init();
        child0.set_low_fence(&k0);
        child0.set_high_fence(&k2);
        child0.set_level(0);
        {
            // Insert a slot into child0 page so that the total size of the page is child0_size
            let current_size = child0.total_bytes_used();
            let val_size = child0_size - current_size - child0.bytes_needed(&k0, &[]);
            let val = vec![2_u8; val_size as usize];
            child0.insert(&k0, &val, false);
        }

        child1.init();
        child1.set_low_fence(&k1);
        child1.set_high_fence(&k2);
        child1.set_level(0);

        // Run consistency checks
        parent.run_consistency_checks(true);
        child0.run_consistency_checks(true);
        child1.run_consistency_checks(true);

        let parent_id = PageKey::new(c_key, parent.get_id());
        let child0_id = PageKey::new(c_key, child0.get_id());

        drop(parent);
        drop(child0);
        drop(child1);

        (temp_dir, bp, parent_id, child0_id)
    }

    fn build_single_child_with_foster_child_tree(
        child0_size: usize,
    ) -> (TempDir, BufferPoolRef, PageKey, PageKey) {
        let child0_size = child0_size as u16;

        // Create a parent with a child and a foster child.
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let (temp_dir, bp) = get_buffer_pool(db_id);
        let mut parent = bp.create_new_page_for_write(c_key).unwrap();
        let mut child0 = bp.create_new_page_for_write(c_key).unwrap();
        let mut child1 = bp.create_new_page_for_write(c_key).unwrap();

        // Parent [k0, k2)
        //  |
        //  v
        // Child0 [k0, k2) --> Child1 [k1, k2)

        let k0 = to_bytes(10);
        let k1 = to_bytes(20);
        let k2 = to_bytes(30);

        parent.init();
        parent.set_low_fence(&k0);
        parent.set_high_fence(&k2);
        parent.set_level(1);
        parent.insert(&k0, &child0.get_id().to_be_bytes(), false);

        child0.init();
        child0.set_low_fence(&k0);
        child0.set_high_fence(&k2);
        child0.set_level(0);
        child0.set_has_foster_child(true);
        child0.insert(&k1, &child1.get_id().to_be_bytes(), false);
        {
            // Insert a slot into child0 page so that the total size of the page is child0_size
            let current_size = child0.total_bytes_used();
            let val_size = child0_size - current_size - child0.bytes_needed(&k0, &[]);
            let val = vec![2_u8; val_size as usize];
            child0.insert(&k0, &val, false);
        }

        child1.init();
        child1.set_low_fence(&k1);
        child1.set_high_fence(&k2);
        child1.set_level(0);

        // Run consistency checks
        parent.run_consistency_checks(true);
        child0.run_consistency_checks(true);
        child1.run_consistency_checks(true);

        let parent_id = PageKey::new(c_key, parent.get_id());
        let child0_id = PageKey::new(c_key, child0.get_id());

        drop(parent);
        drop(child0);
        drop(child1);

        (temp_dir, bp, parent_id, child0_id)
    }

    fn build_single_child_tree(child0_size: usize) -> (TempDir, BufferPoolRef, PageKey, PageKey) {
        let child0_size = child0_size as u16;

        // Create a parent with a child.
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let (temp_dir, bp) = get_buffer_pool(db_id);
        let mut parent = bp.create_new_page_for_write(c_key).unwrap();
        let mut child0 = bp.create_new_page_for_write(c_key).unwrap();

        // Parent [k0, k2)
        //  |
        //  v
        // Child0 [k0, k2)

        let k0 = to_bytes(10);
        // let k1 = to_bytes(20);
        let k2 = to_bytes(30);

        parent.init_as_root();
        parent.set_low_fence(&k0);
        parent.set_high_fence(&k2);
        parent.set_level(1);
        parent.insert(&k0, &child0.get_id().to_be_bytes(), false);

        child0.init();
        child0.set_low_fence(&k0);
        child0.set_high_fence(&k2);
        child0.set_left_most(true);
        child0.set_right_most(true);
        child0.set_level(0);
        {
            // Insert a slot into child0 page so that the total size of the page is child0_size
            let current_size = child0.total_bytes_used();
            let val_size = child0_size - current_size - child0.bytes_needed(&k0, &[]);
            let val = vec![2_u8; val_size as usize];
            child0.insert(&k0, &val, false);
        }

        // Run consistency checks
        parent.run_consistency_checks(true);
        child0.run_consistency_checks(true);

        print_page(&parent);
        print_page(&child0);

        let parent_id = PageKey::new(c_key, parent.get_id());
        let child0_id = PageKey::new(c_key, child0.get_id());

        drop(parent);
        drop(child0);

        (temp_dir, bp, parent_id, child0_id)
    }

    // In a parent-child relationship, ADOPT, ANTI_ADOPT, ROOT_DESCEND is allowed.
    // [0, MIN_BYTES_USED) is small
    // [MIN_BYTES_USED, MAX_BYTES_USED) is normal.
    // [MAX_BYTES_USED, PAGE_SIZE) is large
    // There are 3 * 3 * 3 = 9 possible size
    // S: Small, N: Normal, L: Large
    // SS, NS, LS: AntiAdopt if parent has more than 1 child.
    // SN, SL, NN, NL: Adopt if child has foster child.
    // LN, LL: Nothing
    #[test]
    fn test_parent_child_relationship_structure_modification_criteria() {
        {
            // Parent [k0, k2)
            //  +-------------------+
            //  |                   |
            //  v                   v
            // Child0 [k0, k1)     Child1 [k1, k2)
            {
                // Should anti_adopt
                // Should not adopt, root descend
                let (temp_dir, bp, parent_id, child0_id) =
                    build_two_children_tree(MIN_BYTES_USED - 1);
                let parent = bp.get_page_for_write(parent_id).unwrap();
                let child0 = bp.get_page_for_write(child0_id).unwrap();
                assert!(is_small(&child0));
                assert!(should_antiadopt(&parent, &child0));
                assert!(!should_adopt(&parent, &child0));
                assert!(!should_root_descend(&parent, &child0));
            }
            {
                // Should not adopt, anti_adopt, root_descend
                let (temp_dir, bp, parent_id, child0_id) = build_two_children_tree(MIN_BYTES_USED);
                let parent = bp.get_page_for_write(parent_id).unwrap();
                let child0 = bp.get_page_for_write(child0_id).unwrap();
                assert!(!is_small(&child0));
                assert!(!should_antiadopt(&parent, &child0));
                assert!(!should_adopt(&parent, &child0));
                assert!(!should_root_descend(&parent, &child0));
            }
        }
        {
            // Parent [k0, k2)
            //  |
            //  v
            // Child0 [k0, k2) --> Child1 [k1, k2)
            {
                // Should adopt
                // Should not anti_adopt, root_descend
                let (temp_dir, bp, parent_id, child0_id) =
                    build_single_child_with_foster_child_tree(MIN_BYTES_USED);
                let parent = bp.get_page_for_write(parent_id).unwrap();
                let child0 = bp.get_page_for_write(child0_id).unwrap();
                assert!(!is_small(&child0));
                assert!(should_adopt(&parent, &child0));
                assert!(!should_antiadopt(&parent, &child0));
                assert!(!should_root_descend(&parent, &child0));
            }
            {
                // Should not adopt, anti_adopt, root_descend
                let (temp_dir, bp, parent_id, child0_id) =
                    build_single_child_with_foster_child_tree(MIN_BYTES_USED - 1);
                let parent = bp.get_page_for_write(parent_id).unwrap();
                let child0 = bp.get_page_for_write(child0_id).unwrap();
                assert!(is_small(&child0));
                assert!(!should_adopt(&parent, &child0));
                assert!(!should_antiadopt(&parent, &child0));
                assert!(!should_root_descend(&parent, &child0));
            }
        }
        {
            // Parent [k0, k2)
            //  |
            //  v
            // Child0 [k0, k2)
            {
                // Should not adopt, anti_adopt, root_descend
                let (temp_dir, bp, parent_id, child0_id) =
                    build_single_child_tree(MIN_BYTES_USED - 1);
                let parent = bp.get_page_for_write(parent_id).unwrap();
                let child0 = bp.get_page_for_write(child0_id).unwrap();
                assert!(!should_adopt(&parent, &child0));
                assert!(!should_antiadopt(&parent, &child0));
                assert!(should_root_descend(&parent, &child0));
            }
        }
    }

    fn setup_btree_empty() -> (TempDir, FosterBtree<BufferPool>) {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let (temp_dir, bp) = get_buffer_pool(db_id);

        let root_key = {
            let mut root = bp.create_new_page_for_write(c_key).unwrap();
            root.init_as_root();
            root.key().unwrap()
        };

        let btree = FosterBtree {
            c_key,
            root_key,
            mem_pool: bp.clone(),
        };
        (temp_dir, btree)
    }

    #[test]
    fn test_sorted_insertion() {
        let (temp_dir, btree) = setup_btree_empty();
        // Insert 1024 bytes
        let val = vec![2_u8; 1024];
        for i in 0..10 {
            println!(
                "**************************** Inserting key {} **************************",
                i
            );
            let key = to_bytes(i);
            btree.insert(&key, &val).unwrap();
        }
        for i in 0..10 {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(i);
            let current_val = btree.get_key(&key).unwrap();
            assert_eq!(current_val, val);
        }
        drop(temp_dir);
    }

    fn setup_inmem_btree_empty() -> FosterBtree<InMemPool> {
        use super::MemPool;
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let mem_pool = Arc::new(InMemPool::new());

        let root_key = {
            let mut root = mem_pool.create_new_page_for_write(c_key).unwrap();
            root.init_as_root();
            root.key().unwrap()
        };

        let btree = FosterBtree {
            c_key,
            root_key,
            mem_pool,
        };
        btree
    }

    #[test]
    fn test_random_insertion() {
        let btree = setup_inmem_btree_empty();
        // Insert 1024 bytes
        let val = vec![3_u8; 1024];
        let order = [6, 3, 8, 1, 5, 7, 2, 4, 9, 0];
        for i in order.iter() {
            println!(
                "**************************** Inserting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            btree.debug_insert(&key, &val).unwrap();
        }
        for i in order.iter() {
            println!(
                "**************************** Getting key {} **************************",
                i
            );
            let key = to_bytes(*i);
            let current_val = btree.get_key(&key).unwrap();
            assert_eq!(current_val, val);
        }
    }

    #[test]
    fn test_stress() {
        let num_keys = 5000;
        let val_min_size = 50;
        let val_max_size = 100;
        let kvs = RandomKVs::new(num_keys, val_min_size, val_max_size);

        let btree = setup_inmem_btree_empty();

        // Write kvs to file
        // let kvs_file = "kvs.dat";
        // // serde cbor to write to file
        // let mut file = File::create(kvs_file).unwrap();
        // let kvs_str = serde_cbor::to_vec(&kvs).unwrap();
        // file.write_all(&kvs_str).unwrap();

        for (i, (key, val)) in kvs.iter().enumerate() {
            println!(
                "**************************** Inserting {} key={} **************************",
                i, key
            );
            let key = to_bytes(*key);
            btree.insert(&key, val).unwrap();
        }

        for (key, val) in kvs.iter() {
            println!(
                "**************************** Getting key {} **************************",
                key
            );
            let key = to_bytes(*key);
            let current_val = btree.get_key(&key).unwrap();
            assert_eq!(current_val, *val);
        }

        println!("SUCCESS");
    }

    // skip default
    #[test]
    #[ignore]
    fn replay_stress() {
        let btree = setup_inmem_btree_empty();

        let kvs_file = "kvs.dat";
        let file = File::open(kvs_file).unwrap();
        let kvs: RandomKVs = serde_cbor::from_reader(file).unwrap();

        let bug_occurred_at = 1138;
        for (i, (key, val)) in kvs.iter().enumerate() {
            if i == bug_occurred_at {
                break;
            }
            println!(
                "**************************** Inserting {} key={} **************************",
                i, key
            );
            let key = to_bytes(*key);
            btree.insert(&key, val).unwrap();
        }

        let (k, v) = &kvs[bug_occurred_at];
        println!(
            "BUG INSERT ************** Inserting {} key={} **************************",
            bug_occurred_at, k
        );
        let key = to_bytes(*k);
        btree.debug_insert(&key, &v).unwrap();

        /*
        for (i, (key, val)) in kvs.iter().enumerate() {
            println!(
                "**************************** Getting {} key={} **************************",
                i,
                key
            );
            let key = to_bytes(*key);
            let current_val = btree.get_key(&key).unwrap();
            assert_eq!(current_val, *val);
        }
        */

        // let dot_string = btree.generate_dot();
        // let dot_file = "btree.dot";
        // let mut file = File::create(dot_file).unwrap();
        // // write dot_string as txt
        // file.write_all(dot_string.as_bytes()).unwrap();
    }

    #[test]
    fn test_parallel_insertion() {
        // init_test_logger();
        let btree = Arc::new(setup_inmem_btree_empty());
        let num_keys = 5000;
        let val_min_size = 50;
        let val_max_size = 100;
        let kvs = Arc::new(RandomKVs::new(num_keys, val_min_size, val_max_size));

        log_trace!("Number of keys: {}", num_keys);

        // Use 3 threads to insert keys into the tree.
        // Increment the counter for each key inserted and if the counter is equal to the number of keys, then all keys have been inserted.
        let counter = Arc::new(AtomicUsize::new(0));
        thread::scope(
            // issue three threads to insert keys into the tree
            |s| {
                for i in 0..3 {
                    let btree = btree.clone();
                    let kvs = kvs.clone();
                    let counter = counter.clone();
                    s.spawn(move || {
                        log_trace!("Spawned");
                        loop {
                            let counter = counter.fetch_add(1, Ordering::AcqRel);
                            if counter >= num_keys {
                                break;
                            }
                            let (key, val) = &kvs[counter];
                            log_trace!("Inserting key {}", key);
                            let key = to_bytes(*key);
                            btree.insert(&key, val).unwrap();
                        }
                    });
                }
            },
        );

        // Check if all keys have been inserted.
        for (key, val) in kvs.iter() {
            let key = to_bytes(*key);
            let current_val = btree.get_key(&key).unwrap();
            assert_eq!(current_val, *val);
        }
    }
}
