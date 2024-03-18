/*
 * Reference: https://github.com/cacheMon/lru-rs
 * https://github.com/jedisct1/rust-sieve-cache/blob/master/src/lib.rs
 */

use crate::eviction_policy::EvictionPolicy;
use std::{
    collections::{BinaryHeap, HashMap},
    ptr::NonNull,
};

struct Entry {
    index: usize,
    visited: bool,
    prev: Option<NonNull<Entry>>,
    next: Option<NonNull<Entry>>,
}

impl Entry {
    fn new(index: usize) -> Self {
        Entry {
            index,
            visited: false,
            prev: None,
            next: None,
        }
    }
}

pub struct SieveEvictionPolicy {
    map: HashMap<usize, Box<Entry>>,
    next_to_allocate: usize,
    head: Option<NonNull<Entry>>,
    tail: Option<NonNull<Entry>>,
    hand: Option<NonNull<Entry>>,
    capacity: usize,
    len: usize,
}

impl SieveEvictionPolicy {
    pub fn new(num_pages: usize) -> Self {
        let map: HashMap<usize, Box<Entry>> = (0..num_pages)
            .map(|i| (i, Box::new(Entry::new(i))))
            .collect();

        SieveEvictionPolicy {
            map,
            next_to_allocate: 0,
            head: None,
            tail: None,
            hand: None,
            capacity: num_pages,
            len: 0,
        }
    }

    // Establish the link in the doubly linked list
    // Reference: https://github.com/jedisct1/rust-sieve-cache/blob/master/src/lib.rs
    fn add_node(&mut self, mut node: NonNull<Entry>) {
        unsafe {
            node.as_mut().next = self.head;
            node.as_mut().prev = None;
            if let Some(mut head) = self.head {
                head.as_mut().prev = Some(node);
            }
        }
        self.head = Some(node);
        if self.tail.is_none() {
            self.tail = self.head;
        }
    }

    // Remove the link in the doubly linked list
    // Reference: https://github.com/jedisct1/rust-sieve-cache/blob/master/src/lib.rs
    fn remove_node(&mut self, node: NonNull<Entry>) {
        unsafe {
            if let Some(mut prev) = node.as_ref().prev {
                prev.as_mut().next = node.as_ref().next;
            } else {
                self.head = node.as_ref().next;
            }
            if let Some(mut next) = node.as_ref().next {
                next.as_mut().prev = node.as_ref().prev;
            } else {
                self.tail = node.as_ref().prev;
            }
        }
    }

    // Buffer pool is full, evict a page.
    // Reference: https://github.com/jedisct1/rust-sieve-cache/blob/master/src/lib.rs
    fn evict(&mut self) -> usize {
        let mut node = self.hand.or(self.tail);
        while node.is_some() {
            let mut curr_node = node.unwrap();
            unsafe {
                if !curr_node.as_ref().visited {
                    break;
                }
                curr_node.as_mut().visited = false;
                if curr_node.as_ref().prev.is_some() {
                    node = curr_node.as_ref().prev;
                } else {
                    node = self.tail;
                }
            }
        }

        // not sure if we can unwrap here
        let node_to_evict = node.unwrap();
        let index;
        unsafe {
            self.hand = node_to_evict.as_ref().prev;
            index = node_to_evict.as_ref().index;
        }
        self.remove_node(node_to_evict);
        self.len -= 1;
        index
    }
}

impl EvictionPolicy for SieveEvictionPolicy {
    fn choose_victim(&mut self) -> usize {
        let index: usize = if self.len < self.capacity {
            debug_assert!(self.next_to_allocate < self.capacity);
            let next_idx = self.next_to_allocate;
            self.next_to_allocate += 1;
            next_idx
        } else {
            self.evict()
        };
        let entry = self.map.get(&index).unwrap();
        self.add_node(NonNull::from(entry.as_ref()));
        self.len += 1;
        index
    }

    fn reset(&mut self, index: usize) {
        let entry = self.map.get_mut(&index).unwrap();
        entry.visited = false;
    }

    fn update(&mut self, index: usize) {
        let entry = self.map.get_mut(&index).unwrap();
        entry.visited = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sieve_eviction_policy_simple1() {
        let mut policy = SieveEvictionPolicy::new(3);
        assert_eq!(policy.choose_victim(), 0);
        assert_eq!(policy.choose_victim(), 1);
        assert_eq!(policy.choose_victim(), 2);
        policy.update(0);
        policy.update(1);
        policy.update(2);
        assert_eq!(policy.choose_victim(), 0);
    }

    #[test]
    fn test_sieve_eviction_policy_simple2() {
        let mut policy = SieveEvictionPolicy::new(3);
        assert_eq!(policy.choose_victim(), 0);
        assert_eq!(policy.choose_victim(), 1);
        assert_eq!(policy.choose_victim(), 2);
        policy.update(1);
        policy.update(2);
        assert_eq!(policy.choose_victim(), 0);
    }

    #[test]
    fn test_sieve_eviction_policy_simple3() {
        let mut policy = SieveEvictionPolicy::new(3);
        assert_eq!(policy.choose_victim(), 0);
        assert_eq!(policy.choose_victim(), 1);
        assert_eq!(policy.choose_victim(), 2);
        policy.update(0);
        policy.update(2);
        assert_eq!(policy.choose_victim(), 1);
    }

    #[test]
    fn test_sieve_eviction_policy_simple4() {
        let mut policy = SieveEvictionPolicy::new(3);
        assert_eq!(policy.choose_victim(), 0);
        assert_eq!(policy.choose_victim(), 1);
        assert_eq!(policy.choose_victim(), 2);
        policy.update(0);
        policy.update(1);
        policy.update(2);
        assert_eq!(policy.choose_victim(), 0);
        policy.reset(0);
        policy.update(1);
        assert_eq!(policy.choose_victim(), 2);
        assert_eq!(policy.choose_victim(), 0);
    }

    #[test]
    fn test_sieve_eviction_policy_simple5() {
        // Example from official website
        let mut policy = SieveEvictionPolicy::new(7);
        assert_eq!(policy.choose_victim(), 0);
        assert_eq!(policy.choose_victim(), 1);
        assert_eq!(policy.choose_victim(), 2);
        assert_eq!(policy.choose_victim(), 3);
        assert_eq!(policy.choose_victim(), 4);
        assert_eq!(policy.choose_victim(), 5);
        assert_eq!(policy.choose_victim(), 6);
        policy.update(0);
        policy.update(1);
        policy.update(6);
        assert_eq!(policy.choose_victim(), 2);
        policy.update(0);
        policy.update(3);
        assert_eq!(policy.choose_victim(), 4);
        policy.update(1);
        assert_eq!(policy.choose_victim(), 5);
    }
}
