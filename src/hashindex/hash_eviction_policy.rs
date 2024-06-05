use std::sync::atomic::{AtomicU64, Ordering};

use crate::bp::prelude::{BufferFrame, EvictionPolicy};

pub const INITIAL_COUNTER: u64 = 1;
static LRU_COUNTER: AtomicU64 = AtomicU64::new(INITIAL_COUNTER);
    
pub struct HashEvictionPolicy {
    pub score: u64,
}

impl EvictionPolicy for HashEvictionPolicy {
    fn new() -> Self {
        HashEvictionPolicy {
            score: INITIAL_COUNTER,
        }
    }

    fn score(&self, frame: &BufferFrame<Self>) -> u64
    where
        Self: Sized,
    {
        if frame.get_id() == 0 {
            return 0;
        }
        if frame.get_id() < 4097 {
            return self.score + 10;
        }
        self.score
    }

    fn update(&mut self) {
        self.score = LRU_COUNTER.fetch_add(1, Ordering::AcqRel);
    }

    fn reset(&mut self) {
        self.score = INITIAL_COUNTER;
    }
}