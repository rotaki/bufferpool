use rand::Rng;

use super::eviction_policy::EvictionPolicy;

// Very simple LFU eviction policy
pub struct LFUEvictionPolicy {
    frequency: Vec<usize>,
}

impl LFUEvictionPolicy {
    pub fn new(num_pages: usize) -> Self {
        LFUEvictionPolicy {
            frequency: vec![0; num_pages],
        }
    }
}

impl EvictionPolicy for LFUEvictionPolicy {
    fn choose_victim(&mut self) -> usize {
        let mut min = usize::MAX;
        for &f in self.frequency.iter() {
            if f < min {
                min = f;
            }
        }
        // If there are multiple pages with the same minimum frequency, choose one at random
        let mut candidates = Vec::new();
        for (i, &f) in self.frequency.iter().enumerate() {
            if f == min {
                candidates.push(i);
            }
        }
        let mut rng = rand::thread_rng();
        candidates[rng.gen_range(0..candidates.len())]
    }

    fn reset(&mut self, index: usize) {
        self.frequency[index] = 0;
    }

    fn reset_all(&mut self) {
        for f in self.frequency.iter_mut() {
            *f = 0;
        }
    }

    fn update(&mut self, index: usize) {
        self.frequency[index] += 1;
    }
}
