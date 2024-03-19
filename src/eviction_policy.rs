use rand::Rng;

// Very simple LFU eviction policy
pub struct EvictionPolicy {
    frequency: Vec<usize>,
}

impl EvictionPolicy {
    pub fn new(buffer_size: usize) -> Self {
        EvictionPolicy {
            frequency: vec![0; buffer_size],
        }
    }

    pub fn choose_victim(&self) -> usize {
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

    pub fn reset(&mut self, index: usize) {
        self.frequency[index] = 0;
    }

    pub fn reset_all(&mut self) {
        for f in self.frequency.iter_mut() {
            *f = 0;
        }
    }

    pub fn update(&mut self, index: usize) {
        self.frequency[index] += 1;
    }
}
