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
        let mut min_index = 0;
        for (i, &f) in self.frequency.iter().enumerate() {
            if f < min {
                min = f;
                min_index = i;
            }
        }
        min_index
    }

    pub fn reset(&mut self, index: usize) {
        self.frequency[index] = 0;
    }

    pub fn update(&mut self, index: usize) {
        self.frequency[index] += 1;
    }
}
