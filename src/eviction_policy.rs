pub struct EvictionPolicy {
    buffer_size: usize,
}

impl EvictionPolicy {
    pub fn new(buffer_size: usize) -> Self {
        EvictionPolicy { buffer_size }
    }

    pub fn choose_victim(&self) -> usize {
        // index
        0
    }

    pub fn reset(&self, index: usize) {}

    pub fn update(&self, index: usize) {}
}
