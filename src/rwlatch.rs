use std::sync::atomic::{AtomicI16, Ordering};

pub struct RwLatch {
    pub cnt: AtomicI16,
}

impl Default for RwLatch {
    fn default() -> Self {
        RwLatch {
            cnt: AtomicI16::new(0), // Up to 2^15 readers or 1 writer
        }
    }
}

impl RwLatch {
    pub fn shared(&self) {
        let mut expected: i16;
        loop {
            expected = self.cnt.load(Ordering::Acquire);
            if expected >= 0
                && self.cnt.compare_exchange(
                    expected,
                    expected + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) == Ok(expected)
            {
                break;
            }
        }
    }

    pub fn exclusive(&self) {
        let mut expected: i16;
        loop {
            expected = self.cnt.load(Ordering::Acquire);
            if expected == 0
                && self
                    .cnt
                    .compare_exchange(expected, -1, Ordering::AcqRel, Ordering::Acquire)
                    == Ok(expected)
            {
                break;
            }
        }
    }

    pub fn upgrade(&self) {
        let mut expected: i16;
        loop {
            expected = self.cnt.load(Ordering::Acquire);
            if expected == 1
                && self
                    .cnt
                    .compare_exchange(expected, -1, Ordering::AcqRel, Ordering::Acquire)
                    == Ok(expected)
            {
                break;
            }
        }
    }

    pub fn downgrade(&self) {
        let mut expected: i16;
        loop {
            expected = self.cnt.load(Ordering::Acquire);
            if expected == -1
                && self
                    .cnt
                    .compare_exchange(expected, 1, Ordering::AcqRel, Ordering::Acquire)
                    == Ok(expected)
            {
                break;
            }
        }
    }

    pub fn release_shared(&self) {
        self.cnt.fetch_sub(1, Ordering::Release);
    }

    pub fn release_exclusive(&self) {
        self.cnt.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::UnsafeCell, thread};

    use super::*;

    // Need to wrap the UnsafeCell in a struct to implement Sync
    pub struct Counter {
        cnt: UnsafeCell<usize>,
    }

    impl Counter {
        pub fn new() -> Self {
            Counter {
                cnt: UnsafeCell::new(0),
            }
        }

        pub fn increment(&self) {
            unsafe {
                *self.cnt.get() += 1;
            }
        }

        pub fn read(&self) -> usize {
            unsafe { *self.cnt.get() }
        }
    }

    unsafe impl Sync for Counter {}

    pub struct RwLatchProtectedCounter {
        pub rwlatch: RwLatch,
        pub counter: Counter,
    }

    impl Default for RwLatchProtectedCounter {
        fn default() -> Self {
            RwLatchProtectedCounter {
                rwlatch: RwLatch::default(),
                counter: Counter::new(),
            }
        }
    }

    unsafe impl Sync for RwLatchProtectedCounter {}

    #[test]
    fn test_multiple_writers_consistency() {
        let counter = RwLatchProtectedCounter::default();
        thread::scope(|s| {
            for _ in 0..10000 {
                s.spawn(|| {
                    counter.rwlatch.exclusive();
                    counter.counter.increment();
                    counter.rwlatch.release_exclusive();
                });
            }
        });
        assert_eq!(counter.counter.read(), 10000);
    }

    #[test]
    fn test_multiple_readers_do_not_block() {
        let counter = RwLatchProtectedCounter::default();
        thread::scope(|s| {
            for _ in 0..1000 {
                s.spawn(|| {
                    counter.rwlatch.shared();
                    assert_eq!(counter.counter.read(), 0);
                });
            }
        });
        assert_eq!(counter.counter.read(), 0);
    }
}
