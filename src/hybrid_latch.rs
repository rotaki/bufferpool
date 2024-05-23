use std::sync::atomic::{AtomicU64, Ordering};

use crate::rwlatch::RwLatch;

pub struct HybridLatch {
    rwlatch: RwLatch,
    version: AtomicU64,
}

impl Default for HybridLatch {
    fn default() -> Self {
        HybridLatch {
            rwlatch: RwLatch::default(),
            version: AtomicU64::new(0),
        }
    }
}

impl HybridLatch {
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    pub fn check_version(&self, version: u64) -> bool {
        self.version() == version
    }

    pub fn is_locked(&self) -> bool {
        self.rwlatch.is_locked()
    }

    pub fn is_shared(&self) -> bool {
        self.rwlatch.is_shared()
    }

    pub fn is_exclusive(&self) -> bool {
        self.rwlatch.is_exclusive()
    }

    pub fn optimistic(&self) {
        while self.is_exclusive() {
            std::hint::spin_loop();
        }
    }

    pub fn shared(&self) {
        self.rwlatch.shared()
    }

    pub fn try_shared(&self) -> bool {
        self.rwlatch.try_shared()
    }

    pub fn exclusive(&self) {
        self.rwlatch.exclusive()
    }

    pub fn try_exclusive(&self) -> bool {
        self.rwlatch.try_exclusive()
    }

    pub fn upgrade(&self) {
        self.rwlatch.exclusive()
    }

    pub fn try_upgrade(&self) -> bool {
        self.rwlatch.try_upgrade()
    }

    pub fn downgrade(&self)  {
        self.version.fetch_add(1, Ordering::AcqRel);
        self.rwlatch.downgrade()
    }

    pub fn release_shared(&self) {
        self.rwlatch.release_shared()
    }


    pub fn release_exclusive(&self) {
        self.version.fetch_add(1, Ordering::AcqRel);
        self.rwlatch.release_exclusive()
    }
}