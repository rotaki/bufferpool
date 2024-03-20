mod buffer_frame;
mod buffer_pool;

mod eviction_policy;
use eviction_policy::eviction_policy::EvictionPolicy;
use eviction_policy::lfu_eviction_policy::LFUEvictionPolicy;
// use eviction_policy::sieve_eviction_policy::SieveEvictionPolicy;

use buffer_pool::BufferPool as TempBP;
pub type BufferPool = TempBP<LFUEvictionPolicy>;

pub mod prelude {
    pub use super::buffer_frame::{BufferFrame, FrameReadGuard, FrameWriteGuard};
    pub use super::buffer_pool::{BPStatus, ContainerKey, PageKey};
    pub use super::BufferPool;
}
