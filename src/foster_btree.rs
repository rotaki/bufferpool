
use std::sync::Arc;

use crate::buffer_pool::BufferPool;
use crate::buffer_frame::{BufferFrame, FrameReadGuard, FrameWriteGuard};

type BufferPoolRef = Arc<BufferPool>;

pub struct FosterBtree {
    pub bp: BufferPoolRef,
}


impl FosterBtree {
    pub fn new(bp: BufferPoolRef) -> Self {
        FosterBtree {
            bp,
        }
    }

    
}