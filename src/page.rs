pub const PAGE_SIZE: usize = 4096;
use std::ops::{Index, IndexMut};

pub struct Page(pub [u8; PAGE_SIZE]);

impl Page {
    pub fn new() -> Self {
        Page([0; PAGE_SIZE])
    }

    pub fn copy(&mut self, other: &Page) {
        self.0.copy_from_slice(&other.0);
    }
}

// Index and IndexMut allow us to use the [] operator on Page.
// This disallows us from touching the header of the page.

impl Index<usize> for Page {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl Index<std::ops::Range<usize>> for Page {
    type Output = [u8];

    fn index(&self, index: std::ops::Range<usize>) -> &Self::Output {
        &self.0[index.start..index.end]
    }
}

impl Index<std::ops::RangeTo<usize>> for Page {
    type Output = [u8];

    fn index(&self, index: std::ops::RangeTo<usize>) -> &Self::Output {
        &self.0[..index.end]
    }
}

impl Index<std::ops::RangeFrom<usize>> for Page {
    type Output = [u8];

    fn index(&self, index: std::ops::RangeFrom<usize>) -> &Self::Output {
        &self.0[index.start..]
    }
}

impl Index<std::ops::RangeFull> for Page {
    type Output = [u8];

    fn index(&self, _index: std::ops::RangeFull) -> &Self::Output {
        &self.0[..]
    }
}

impl IndexMut<usize> for Page {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}

impl IndexMut<std::ops::Range<usize>> for Page {
    fn index_mut(&mut self, index: std::ops::Range<usize>) -> &mut Self::Output {
        &mut self.0[index.start..index.end]
    }
}

impl IndexMut<std::ops::RangeTo<usize>> for Page {
    fn index_mut(&mut self, index: std::ops::RangeTo<usize>) -> &mut Self::Output {
        &mut self.0[..index.end]
    }
}

impl IndexMut<std::ops::RangeFrom<usize>> for Page {
    fn index_mut(&mut self, index: std::ops::RangeFrom<usize>) -> &mut Self::Output {
        &mut self.0[index.start..]
    }
}

impl IndexMut<std::ops::RangeFull> for Page {
    fn index_mut(&mut self, _index: std::ops::RangeFull) -> &mut Self::Output {
        &mut self.0[..]
    }
}
