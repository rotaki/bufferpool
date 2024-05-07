pub trait KVIterator {
    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)>; // (key, value)
}
