use std::collections::HashMap;

pub struct RustHashMap {
    func: Box<dyn Fn(&[u8], &[u8]) -> Vec<u8>>,
    pub hm: HashMap<Vec<u8>, Vec<u8>>,
}

impl RustHashMap {
    pub fn new(func: Box<dyn Fn(&[u8], &[u8]) -> Vec<u8>>) -> Self {
        RustHashMap {
            func,
            hm: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>> {
        match self.hm.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let new_value = (self.func)(entry.get(), &value);
                Some(entry.insert(new_value))
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(value);
                None
            }
        }
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>> {
        self.hm.get(key)
    }

    pub fn remove(&mut self, key: &Vec<u8>) -> Option<Vec<u8>> {
        self.hm.remove(key)
    }
}
