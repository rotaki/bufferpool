use wasm_bindgen::prelude::*;

use crate::buffer_pool::prelude::*;
use crate::foster_btree::foster_btree::MAX_BYTES_USED;
use crate::foster_btree::{FosterBtree, FosterBtreePage};

use std::sync::Arc;

#[wasm_bindgen]
pub struct FosterBtreeVisualizer {
    bt: FosterBtree<InMemPool>,
}

#[wasm_bindgen]
impl FosterBtreeVisualizer {
    #[wasm_bindgen(constructor)]
    pub fn new() -> FosterBtreeVisualizer {
        let (db_id, c_id) = (0, 0);
        let c_key = ContainerKey::new(db_id, c_id);
        let mem_pool = Arc::new(InMemPool::new());

        let root_key = {
            let mut root = mem_pool.create_new_page_for_write(c_key).unwrap();
            root.init_as_root();
            root.key().unwrap()
        };

        let btree = FosterBtree {
            c_key,
            root_key,
            mem_pool: mem_pool.clone(),
        };
        FosterBtreeVisualizer { bt: btree }
    }

    pub fn insert(&mut self, key: usize, payload_size: usize) -> String {
        if payload_size > MAX_BYTES_USED {
            return "Payload size is too large".to_string();
        }
        match self.bt.insert(&key.to_be_bytes(), &vec![1; payload_size]) {
            Ok(_) => {
                format!("Inserted key: {}", key)
            }
            Err(e) => {
                format!("Error: {:?}", e)
            }
        }
    }

    pub fn delete(&mut self, key: usize) -> String {
        unimplemented!("delete")
    }

    pub fn search(&self, key: usize) -> String {
        match self.bt.get_key(&key.to_be_bytes()) {
            Ok(val) => {
                format!("Found key: {}, payload_size: {}", key, val.len())
            }
            Err(e) => {
                format!("Error: {:?}", e)
            }
        }
    }

    pub fn visualize(&self) -> String {
        self.bt.generate_dot()
    }
}
