use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

use foster_btree::{
    buffer_pool::prelude::{ContainerKey, InMemPool, MemPool},
    foster_btree::{FosterBtree, FosterBtreePage},
    log, log_trace,
    random::RandomKVs,
};

fn to_bytes(key: usize, key_size: usize) -> Vec<u8> {
    // Pad the key with 0s to make it key_size bytes long.
    let mut key_vec = vec![0u8; key_size];
    let bytes = key.to_be_bytes().to_vec();
    key_vec[..bytes.len()].copy_from_slice(&bytes);
    key_vec
}

fn setup_inmem_btree_empty() -> FosterBtree<InMemPool> {
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
        mem_pool,
    };
    btree
}

fn run_insertion_bench(num_threads: usize) {
    assert!(
        num_threads > 0,
        "Number of threads should be greater than 0"
    );
    let btree = Arc::new(setup_inmem_btree_empty());
    let num_keys = 500000;
    let val_min_size = 50;
    let val_max_size = 100;

    log_trace!("Generating {} keys into the tree", num_keys);
    let kvs = Arc::new(RandomKVs::new(num_keys, val_min_size, val_max_size));
    log_trace!("KVs generated");

    // Use 3 threads to insert keys into the tree.
    // Increment the counter for each key inserted and if the counter is equal to the number of keys, then all keys have been inserted.
    let counter = Arc::new(AtomicUsize::new(0));
    thread::scope(
        // issue three threads to insert keys into the tree
        |s| {
            for _ in 0..num_threads {
                let btree = btree.clone();
                let kvs = kvs.clone();
                let counter = counter.clone();
                s.spawn(move || {
                    log_trace!("Thread {:?} started", thread::current().id());
                    loop {
                        let counter = counter.fetch_add(1, Ordering::AcqRel);
                        if counter >= num_keys {
                            break;
                        }
                        let (key, val) = &kvs[counter];
                        log_trace!("Inserting key: {:?}", key);
                        let key = to_bytes(*key, 100);
                        btree.insert(&key, val).unwrap();
                    }
                });
            }
        },
    );

    println!("stats: \n{}", btree.stats());

    // // Check if all keys have been inserted.
    // for (key, val) in kvs.iter() {
    //     log_trace!("Checking key: {:?}", key);
    //     let key = to_bytes(*key, 100);
    //     let current_val = btree.get_key(&key).unwrap();
    //     assert_eq!(current_val, *val);
    // }
}

// main function
// get number of threads from command line
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_threads = args[1].parse::<usize>().unwrap();
    run_insertion_bench(num_threads);
}
