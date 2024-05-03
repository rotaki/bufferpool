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
    let original_kvs = RandomKVs::new(num_keys, val_min_size, val_max_size);
    let mut kvs = original_kvs.partition(num_threads);

    // Check the total #kvs is same as num_keys
    #[cfg(debug_assertions)]
    {
        assert_eq!(kvs.len(), num_threads);
        let total_kvs: usize = kvs.iter().map(|kvs| kvs.len()).sum();
        assert_eq!(total_kvs, num_keys);
    }

    let mut handles = vec![];
    while let Some(kvs) = kvs.pop_front() {
        let btree = Arc::clone(&btree);
        let handle = thread::spawn(move || {
            for (key, val) in kvs.iter() {
                let key = to_bytes(*key, 100);
                btree.insert(&key, val).unwrap();
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }

    println!("stats: \n{}", btree.op_stats());

    // Check if all keys have been inserted.
    #[cfg(debug_assertions)]
    for (key, val) in original_kvs.iter() {
        log_trace!("Checking key: {:?}", key);
        let key = to_bytes(*key, 100);
        let current_val = btree.get(&key).unwrap();
        assert_eq!(current_val, *val);
    }
}

fn run_insertion_bench_single_thread() {
    let btree = Arc::new(setup_inmem_btree_empty());
    let num_keys = 500000;
    let val_min_size = 50;
    let val_max_size = 100;

    let kvs = RandomKVs::new(num_keys, val_min_size, val_max_size);
    for (key, val) in kvs.iter() {
        let key = to_bytes(*key, 100);
        btree.insert(&key, val).unwrap();
    }

    println!("stats: \n{}", btree.op_stats());
}

// main function
// get number of threads from command line
fn main() {
    // let args: Vec<String> = std::env::args().collect();
    // assert_eq!(args.len(), 2, "Usage: ./main <num_threads>");
    // let num_threads = args[1].parse::<usize>().unwrap();
    // run_insertion_bench(num_threads);
    // run_insertion_bench_single_thread();
    run_insertion_bench(2)
}

