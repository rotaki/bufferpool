use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

use foster_btree::{
    buffer_pool::{
        get_in_mem_pool, get_test_bp,
        prelude::{
            ContainerKey, DummyEvictionPolicy, EvictionPolicy, InMemPool, LRUEvictionPolicy,
            MemPool,
        },
        BufferPoolForTest,
    },
    foster_btree::{FosterBtree, FosterBtreePage},
    log, log_trace,
    random::RandomKVs,
};

const NUM_KEYS: usize = 500000;
const KEY_SIZE: usize = 100;
const VAL_MIN_SIZE: usize = 50;
const VAL_MAX_SIZE: usize = 100;
const BP_SIZE: usize = 10000;

fn to_bytes(key: usize) -> Vec<u8> {
    // Pad the key with 0s to make it key_size bytes long.
    let mut key_vec = vec![0u8; KEY_SIZE];
    let bytes = key.to_be_bytes().to_vec();
    key_vec[..bytes.len()].copy_from_slice(&bytes);
    key_vec
}

fn gen_foster_btree_in_mem() -> Arc<FosterBtree<DummyEvictionPolicy, InMemPool<DummyEvictionPolicy>>>
{
    let (db_id, c_id) = (0, 0);
    let c_key = ContainerKey::new(db_id, c_id);
    let btree = FosterBtree::new(c_key, get_in_mem_pool());
    Arc::new(btree)
}

fn gen_foster_btree_on_disk(
) -> Arc<FosterBtree<LRUEvictionPolicy, BufferPoolForTest<LRUEvictionPolicy>>> {
    let (db_id, c_id) = (0, 0);
    let c_key = ContainerKey::new(db_id, c_id);
    let btree = FosterBtree::new(c_key, get_test_bp(BP_SIZE));
    Arc::new(btree)
}

fn insert_into_foster_tree<E: EvictionPolicy, M: MemPool<E>>(
    btree: Arc<FosterBtree<E, M>>,
    kvs: &RandomKVs,
) {
    for (k, v) in kvs.iter() {
        let key = to_bytes(*k);
        btree.insert(&key, v).unwrap();
    }
}

fn insert_into_foster_tree_parallel<E: EvictionPolicy, M: MemPool<E>>(
    btree: Arc<FosterBtree<E, M>>,
    kvs: &VecDeque<RandomKVs>,
) {
    // Scopeed threads
    thread::scope(|s| {
        for partition in kvs.iter() {
            let btree = btree.clone();
            s.spawn(move || {
                for (k, v) in partition.iter() {
                    let key = to_bytes(*k);
                    btree.insert(&key, v).unwrap();
                }
            });
        }
    })
}

fn insert_into_btree(kvs: &RandomKVs) {
    let mut tree = BTreeMap::new();
    for (k, v) in kvs.iter() {
        let key = to_bytes(*k);
        tree.insert(key, v.clone());
    }
}

fn run_insertion_bench(num_threads: usize) {
    assert!(
        num_threads > 0,
        "Number of threads should be greater than 0"
    );
    let btree = gen_foster_btree_on_disk();
    let original_kvs = RandomKVs::new(NUM_KEYS, VAL_MIN_SIZE, VAL_MAX_SIZE);
    let mut kvs = original_kvs.partition(num_threads);

    // Check the total #kvs is same as num_keys
    #[cfg(debug_assertions)]
    {
        assert_eq!(kvs.len(), num_threads);
        let total_kvs: usize = kvs.iter().map(|kvs| kvs.len()).sum();
        assert_eq!(total_kvs, NUM_KEYS);
    }

    let mut handles = vec![];
    while let Some(kvs) = kvs.pop_front() {
        let btree = Arc::clone(&btree);
        let handle = thread::spawn(move || {
            for (key, val) in kvs.iter() {
                let key = to_bytes(*key);
                btree.insert(&key, val).unwrap();
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }

    #[cfg(any(feature = "stat"))]
    {
        println!("bp stats: \n{}", btree.mem_pool.eviction_stats());
        println!("foster_op_stats: \n{}", btree.op_stats());
        println!("foster_page_stats: \n{}", btree.page_stats(false));
    }

    // Check if all keys have been inserted.
    #[cfg(debug_assertions)]
    for (key, val) in original_kvs.iter() {
        log_trace!("Checking key: {:?}", key);
        let key = to_bytes(*key);
        let current_val = btree.get(&key).unwrap();
        assert_eq!(current_val, *val);
    }
}

fn run_insertion_bench_single_thread() {
    // let btree = gen_foster_btree_in_mem();
    let btree = gen_foster_btree_on_disk();
    let kvs = RandomKVs::new(NUM_KEYS, VAL_MIN_SIZE, VAL_MAX_SIZE);
    for (key, val) in kvs.iter() {
        let key = to_bytes(*key);
        btree.insert(&key, val).unwrap();
    }

    #[cfg(any(feature = "stat"))]
    {
        println!("bp stats: \n{}", btree.mem_pool.eviction_stats());
        println!("foster_op_stats: \n{}", btree.op_stats());
        println!("foster_page_stats: \n{}", btree.page_stats(false));
    }
}

// main function
// get number of threads from command line
fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(args.len(), 2, "Usage: ./main <num_threads>");
    let num_threads = args[1].parse::<usize>().unwrap();
    if num_threads == 1 {
        run_insertion_bench_single_thread();
    } else {
        run_insertion_bench(num_threads);
    }
}
