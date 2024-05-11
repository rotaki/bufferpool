use std::{
    collections::VecDeque,
    fs::File,
    io::Write,
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

fn to_bytes(key: usize, key_size: usize) -> Vec<u8> {
    // Pad the key with 0s to make it key_size bytes long.
    let mut key_vec = vec![0u8; key_size];
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
    let btree = FosterBtree::new(c_key, get_test_bp(1000));
    Arc::new(btree)
}

fn insert_into_foster_tree<E: EvictionPolicy, M: MemPool<E>>(
    btree: Arc<FosterBtree<E, M>>,
    kvs: &RandomKVs,
) {
    for (k, v) in kvs.iter() {
        let key = to_bytes(*k, 100);
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
                    let key = to_bytes(*k, 100);
                    btree.insert(&key, v).unwrap();
                }
            });
        }
    })
}

fn test_stress() {
    let num_keys = 100000;
    let val_min_size = 50;
    let val_max_size = 100;
    let mut kvs = RandomKVs::new(1, num_keys, val_min_size, val_max_size);
    let kvs = kvs.pop().unwrap();

    let btree = gen_foster_btree_in_mem();

    // Write kvs to file
    // let kvs_file = "kvs.dat";
    // // serde cbor to write to file
    // let mut file = File::create(kvs_file).unwrap();
    // let kvs_str = serde_cbor::to_vec(&kvs).unwrap();
    // file.write_all(&kvs_str).unwrap();

    println!("Inserting {} keys", num_keys);
    for (i, (key, val)) in kvs.iter().enumerate() {
        // println!(
        //     "**************************** Inserting {} key={} **************************",
        //     i, key
        // );
        let key = to_bytes(*key, 100);
        btree.insert(&key, val).unwrap();
    }

    btree.check_consistency();
    println!("{}", btree.page_stats(false));

    println!("Getting {} keys", num_keys);
    for (key, val) in kvs.iter() {
        // println!(
        //     "**************************** Getting key {} **************************",
        //     key
        // );
        let key = to_bytes(*key, 100);
        let current_val = btree.get(&key).unwrap();
        assert_eq!(current_val, *val);
    }

    btree.check_consistency();
    println!("{}", btree.page_stats(false));
}

// skip default
fn replay_stress() {
    let btree = gen_foster_btree_in_mem();

    let kvs_file = "kvs.dat";
    let file = File::open(kvs_file).unwrap();
    let kvs: RandomKVs = serde_cbor::from_reader(file).unwrap();

    let bug_occurred_at = 48642;
    for (i, (key, val)) in kvs.iter().enumerate() {
        if i == bug_occurred_at {
            break;
        }
        println!(
            "**************************** Inserting {} key={} **************************",
            i, key
        );
        let key = to_bytes(*key, 100);
        btree.insert(&key, val).unwrap();
    }

    let (k, v) = &kvs[bug_occurred_at];
    println!(
        "BUG INSERT ************** Inserting {} key={} **************************",
        bug_occurred_at, k
    );
    let key = to_bytes(*k, 100);
    btree.insert(&key, &v).unwrap();

    /*
    for (i, (key, val)) in kvs.iter().enumerate() {
        println!(
            "**************************** Getting {} key={} **************************",
            i,
            key
        );
        let key = to_bytes(*key);
        let current_val = btree.get_key(&key).unwrap();
        assert_eq!(current_val, *val);
    }
    */

    // let dot_string = btree.generate_dot();
    // let dot_file = "btree.dot";
    // let mut file = File::create(dot_file).unwrap();
    // // write dot_string as txt
    // file.write_all(dot_string.as_bytes()).unwrap();
}

fn main() {
    println!("Running stress test");
    // for _ in 0..100 {
    //     test_stress();
    // }
    test_stress();
    println!("SUCCESS");
    // replay_stress();
}
