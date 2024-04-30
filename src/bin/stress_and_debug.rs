use std::{
    fs::File,
    io::Write,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

use bp::{
    buffer_pool::prelude::{ContainerKey, InMemPool, MemPool},
    foster_btree::{FosterBtree, FosterBtreePage},
    log, log_trace,
    random::RandomKVs,
};

fn to_bytes(key: usize) -> Vec<u8> {
    key.to_be_bytes().to_vec()
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

fn test_stress() {
    let num_keys = 100000;
    let val_min_size = 50;
    let val_max_size = 100;
    let kvs = RandomKVs::new(num_keys, val_min_size, val_max_size);

    let btree = setup_inmem_btree_empty();

    // Write kvs to file
    let kvs_file = "kvs.dat";
    // serde cbor to write to file
    let mut file = File::create(kvs_file).unwrap();
    let kvs_str = serde_cbor::to_vec(&kvs).unwrap();
    file.write_all(&kvs_str).unwrap();

    for (i, (key, val)) in kvs.iter().enumerate() {
        println!(
            "**************************** Inserting {} key={} **************************",
            i, key
        );
        let key = to_bytes(*key);
        btree.insert(&key, val).unwrap();
    }

    // for (key, val) in kvs.iter() {
    //     println!(
    //         "**************************** Getting key {} **************************",
    //         key
    //     );
    //     let key = to_bytes(*key);
    //     let current_val = btree.get_key(&key).unwrap();
    //     assert_eq!(current_val, *val);
    // }
}

// skip default
fn replay_stress() {
    let btree = setup_inmem_btree_empty();

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
        let key = to_bytes(*key);
        btree.insert(&key, val).unwrap();
    }

    let (k, v) = &kvs[bug_occurred_at];
    println!(
        "BUG INSERT ************** Inserting {} key={} **************************",
        bug_occurred_at, k
    );
    let key = to_bytes(*k);
    btree.debug_insert(&key, &v).unwrap();

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
    // println!("Running stress test");
    // for _ in 0..100 {
    //     test_stress();
    // }
    // println!("SUCCESS");
    replay_stress();
}
