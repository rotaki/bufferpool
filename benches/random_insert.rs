use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

use bp::{
    buffer_pool::prelude::{ContainerKey, InMemPool, MemPool},
    foster_btree::{FosterBtree, FosterBtreePage},
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

fn insert_into_foster_tree(kvs: &Arc<RandomKVs>) {
    let tree = setup_inmem_btree_empty();
    for (k, v) in kvs.iter() {
        let key = to_bytes(*k);
        tree.insert(&key, v).unwrap();
    }
}

fn insert_into_foster_tree_parallel(kvs: &Arc<RandomKVs>) {
    let btree = Arc::new(setup_inmem_btree_empty());
    // Use 3 threads to insert keys into the tree.
    // Increment the counter for each key inserted and if the counter is equal to the number of keys, then all keys have been inserted.
    let counter = Arc::new(AtomicUsize::new(0));
    thread::scope(
        // issue three threads to insert keys into the tree
        |s| {
            for i in 0..10 {
                let btree = btree.clone();
                let kvs = kvs.clone();
                let counter = counter.clone();
                s.spawn(move || loop {
                    let counter = counter.fetch_add(1, Ordering::AcqRel);
                    if counter >= kvs.len() {
                        break;
                    }
                    let (key, val) = &kvs[counter];
                    let key = to_bytes(*key);
                    btree.insert(&key, val).unwrap();
                });
            }
        },
    );
}

fn insert_into_btree(kvs: &Arc<RandomKVs>) {
    let mut tree = BTreeMap::new();
    for (k, v) in kvs.iter() {
        let key = to_bytes(*k);
        tree.insert(key, v.clone());
    }
}

fn bench_random_insertion(c: &mut Criterion) {
    let num_keys = 100000;
    let val_min_size = 50;
    let val_max_size = 100;

    let kvs = RandomKVs::new(num_keys, val_min_size, val_max_size);
    let kvs = Arc::new(kvs);

    let mut group = c.benchmark_group("Random Insertion");
    // group.sample_size(10);

    group.bench_function("Foster BTree Insertion", |b| {
        b.iter(|| black_box(insert_into_foster_tree(&kvs)));
    });

    group.bench_function("Foster BTree Insertion Parallel", |b| {
        b.iter(|| black_box(insert_into_foster_tree_parallel(&kvs)));
    });

    group.bench_function("BTreeMap Insertion", |b| {
        b.iter(|| black_box(insert_into_btree(&kvs)));
    });

    group.finish();
}

criterion_group!(benches, bench_random_insertion);
criterion_main!(benches);
