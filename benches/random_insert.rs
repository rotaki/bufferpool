use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

use foster_btree::{
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

fn insert_into_foster_tree(kvs: &RandomKVs) {
    let tree = setup_inmem_btree_empty();
    for (k, v) in kvs.iter() {
        let key = to_bytes(*k);
        tree.insert(&key, v).unwrap();
    }
}

fn insert_into_foster_tree_parallel(_num_threads: usize, kvs: &VecDeque<RandomKVs>) {
    let btree = Arc::new(setup_inmem_btree_empty());

    // Scopeed threads
    thread::scope(|s| {
        for partition in kvs.iter() {
            let btree = Arc::clone(&btree);
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

fn bench_random_insertion(c: &mut Criterion) {
    let num_keys = 100000;
    let val_min_size = 50;
    let val_max_size = 100;
    let num_threads = 10;

    let kvs = RandomKVs::new(num_keys, val_min_size, val_max_size);
    let partitioned_kvs = kvs.partition(num_threads);

    let mut group = c.benchmark_group("Random Insertion");
    // group.sample_size(10);

    group.bench_function("Foster BTree Insertion", |b| {
        b.iter(|| black_box(insert_into_foster_tree(&kvs)));
    });

    group.bench_function("Foster BTree Insertion Parallel", |b| {
        b.iter(|| {
            black_box(insert_into_foster_tree_parallel(
                num_threads,
                &partitioned_kvs,
            ))
        });
    });

    group.bench_function("BTreeMap Insertion", |b| {
        b.iter(|| black_box(insert_into_btree(&kvs)));
    });

    group.finish();
}

criterion_group!(benches, bench_random_insertion);
criterion_main!(benches);
