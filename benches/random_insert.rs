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
    buffer_pool::{
        prelude::{
            get_in_mem_pool, get_test_bp, DummyEvictionPolicy, EvictionPolicy, LRUEvictionPolicy,
        },
        BufferPoolForTest, ContainerKey, InMemPool, MemPool,
    },
    foster_btree::{FosterBtree, FosterBtreePage},
    random::RandomKVs,
};

const NUM_KEYS: usize = 100000;
const KEY_SIZE: usize = 8;
const VAL_MIN_SIZE: usize = 50;
const VAL_MAX_SIZE: usize = 100;
const NUM_THREADS: usize = 10;
const BP_SIZE: usize = 10000;

fn to_bytes(key: usize) -> Vec<u8> {
    // Pad the key with 0s to make it key_size bytes long.
    // let mut key_vec = vec![0u8; KEY_SIZE];
    // let bytes = key.to_be_bytes().to_vec();
    // key_vec[..bytes.len()].copy_from_slice(&bytes);
    // key_vec
    key.to_be_bytes().to_vec()
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

fn bench_random_insertion(c: &mut Criterion) {
    let kvs = RandomKVs::new(NUM_KEYS, VAL_MIN_SIZE, VAL_MAX_SIZE);
    let partitioned_kvs = kvs.partition(NUM_THREADS);

    let mut group = c.benchmark_group("Random Insertion");
    // group.sample_size(10);

    group.bench_function("In memory Foster BTree Insertion", |b| {
        b.iter(|| insert_into_foster_tree(gen_foster_btree_in_mem(), &kvs));
    });

    group.bench_function("In memory Foster BTree Insertion Parallel", |b| {
        b.iter(|| insert_into_foster_tree_parallel(gen_foster_btree_in_mem(), &partitioned_kvs));
    });

    group.bench_function("On disk Foster BTree Insertion", |b| {
        b.iter(|| insert_into_foster_tree(gen_foster_btree_on_disk(), &kvs));
    });

    group.bench_function("On disk Foster BTree Insertion Parallel", |b| {
        b.iter(|| insert_into_foster_tree_parallel(gen_foster_btree_on_disk(), &partitioned_kvs));
    });

    group.bench_function("BTreeMap Insertion", |b| {
        b.iter(|| black_box(insert_into_btree(&kvs)));
    });

    group.finish();
}

criterion_group!(benches, bench_random_insertion);
criterion_main!(benches);
