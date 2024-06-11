use clap::Parser;
use fbtree::{bench_utils::*, random::RandomKVs};
fn main() {
    let bench_params = BenchParams::parse();
    println!("{}", bench_params);

    let bp_size = bench_params.bp_size;
    let phm = gen_paged_hash_map_on_disk(bp_size);
    // let phm = gen_paged_hash_map_on_disk_with_hash_eviction_policy(bp_size);

    let kvs = RandomKVs::new(
        bench_params.unique_keys,
        bench_params.num_threads,
        bench_params.num_keys,
        bench_params.key_size,
        bench_params.val_min_size,
        bench_params.val_max_size,
    );

    // run_bench_for_hash_map(bench_params, kvs, phm.clone());
    for partition in kvs.iter() {
        for (k, v) in partition.iter() {
            phm.upsert_with_merge(k, v, |v1: &[u8], v2: &[u8]| -> Vec<u8> {
                v2.to_vec()
            });
        }
    }

    #[cfg(feature = "stat")]
    {
        // println!("Btree op stats: ");
        // println!("{}", tree.op_stats());
        // println!("Btree page stats: ");
        // println!("{}", tree.page_stats(false));
        println!("BP stats: ");
        println!("{}", phm.bp.eviction_stats());
        println!("File stats: ");
        println!("{}", phm.bp.file_stats());
        println!("PagedHashMap stats: ");
        println!("{}", phm.stats());
    }
}
