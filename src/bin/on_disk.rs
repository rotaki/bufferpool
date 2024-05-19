use clap::Parser;
use foster_btree::{bench_utils::*, random::RandomKVs};
fn main() {
    let bench_params = BenchParams::parse();
    println!("{}", bench_params.to_string());

    let bp_size = bench_params.bp_size;
    let tree = gen_foster_btree_on_disk(bp_size);

    let kvs = RandomKVs::new(
        bench_params.unique_keys,
        bench_params.num_threads,
        bench_params.num_keys,
        bench_params.key_size,
        bench_params.val_min_size,
        bench_params.val_max_size,
    );

    run_bench(bench_params, kvs, tree.clone());

    #[cfg(feature = "stat")]
    {
        println!("BP stats: ");
        println!("{}", tree.mem_pool.eviction_stats());
        println!("File stats: ");
        println!("{}", tree.mem_pool.file_stats());
        println!("Btree op stats: ");
        println!("{}", tree.op_stats());
        println!("Btree page stats: ");
        println!("{}", tree.page_stats(false));
    }
}
