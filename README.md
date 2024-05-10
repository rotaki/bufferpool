# TODO

## Buffer pool
* [x] Add reader-writer latch
* [x] Add container file mapping
* [x] Write the evicted page to disk if dirty
* [x] Add logger for debugging
* [x] Add cache replacement implementations
    * [x] Add more eviction algo (SIEVE)
* [x] Add write-ahead log, page lsn
* [] Add pointer swizzling

## Foster B-Tree
* [x] Add Foster B-tree Page
  * [x] Add insert operator
  * [x] Add get operator
  * [x] Add delete operator
  * [x] Add upsert operator
  * [x] Add statistics for each structure modification
    * [x] Revise the split decision
  * [x] Add thread-unsafe page traversal operator
    * [x] Add consistency checker
  * [x] Fix the buffer pool free frame allocation bug
  * [] Add memory usage profiler
  * [x] Test with bp (limited memory pool with disk offloading)
  * [] Reuse the removed page in merge
  * [x] Add range scan operator
  * [] Add thread-safe range scan operator with page versioning (optimistic lock coupling)
  * [] Optimistic lock coupling with hybrid latches
  * [] Add ghost record support for transaction support
  * [] Add prefix compression in pages
  * [] Add page split into three pages
  * [] Add better latch for bp

### Open Questions
* [] How many threads are needed to get comparable performance with a single-threaded execution?

## Add Logging
* [x] Add log buffer with append and flush capabilities


## Visualize foster b-tree
```
wasm-pack build --target web
python3 -m http.server
```
Then open `http://localhost:8000` in your browser.
May need to comment out `criterion` in `Cargo.toml` to build for wasm.


## Multi-thread logger
See `logger.rs` and 
```
cargo run --features "log_trace"
```

## Benchmarking notes

```
cargo bench
```

### Result (temp)

#### Test scenario

Inserting to a Foster BTree and BTreeMap with 100000 keys and values of size 50-100 bytes.
```
    let num_keys = 100000;
    let val_min_size = 50;
    let val_max_size = 100;
```

1. Inserting 100000 keys to Foster BTree single-threaded
2. Inserting 100000 keys to Foster BTree multi-threaded (10 threads)
3. Inserting 100000 keys to BTreeMap


```
     Running benches/random_insert.rs (target/release/deps/random_insert-1e8dd7ac6b41ff2a)
Gnuplot not found, using plotters backend
Random Insertion/In memory Foster BTree Insertion
                        time:   [48.246 ms 48.583 ms 48.990 ms]
                        change: [-1.4311% -0.6377% +0.3116%] (p = 0.14 > 0.05)
                        No change in performance detected.
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high severe
Random Insertion/In memory Foster BTree Insertion Parallel
                        time:   [21.482 ms 21.634 ms 21.789 ms]
                        change: [-3.2891% -1.8410% -0.5282%] (p = 0.01 < 0.05)
                        Change within noise threshold.
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild
Benchmarking Random Insertion/On disk Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 6.7s, or reduce sample count to 70.
Random Insertion/On disk Foster BTree Insertion
                        time:   [66.690 ms 67.001 ms 67.341 ms]
                        change: [-0.0443% +0.5471% +1.2015%] (p = 0.09 > 0.05)
                        No change in performance detected.
Found 13 outliers among 100 measurements (13.00%)
  2 (2.00%) low mild
  3 (3.00%) high mild
  8 (8.00%) high severe
Benchmarking Random Insertion/On disk Foster BTree Insertion Parallel: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 10.0s, or reduce sample count to 40.
Random Insertion/On disk Foster BTree Insertion Parallel
                        time:   [99.642 ms 104.53 ms 109.60 ms]
                        change: [-13.673% -6.7232% +0.9424%] (p = 0.08 > 0.05)
                        No change in performance detected.
Found 4 outliers among 100 measurements (4.00%)
  4 (4.00%) high mild
Benchmarking Random Insertion/BTreeMap Insertion: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 5.7s, or reduce sample count to 80.
Random Insertion/BTreeMap Insertion
                        time:   [56.393 ms 56.881 ms 57.392 ms]
                        change: [+0.9020% +2.2179% +3.4426%] (p = 0.00 < 0.05)
                        Change within noise threshold.
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild
```


## Perf
```
cargo build --release
perf record -e cycles -g --call-graph dwarf ./target/release/main 
hotspot perf.data
```

if sudo is needed, set `echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid` and `echo 0 | sudo tee /proc/sys/kernel/kptr_restrict`