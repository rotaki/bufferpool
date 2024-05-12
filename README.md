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
  * [x] Add memory usage profiler
    * [x] Heaptrack for memory profiling
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

Measured the time taken to insert kvs.

```
unique_keys: true,
num_threads: 3 (when run in parallel),
num_keys: 500000,
key_size: 100,
val_min_size: 50,
val_max_size: 100,
bp_size: 10000 (when run on-disk)
```


```Benchmarking Random Insertion/In memory Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 5.2s.
Benchmarking Random Insertion/In memory Foster BTree Insertion: Collecting 10 samples in estimated 
Random Insertion/In memory Foster BTree Insertion
                        time:   [514.77 ms 516.88 ms 519.51 ms]
                        change: [-1.0819% -0.3517% +0.3521%] (p = 0.41 > 0.05)
                        No change in performance detected.
Found 2 outliers among 10 measurements (20.00%)
  1 (10.00%) high mild
  1 (10.00%) high severe
Benchmarking Random Insertion/In memory Foster BTree Insertion Parallel: Collecting 10 samples in e
Random Insertion/In memory Foster BTree Insertion Parallel
                        time:   [287.53 ms 289.63 ms 292.29 ms]
                        change: [-0.8400% +0.2885% +1.5403%] (p = 0.65 > 0.05)
                        No change in performance detected.
Found 2 outliers among 10 measurements (20.00%)
  2 (20.00%) high severe
Benchmarking Random Insertion/On disk Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 12.2s.
Benchmarking Random Insertion/On disk Foster BTree Insertion: Collecting 10 samples in estimated 12
Random Insertion/On disk Foster BTree Insertion
                        time:   [1.2123 s 1.2167 s 1.2216 s]
                        change: [+0.1426% +0.6233% +1.1586%] (p = 0.03 < 0.05)
                        Change within noise threshold.
Benchmarking Random Insertion/On disk Foster BTree Insertion Parallel: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 11.3s.
Benchmarking Random Insertion/On disk Foster BTree Insertion Parallel: Collecting 10 samples in est
Random Insertion/On disk Foster BTree Insertion Parallel
                        time:   [1.1191 s 1.1252 s 1.1316 s]
                        change: [-2.8239% -2.2421% -1.6455%] (p = 0.00 < 0.05)
                        Performance has improved.
Benchmarking Random Insertion/BTreeMap Insertion: Collecting 10 samples in estimated 8.6512 s (20 i
Random Insertion/BTreeMap Insertion
                        time:   [435.52 ms 442.65 ms 451.68 ms]
                        change: [-1.6347% +0.1575% +2.0383%] (p = 0.89 > 0.05)
                        No change in performance detected.
Found 2 outliers among 10 measurements (20.00%)
  1 (10.00%) high mild
  1 (10.00%) high severe
```


## Perf
```
cargo build --release
perf record -e cycles -g --call-graph dwarf ./target/release/main 
hotspot perf.data
```

if sudo is needed, set `echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid` and `echo 0 | sudo tee /proc/sys/kernel/kptr_restrict`.
To install `perf`, `apt-get install linux-tools-common linux-tools-generic linux-tools-`uname -r``
To install `hotspot`, `apt-get install hotspot`

## Heaptrack
Heaptrack is a heap memory profiler. To install, `apt-get install heaptrack heaptrack-gui`.
To profile, `heaptrack <binary> <my params>`. This will open a gui to analyze the heap memory usage.
To open the gui later, `heaptrack_gui <heaptrack.log>`