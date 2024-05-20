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
  * [x] Add range scan operator
  * [] Reuse the removed page in merge
  * [] Improve page binary search
  * [] Add prefix compression in pages
  * [] Asynchronous page read/write
  * [] Add page split into three pages
  * [] Add better latch for bp
  * [] Add transactional support
  * [] Optimistic lock coupling with hybrid latches with page versioning.
  * [] Add ghost record support for transaction support

### Open Questions

* [] How many threads are needed to get comparable performance with a single-threaded execution?

## Add Logging

* [x] Add log buffer with append and flush capabilities

## Visualize foster b-tree

```sh
wasm-pack build --target web
python3 -m http.server
```

Then open `http://localhost:8000` in your browser.
May need to comment out `criterion` in `Cargo.toml` to build for wasm.

## Multi-thread logger

See `logger.rs` and

```sh
cargo run --features "log_trace"
```

## Benchmarking notes

```sh
cargo bench
```

### Result (temp)

#### Test scenario

Measured the time taken to insert kvs.

```text
unique_keys: true,
num_threads: 3 (when run in parallel),
num_keys: 500000,
key_size: 100,
val_min_size: 50,
val_max_size: 100,
bp_size: 10000 (when run on-disk)
```

```text
Random Insertion/In memory Foster BTree Initial Allocation
                        time:   [204.43 ns 205.03 ns 206.00 ns]
                        change: [-0.1509% +2.1046% +4.0774%] (p = 0.09 > 0.05)
                        No change in performance detected.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe
Benchmarking Random Insertion/In memory Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 5.3s.
Random Insertion/In memory Foster BTree Insertion
                        time:   [526.55 ms 530.25 ms 534.95 ms]
                        change: [-0.8147% +0.7642% +2.1429%] (p = 0.35 > 0.05)
                        No change in performance detected.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high mild
Random Insertion/In memory Foster BTree Insertion Parallel
                        time:   [297.13 ms 299.73 ms 302.44 ms]
                        change: [+1.9298% +3.0824% +4.2221%] (p = 0.00 < 0.05)
                        Performance has regressed.
Random Insertion/On disk Foster BTree Initial Allocation
                        time:   [15.292 ms 15.340 ms 15.392 ms]
                        change: [-4.3608% -0.8736% +2.9321%] (p = 0.71 > 0.05)
                        No change in performance detected.
Found 2 outliers among 10 measurements (20.00%)
  1 (10.00%) high mild
  1 (10.00%) high severe
Benchmarking Random Insertion/On disk Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 10.2s.
Random Insertion/On disk Foster BTree Insertion
                        time:   [1.0093 s 1.0157 s 1.0250 s]
                        change: [-0.7224% +0.6344% +2.1050%] (p = 0.40 > 0.05)
                        No change in performance detected.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe
Benchmarking Random Insertion/On disk Foster BTree Insertion Parallel: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 8.1s.
Random Insertion/On disk Foster BTree Insertion Parallel
                        time:   [806.46 ms 811.53 ms 817.61 ms]
                        change: [-0.8242% +0.1775% +1.2137%] (p = 0.76 > 0.05)
                        No change in performance detected.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high mild
Random Insertion/BTreeMap Insertion
                        time:   [442.08 ms 444.52 ms 447.57 ms]
                        change: [+0.3357% +1.0474% +1.7972%] (p = 0.02 < 0.05)
                        Change within noise threshold.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe
```

## Perf

```sh
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
