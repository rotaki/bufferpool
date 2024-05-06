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
  * [] Add memory usage profiler
  * [] Test with bp (limited memory pool with disk offloading)
  * [] Reuse the removed page in merge
  * [] Add thread-safe range scan operator with page versioning (optimistic lock coupling)
  * [] Optimistic lock coupling with hybrid latches
  * [] Add ghost record support for transaction support
  * [] Add prefix compression in pages
  * [] Add page split into three pages

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
Random Insertion/Foster BTree Insertion
                        time:   [34.515 ms 34.734 ms 34.995 ms]
Found 11 outliers among 100 measurements (11.00%)
  3 (3.00%) high mild
  8 (8.00%) high severe
Benchmarking Random Insertion/Foster BTree Insertion Parallel: Collecting 100 samples in estimated 6.3729 s (300 it
Random Insertion/Foster BTree Insertion Parallel
                        time:   [20.879 ms 21.186 ms 21.503 ms]
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild
Random Insertion/BTreeMap Insertion
                        time:   [38.618 ms 38.916 ms 39.268 ms]
Found 8 outliers among 100 measurements (8.00%)
  3 (3.00%) high mild
  5 (5.00%) high severe
```


## Perf
```
cargo build --release
perf record -e cycles -g --call-graph dwarf ./target/release/main 
hotspot perf.data
```

if sudo is needed, set `echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid` and `echo 0 | sudo tee /proc/sys/kernel/kptr_restrict`