# TODO

## Buffer pool
* [x] Add reader-writer latch
* [x] Add container file mapping
* [x] Write the evicted page to disk if dirty
* [x] Add logger for debugging
* [x] Add cache replacement implementations
    * [x] Add more eviction algo
* [x] Add write-ahead log, page lsn
* [] Add pointer swizzling

## Foster B-Tree
* [x] Add Foster B-tree Page
  * [] Add remove operator
  * [] Add upsert operator
  * [] Add range scan operator
    * [] Add consistency checker
  * [x] Add statistics for each structure modification
    * [] Revise the split decision
  * [] Add prefix compression in pages
  * [] Add page split into three pages
  * [] Reuse the removed page in merge
  * [] Optimistic lock coupling

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
                        time:   [38.132 ms 38.312 ms 38.540 ms]
Found 13 outliers among 100 measurements (13.00%)
  4 (4.00%) high mild
  9 (9.00%) high severe
Random Insertion/Foster BTree Insertion Parallel
                        time:   [21.047 ms 21.334 ms 21.627 ms]
Random Insertion/BTreeMap Insertion
                        time:   [38.120 ms 38.430 ms 38.825 ms]
Found 5 outliers among 100 measurements (5.00%)
  3 (3.00%) high mild
  2 (2.00%) high severe
```


## Perf
```
cargo build --release
perf record -e cycles -g --call-graph dwarf ./target/release/main 
hotspot perf.data
```

if sudo is needed, set `echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid` and `echo 0 | sudo tee /proc/sys/kernel/kptr_restrict`