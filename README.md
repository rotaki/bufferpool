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
  * [] Add statistics for each structure modification
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
                        time:   [57.208 ms 57.569 ms 58.016 ms]
                        change: [+0.1856% +1.5084% +2.6627%] (p = 0.02 < 0.05)
                        Change within noise threshold.
Found 11 outliers among 100 measurements (11.00%)
  6 (6.00%) high mild
  5 (5.00%) high severe
Random Insertion/Foster BTree Insertion Parallel
                        time:   [37.737 ms 38.064 ms 38.407 ms]
                        change: [-6.7317% -5.6469% -4.5893%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild
Random Insertion/BTreeMap Insertion
                        time:   [39.303 ms 39.615 ms 39.963 ms]
                        change: [-2.9857% -1.4908% -0.0292%] (p = 0.06 > 0.05)
                        No change in performance detected.
Found 17 outliers among 100 measurements (17.00%)
  10 (10.00%) high mild
  7 (7.00%) high severe
```


## Perf
```
cargo build --release
perf record -e cycles -g --call-graph dwarf ./target/release/main 
hotspot perf.data
```

if sudo is needed, set `echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid` and `echo 0 | sudo tee /proc/sys/kernel/kptr_restrict`