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
     Running benches/random_insert.rs (target/release/deps/random_insert-e8a151c04c64f10c)
Gnuplot not found, using plotters backend
Benchmarking Random Insertion/Foster BTree Insertion: Collecting 10 samples in estimated 7.525
Random Insertion/Foster BTree Insertion
                        time:   [66.547 ms 66.878 ms 67.419 ms]
                        change: [-0.8256% +0.8573% +4.1165%] (p = 0.34 > 0.05)
                        No change in performance detected.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe
Benchmarking Random Insertion/Foster BTree Insertion Parallel: Collecting 10 samples in estima
Random Insertion/Foster BTree Insertion Parallel
                        time:   [44.699 ms 44.897 ms 45.236 ms]
                        change: [-29.710% -28.647% -27.603%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) low mild
Benchmarking Random Insertion/BTreeMap Insertion: Collecting 10 samples in estimated 6.6681 s 
Random Insertion/BTreeMap Insertion
                        time:   [39.927 ms 40.194 ms 40.434 ms]
                        change: [-2.2897% +0.8231% +5.8621%] (p = 0.65 > 0.05)
                        No change in performance detected.
Found 2 outliers among 10 measurements (20.00%)
  1 (10.00%) low mild
  1 (10.00%) high severe
```


## Perf
```
cargo build --release
perf record -e cycles -g --call-graph dwarf ./target/release/main 
hotspot perf.data
```

if sudo is needed, set `echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid` and `echo 0 | sudo tee /proc/sys/kernel/kptr_restrict`