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
     Running benches/random_insert.rs (target/release/deps/random_insert-0798ecc00b9f7801)
Gnuplot not found, using plotters backend
Benchmarking Random Insertion/Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 32.9s, or reduce sample count to 10.
Benchmarking Random Insertion/Foster BTree Insertion: Collecting 100 samples in estimated 32.913 s (100 iterat
Random Insertion/Foster BTree Insertion
                        time:   [329.75 ms 330.93 ms 332.27 ms]
                        change: [+3495.4% +3520.1% +3542.5%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 5 outliers among 100 measurements (5.00%)
  2 (2.00%) high mild
  3 (3.00%) high severe
Benchmarking Random Insertion/Foster BTree Insertion Parallel: Warming up for 3.0000 s
Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to 10.1s, or reduce sample count to 40.
Benchmarking Random Insertion/Foster BTree Insertion Parallel: Collecting 100 samples in estimated 10.063 s (1
Random Insertion/Foster BTree Insertion Parallel
                        time:   [92.059 ms 93.532 ms 95.642 ms]
Found 4 outliers among 100 measurements (4.00%)
  2 (2.00%) high mild
  2 (2.00%) high severe
Benchmarking Random Insertion/BTreeMap Insertion: Collecting 100 samples in estimated 8.4116 s (200 iterations
Random Insertion/BTreeMap Insertion
                        time:   [41.386 ms 41.735 ms 42.126 ms]
                        change: [+2114.6% +2137.5% +2162.7%] (p = 0.00 < 0.05)
                        Performance has regressed.
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