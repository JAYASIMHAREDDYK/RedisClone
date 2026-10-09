# Networked In-Memory Key-Value Datastore (Redis Clone)

A high-performance in-memory key-value storage engine engineered in C++17 speaking native RESP2 (Redis Serialization Protocol). Achieving **812,000+ operations per second** with sub-0.3 ms $p99$ tail latency under pipelined workloads and **78,000+ single-round-trip QPS** ($p50 < 0.25$ ms), the engine eliminates stop-the-world latency spikes via dual-table incremental rehashing, provides $O(\log N)$ rank operations via level-span SkipLists, and guarantees crash durability through an Append-Only File (AOF) persistence subsystem with copy-on-write background rewrites.

---

## Architecture Diagram

```
                             [ Clients / SDKs / redis-cli ]
                                            |
                                            | Persistent TCP Sockets
                                            v
+-----------------------------------------------------------------------------------+
| Network I/O Core (Single-Threaded Reactor)                                        |
|                                                                                   |
|   +---------------------------------------------------------------------------+   |
|   |         Linux epoll_wait() Reactor Event Loop (Edge-Triggered / EPOLLET)  |   |
|   +-------------------------------------+-------------------------------------+   |
|                                         |                                         |
|                 +-----------------------+-----------------------+                 |
|                 v                                               v                 |
|    +--------------------------+                   +--------------------------+    |
|    |   Client Read Buffer     |                   |   Client Write Buffer    |    |
|    |  (Drains until EAGAIN)   |                   |  (Flushes on EPOLLOUT)   |    |
|    +------------+-------------+                   +-------------^------------+    |
|                 |                                               |                 |
|                 v                                               |                 |
|    +--------------------------+                                 |                 |
|    |      RespParser          |                                 |                 |
|    |  (State Machine Frame)   |                                 |                 |
|    +------------+-------------+                                 |                 |
+-----------------|-----------------------------------------------|-----------------+
                  | Command Vector (e.g. SET, ZADD)               | RespWriter
                  v                                               |
+-----------------------------------------------------------------|-----------------+
| Command Execution Engine                                                          |
|                                                                                   |
|   +--------------------------+   Step Rehash   +------------------------------+   |
|   |   Primary Table (ht[0])  | <-------------> |   Rehash Target (ht[1])      |   |
|   |   Active Hash Table      |  O(1) / query   |   Expansion Table            |   |
|   +-------------+------------+                 +------------------------------+   |
|                 |                                                                 |
|                 +-----------------------+-----------------------+                 |
|                                         |                       |                 |
|                                         v                       v                 |
|                            +----------------------+ +----------------------+      |
|                            | SkipList / ZSet      | | Evictor (LFU / LRU)  |      |
|                            | 32-Level Geometric   | | O(1) Frequency Lists |      |
|                            | Span Ranking O(log N)| | Sampled LRU Fallback |      |
|                            +----------------------+ +----------------------+      |
+-----------------------------------------+-----------------------------------------+
                                          | Canonical RESP Stream
                                          v
+-----------------------------------------------------------------------------------+
| Persistence Subsystem (Append-Only File)                                          |
|                                                                                   |
|   [ Parent Process ] ---- write() append buffer ------> appendonly.aof            |
|            |                                                                      |
|          fork() (Copy-on-Write)                                                   |
|            v                                                                      |
|   [ Child Process  ] ---- writes memory snapshot -----> temp-rewrite.aof          |
|                                                                                   |
|   On Child Exit: Parent flushes rewrite buffer -> atomic rename(temp, aof)        |
+-----------------------------------------------------------------------------------+
```

---

## Hardware & Environment

- **CPU**: 12th Gen Intel(R) Core(TM) i5-12450H (8 Cores, 12 Threads)
- **RAM**: 16 GB Dual-Channel
- **OS / Kernel**: Linux 6.18 / Windows 11 x86_64
- **Compiler**: GCC 16.1.0 (`-std=c++17 -Wall -Wextra -O2`)

---

## Benchmark Results vs Real Redis

Benchmarking protocol: Release build (`-O2`), isolated CPU pinning, 3 runs per configuration reporting the median throughput.

| Workload Configuration | Clone Throughput | Redis 7.0 Throughput | Relative Perf (% of Redis) | p50 Latency | p99 Latency |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `SET` (c=50, P=16, n=1M) | **812,901 ops/sec** | 1,020,000 ops/sec | **79.7%** | 0.032 ms | 0.197 ms |
| `GET` (c=50, P=16, n=1M) | **687,819 ops/sec** | 940,000 ops/sec | **73.2%** | 0.038 ms | 0.243 ms |
| `SET` (c=50, P=1, n=1M) | **79,971 ops/sec** | 98,500 ops/sec | **81.2%** | 0.216 ms | 0.805 ms |
| `GET` (c=50, P=1, n=1M) | **78,622 ops/sec** | 102,000 ops/sec | **77.1%** | 0.206 ms | 0.947 ms |
| `ZADD` (c=50, P=16, n=1M)| **801,302 ops/sec** | 980,000 ops/sec | **81.8%** | 0.057 ms | 0.124 ms |

---

## Rehash Latency Jitter Profile

During continuous writes from 4 to 131,072 buckets, stop-the-world single-threaded table reallocation blocks the event loop proportionally to table size. Progressive rehashing migrates 1 bucket per command, bounding latency jitter:

```
Latency Spike Comparison (Max & p99.9 Tail Latency during Hash Table Resizes):

Stop-The-World Rehash:
p99.9  [==== 2.35 ms                                                       ]
Max    [=========================================================== 27.16 ms]

Progressive Rehash (Our Engine):
p99.9  [= 0.13 ms                                                         ]
Max    [== 1.13 ms                                                        ]
```

- **Progressive Rehash**: $p50 = 0.021$ ms, $p99 = 0.050$ ms, $p99.9 = 0.127$ ms, $\text{Max} = 1.131$ ms.
- **Stop-The-World**: $p99.9 = 2.348$ ms, $\text{Max} = 27.156$ ms ($>24\times$ latency spike).

---

## Design Trade-Offs

1. **Why Single-Threaded Reactor?**
   - Eliminates mutex lock contention, condition variable context switches, and cache-line bouncing across multi-core CPUs.
   - All in-memory structures (`Dict`, `ZSet`, `Lfu`, `Expirer`) require zero locks, maximizing L1/L2 data cache hit ratios.
   - Predictable deterministic execution order without concurrency hazards or deadlock scenarios.

2. **Why Edge-Triggered (`EPOLLET`) Needs Drain-Until-`EAGAIN`?**
   - In edge-triggered mode, the Linux kernel notifies `epoll_wait` only upon state transitions (e.g., when new bytes arrive on a previously idle socket).
   - If user space reads fewer bytes than available in the OS socket buffer and stops, no further notification will fire for the remaining buffered data until a new packet arrives.
   - Consequently, the reader routine must loop `recv()` until `EAGAIN` or `EWOULDBLOCK` is returned to prevent request stalls and connection hangs.

3. **Why Incremental Progressive Rehashing?**
   - Reallocating and copying a hash table with millions of entries in one synchronous call stalls the reactor thread for 20–50+ ms.
   - Dual-table progressive rehashing (`ht[0]` and `ht[1]`) amortizes the cost: each read/write migrates exactly 1 non-empty bucket ($O(1)$ amortized overhead).
   - Lookups search `ht[0]`, falling back to `ht[1]` only if not found during rehash. All new insertions go directly into `ht[1]`, guaranteeing `ht[0]` continuously empties until fully reclaimed.

4. **What Does `fork()` Copy-On-Write Cost?**
   - `fork()` does not immediately copy physical RAM pages; it clones page table descriptors marked as read-only.
   - While the child process streams the static memory snapshot to disk, every write from the parent triggers an OS page fault and allocates a private 4KB copy of the target page.
   - Under heavy write load during `BGREWRITEAOF`, memory overhead can approach up to $2\times$ of resident memory as pages are progressively copied.

---

## Interview Questions Answered Cold

### 1. Why does edge-triggered epoll require reading until EAGAIN?
Level-triggered epoll reports whether a socket *is* readable (as long as bytes remain in the kernel buffer). Edge-triggered epoll reports only when the readiness state *changes* (e.g. from no data to data arriving). If you do not drain the socket buffer until `read()` returns `EAGAIN` or `EWOULDBLOCK`, any residual bytes remain unread and the kernel will never generate another event for them until brand new data arrives over TCP. The connection deadlocks waiting on a notification that never fires.

### 2. What happens if a command arrives split across two read() calls?
TCP is a byte-stream protocol with no frame boundaries. Network fragmentation or MTU limits can split a single RESP command (e.g., `*3\r\n$3\r\nSET...`) across multiple TCP segments. The `RespParser` state machine inspects the accumulated buffer: if a trailing `\r\n` or the length-prefixed bulk byte count is not yet fully received, `next_command()` returns `false` without consuming the partial bytes. The parser retains the prefix in its connection-local buffer and resumes parsing when the subsequent segment arrives.

### 3. Why is incremental rehash needed, and what do lookups do during rehash?
Synchronous table resizing for large tables ($N > 10^6$) causes severe latency spikes ($> 25$ ms), violating tail latency SLOs. Incremental rehashing allocates `ht[1]` at the next power of two and migrates buckets one by one during subsequent queries (`step_rehash(1)`). During rehashing:
- Lookups first probe `ht[0]`. If found, they return immediately; if not found, they probe `ht[1]`.
- Deletions check `ht[0]`, then `ht[1]`.
- All new insertions write exclusively to `ht[1]`, ensuring `ht[0]` monotonically decreases until empty.

### 4. What does fork() copy, and why can a rewrite double memory under heavy writes?
`fork()` duplicates the parent's page table entries, pointing both processes to the same physical memory pages with copy-on-write (`COW`) permissions. Physical memory is not duplicated at invocation time. However, if the datastore experiences sustained high write volumes while the child is writing the rewrite file, every write modification to a page triggers an OS page fault that copies the 4KB page. If 100% of the keys are modified during the rewrite window, the process memory footprint doubles ($2\times$ RSS).

### 5. What does everysec lose on a crash, and why?
With `fsync everysec`, the main reactor thread calls `write()` on every query, pushing mutation commands into the Linux OS page cache. A dedicated background thread executes `fdatasync()` once per second. If the system crashes abruptly (`SIGKILL`, kernel panic, or power cut), any data written to the page cache within the last 1-second interval that has not yet been flushed to non-volatile disk will be lost.

### 6. Why a member->score map next to the skiplist?
A SkipList provides $O(\log N)$ searches ordered by `score`. However, looking up an element by `member` name would require an $O(N)$ full traversal because the SkipList is indexed by score, not member name. Keeping an auxiliary hash map (`dict[member] -> score`) provides $O(1)$ member existence checks, $O(1)$ `ZSCORE` lookups, and turns `ZADD` member score updates into $O(\log N)$ by locating the old score instantly and re-inserting only that node in the SkipList.

---

## Known Limitations

- Single-node design; does not implement multi-node Redis Cluster gossip protocol or Redis Sentinel consensus.
- No Lua scripting engine or multi-key transaction rollback (`MULTI`/`EXEC` atomicity across commands).
- Non-blocking AOF rewrite utilizes POSIX `fork()` copy-on-write semantics on Linux; on native Windows environments, a snapshot background worker thread is used as a portability fallback.

---

## Build & Test Instructions

### Building
```bash
# Using Makefile
make

# Using CMake
mkdir build && cd build
cmake ..
cmake --build . --config Release
```

### Running Tests
```bash
# Unit tests
./unit_tests

# Integration tests
python tests/test_integration.py

# Stress tests
python tests/test_stress.py

# Black-box SDK compatibility tests
python tests/test_blackbox.py

# Rehash jitter latency profile
python tests/test_rehash_jitter.py

# Persistence SIGKILL crash recovery test
python tests/test_persistence_sigkill.py

# High-concurrency benchmark
python benchmark/benchmark.py 6379 50 100000 16
```
