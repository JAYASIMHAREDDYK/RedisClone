# Architecture and Systems Design Document

This document provides a comprehensive technical reference for the internal architecture, algorithms, trade-offs, and systems programming principles implemented in RedisClone.

---

## 1. High-Level Engine Architecture

```
                             [ Clients / redis-benchmark ]
                                           |
                                           | Persistent Non-Blocking TCP
                                           v
+-----------------------------------------------------------------------------------+
| Network I/O Reactor (Single-Threaded epoll Event Loop)                             |
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
                  | Command Vector (e.g., SET, GET, ZADD)         | RespWriter
                  v                                               |
+-----------------------------------------------------------------|-----------------+
| Execution Core & In-Memory Storage                                                |
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
                                          |
                      +-------------------+-------------------+
                      |                                       |
                      v                                       v
+-------------------------------------------+ +-------------------------------------+
| Persistence Subsystem (Append-Only File)  | | Replication Subsystem (Primary)     |
| - write() append buffer to page cache     | | - Circular 1MB Replication Backlog  |
| - Background thread fdatasync() every sec | | - Byte stream monotonic offset      |
| - fork() copy-on-write log rewrite        | | - Streaming tail to online replicas |
+-------------------------------------------+ +-------------------------------------+
```

---

## 2. Distributed Replication & High Availability

### 2.1 Replication Backlog and Stream Offsets
The primary node maintains a 64-bit monotonic replication offset (`master_repl_offset`) and a circular 1MB byte backlog buffer (`ReplicationBacklog`). When mutating commands (`SET`, `DEL`, `EXPIRE`, `ZADD`) are executed:
1. The primary formats the command into canonical RESP protocol format.
2. The payload is written into the backlog, advancing the global byte offset.
3. The payload is streamed non-blocking to all connected replicas in `ONLINE` status.

### 2.2 Full Synchronization (PSYNC ? -1)
When a replica connects without prior synchronization state:
1. Replica issues `PSYNC ? -1`.
2. Primary replies with `+FULLRESYNC <master_repl_id> <master_repl_offset>\r\n`.
3. Primary scans memory and transmits a snapshot of all active keys as a single RESP bulk string.
4. Replica flushes its local keyspace and deserializes the snapshot into memory.
5. Primary registers replica as `ONLINE` and streams subsequent backlog commands.

### 2.3 Partial Resynchronization (PSYNC <repl_id> <offset>)
If a replica temporarily disconnects due to transient network failure:
1. Replica reconnects and issues `PSYNC <cached_master_repl_id> <applied_offset>`.
2. If `cached_master_repl_id == master_repl_id` and `applied_offset >= backlog_start_offset`:
   - Primary responds with `+CONTINUE\r\n`.
   - Primary extracts and transmits the missing byte range directly from the replication backlog buffer.
   - Replica catches up without requiring a full memory snapshot transfer.
3. If the offset fell off the backlog window, primary transparently falls back to `+FULLRESYNC`.

### 2.4 Synchronous Replication Barrier: WAIT
By default, replication in Redis and RedisClone is asynchronous. The primary acknowledges writes to the client before waiting for replicas to confirm receipt.
The `WAIT <numreplicas> <timeout_ms>` command queries replicas via `REPLCONF GETACK *` and blocks until at least `numreplicas` acknowledge writing up to `master_repl_offset` (or until timeout expires).

---

## 3. Systems Programming Interview Questions & Answers

### Q1: Why does edge-triggered epoll require reading until EAGAIN?
**Answer**:
Level-triggered epoll notifies user space whenever a file descriptor has readable data pending in kernel buffers. In contrast, edge-triggered epoll (`EPOLLET`) only generates an event notification when the socket transitions readiness states (for example, when new bytes arrive on an idle socket).

If the reader routine consumes only a subset of available bytes and returns to `epoll_wait()`, the kernel will not generate another event for the remaining buffered bytes until new data arrives over TCP. If the client is awaiting a response before transmitting further data, both client and server deadlock.

Therefore, an edge-triggered event handler must drain the socket buffer in a loop until `read()` or `recv()` returns `-1` with `errno == EAGAIN` or `EWOULDBLOCK`.

### Q2: What happens if a command arrives split across two read() calls?
**Answer**:
TCP is a byte-stream protocol without frame or message boundaries. Segment fragmentation and OS buffer limits can cause a command such as `*3\r\n$3\r\nSET\r\n...` to be split across arbitrary packet boundaries.

The `RespParser` state machine evaluates the input buffer incrementally:
1. It parses the array length delimited by `\r\n`.
2. For each element, it reads the prefix `$`, parses the bulk string byte length, and verifies whether the complete byte sequence plus the terminating `\r\n` is present in the buffer.
3. If insufficient bytes are present, `next_command()` leaves the buffer intact and returns `false`.
4. The event loop resumes polling and appends the subsequent packet on the next readable notification before retrying the parse.

### Q3: Why is incremental rehash needed, and what do lookups do during rehash?
**Answer**:
For an in-memory dictionary holding millions of keys, resizing the hash table all at once requires allocating a larger table and rehashing every key synchronously. In a single-threaded server, this stalls the event loop for tens of milliseconds, causing severe latency spikes.

Incremental rehashing maintains two hash tables:
- `ht[0]`: Current active table being drained.
- `ht[1]`: New expanded table (next power of two).

Every query (`GET`, `SET`, `DEL`, `EXISTS`) invokes `step_rehash(1)`, migrating one non-empty bucket from `ht[0]` to `ht[1]`.
During rehashing:
- Probing checks `ht[0]` first. If the key is not present, it probes `ht[1]`.
- Deletions check `ht[0]`, then `ht[1]`.
- Insertions write directly into `ht[1]`. This ensures `ht[0]` monotonically drains until empty, at which point `ht[0]` is replaced with `ht[1]` and rehashing terminates.

### Q4: What does fork() copy, and why can a rewrite double memory under heavy writes?
**Answer**:
`fork()` duplicates the parent process's page table entries rather than the physical RAM pages. Both parent and child processes initially point to the identical physical memory pages marked read-only with copy-on-write (COW) flags.

When either process writes to a page, the CPU hardware raises a page fault, prompting the OS kernel to allocate a private 4KB copy for that process.

During `BGREWRITEAOF`, the child process traverses the static memory snapshot to generate the compacted AOF file. If the parent experiences high write throughput modifying existing keys during the rewrite window, the OS copies each modified 4KB page. In the worst case where every page is modified, the resident memory footprint approaches twice the original size (2x RSS).

### Q5: What does everysec lose on a crash, and why?
**Answer**:
With `FsyncPolicy::EverySec`, the main event loop calls `write()` on every mutating command. This writes data to the OS page cache immediately without blocking the event loop on synchronous disk I/O. A dedicated background thread invokes `fdatasync()` once per second.

If the server terminates ungracefully (`SIGKILL`, kernel panic, power loss), any transactions committed to the page cache during the preceding 1-second interval that have not yet been flushed to non-volatile storage will be lost.

### Q6: Why maintain a member->score map next to the skiplist?
**Answer**:
A SkipList orders elements by `score` to enable $O(\log N)$ range scans and rank lookups. However, locating an element by `member` in a SkipList requires an $O(N)$ linear traversal across all nodes because the index key is the score, not the member name.

Maintaining a hash map (`dict[member] -> score`) alongside the SkipList provides:
- $O(1)$ membership checks (`HEXISTS` equivalent).
- $O(1)$ score lookups (`ZSCORE`).
- $O(\log N)$ score updates in `ZADD`: the old score is retrieved in $O(1)$, the old node is unlinked in $O(\log N)$, and the new node is inserted with the updated score in $O(\log N)$.

### Q7: How does asynchronous replication lead to lost writes during failover, and how was it measured?
**Answer**:
In asynchronous replication, the primary server acknowledges a write command to the client as soon as it updates its local memory and queues the command for transmission to replicas. If the primary process abruptly terminates (`SIGKILL` or hardware failure) before the replica socket drains in-flight bytes, those acknowledged writes are never received by the replica.

When the replica is promoted via `REPLICAOF NO ONE`, those acknowledged writes are permanently lost. Under an injected 10ms replication network latency, killing the primary under active write load resulted in **5,327 of 10,000 acknowledged writes lost (53.27% data loss)**.

### Q8: How does the WAIT command prevent write loss?
**Answer**:
The `WAIT <numreplicas> <timeout_ms>` command introduces a semi-synchronous replication barrier. The client sends a write followed by `WAIT 1 1000`. The primary sends `REPLCONF GETACK *` to the replica and pauses returning to the client until the replica explicitly acknowledges having received and applied all byte offsets up to the write's replication offset.

When testing failover with `WAIT 1 1000`, **0 of 300 acknowledged writes were lost (0.00% data loss)** when promoting the synchronized replica.
*Caveat*: `WAIT` guarantees zero acknowledged write loss **only if the acknowledging replica is the one promoted**. If an out-of-date or partitioned replica is elected, writes can still be lost.
*Workload Difference*: The async test executed 10,000 writes to measure high-throughput async pipeline drainage under mid-stream `SIGKILL`. The `WAIT` test executed 300 writes because each batch incurred a synchronous round-trip acknowledgment barrier (~1.2 ms network RTT latency per batch), illustrating the classic latency-durability trade-off.

---

## 4. Performance Investigation: The 29x SET Regression

### 4.1 Symptom
Following the integration of primary-replica streaming replication, pipelined `SET` throughput suffered a severe regression, collapsing from over 800,000 ops/sec down to **41,494 ops/sec**, with p50 latency ballooning from 0.032 ms to **26.815 ms** and p99 to **57.119 ms**. In contrast, `GET` throughput remained high (~1.8M ops/sec), indicating the regression was isolated to mutating commands.

### 4.2 Investigation Methodology
1. **Git Bisect**: Running automated benchmarks at each bisect step isolated the regression to the commit introducing the `ReplicationBacklog` buffer.
2. **Profiling (`perf record` / `strace -c`)**:
   - `strace -c` confirmed that system calls (`write`, `epoll_wait`) were not blocking (less than 3% of elapsed execution time).
   - `perf record -g` / flame graph analysis revealed that over **95% of on-CPU execution time** was concentrated inside glibc `__memmove_avx_unaligned_erms`, invoked by `std::string::erase(0, overflow)` on every mutating command.

### 4.3 Root Cause Analysis
The replication backlog was initially implemented as a dynamically appended `std::string`:
```cpp
// Defective implementation:
void ReplicationBacklog::feed(const std::string& raw_resp) {
    buffer_.append(raw_resp);
    master_offset_ += raw_resp.size();
    if (buffer_.size() > max_size_) {
        size_t overflow = buffer_.size() - max_size_;
        buffer_.erase(0, overflow); // <-- 1MB memmove on EVERY mutation!
    }
}
```
As soon as the backlog reached its 1MB capacity (`max_size_ = 1024 * 1024`), every single 50-byte `SET` command called `buffer_.erase(0, overflow)`. This caused an in-memory `memmove` of **1,048,576 bytes per query** on the reactor thread, saturating system memory bus bandwidth at ~43 GB/s.

Additionally, `Server::cmd_set` performed a redundant second hash lookup: calling `dict_.set(key, val)` followed immediately by `dict_.find(key)`, duplicating hash computation and bucket traversals.

### 4.4 Resolution
1. **Circular Ring Buffer**: Refactored `ReplicationBacklog` to a preallocated `std::vector<char>` ring buffer indexed modulo `max_size_`. Appending new bytes is now strictly $O(\text{payload})$, writing into the buffer via 1 or 2 small `memcpy` operations with **zero `memmove` overhead**.
2. **Single-Pass Hash & Insert**: Refactored `Dict::set(key, val, &out_entry)` to return the created or updated `Entry*` directly from the initial hash traversal, eliminating the redundant `dict_.find()` call.

### 4.5 Before vs. After Benchmark Comparison
Identical benchmark configuration (`redis-benchmark -c 50 -P 16 -n 1000000 -t set` without AOF):

| Metric | Defective Backlog (`std::string::erase`) | Circular Ring Buffer + Single-Pass Dict | Speedup |
| :--- | :---: | :---: | :---: |
| **SET Throughput** | 41,494 ops/sec | **1,204,819 ops/sec** | **29.0x** |
| **SET p50 Latency** | 26.815 ms | **0.479 ms** | **56.0x lower** |
| **SET p99 Latency** | 57.119 ms | **1.591 ms** | **35.9x lower** |
| **Memory Bandwidth** | ~43.5 GB/s (bus saturation) | < 120 MB/s | **Normal** |

---

## 5. Performance Investigation: Closing the Write-Path Gap (SET Optimization)

### 5.1 The Investigation: Why was SET ~53% while GET was ~95%?
After resolving the 29x replication backlog memmove regression, baseline benchmarks revealed an asymmetric throughput profile:
- **GET**: Reached **2,212,390 ops/sec** (**94.9%** of official Redis 7.0).
- **SET**: Reached **1,066,098 ops/sec** (**53.3%** of official Redis 7.0).

While the read path was near-native Redis speed, the write path cost more than twice as much per transaction. An interviewer examining these numbers will immediately probe why mutations incurred this 2x penalty.

### 5.2 Hot-Path Profiling (`perf record -g` & Flame Graph)
We captured on-CPU call graphs using `perf record -F 999 -g` during a 500,000-request pipelined `SET` workload (`-c 50 -P 16`) and collapsed stacks to generate an interactive SVG flame graph (committed in [benchmark/results/flamegraph.svg](benchmark/results/flamegraph.svg)).

The profiling report highlighted four distinct sources of write-path overhead:
1. **Replication Backlog Propagation on Standalone Master**:
   In `Server::execute_internal`, every mutation unconditionally executed:
   ```cpp
   void Server::propagate_to_replicas(const vector<string>& args) {
       string payload = RespWriter::array(args);
       backlog_.feed(payload);
       // ...
   }
   ```
   `RespWriter::array` allocated four separate `std::string` buffers per `SET` command (`*3\r\n`, `$3\r\nSET\r\n`, `$key\r\n`, `$val\r\n`) and copied them into the backlog ring buffer, even when **zero replicas were connected**.
   *Redis Comparison*: Official Redis (`server.c` / `replication.c`) leaves `server.repl_backlog = NULL` until a replica explicitly connects (`createReplicationBacklog()`). Standalone writes bypass backlog formatting entirely.
2. **LFU Eviction Frequency-Bucket Churn**:
   When `config.maxmemory == 0` (unlimited memory, the default benchmark setting), cache eviction is disabled. However, `Server::cmd_set` invoked `eviction_.on_touch(entry)` on every update, which allocated a `new FreqBucket` and deleted the previous bucket on the heap, triggering over 1,000,000 heap allocations per run.
3. **Redundant Expiry Set Erasures**:
   `expire_.clear_expire(entry)` executed `expiring_keys_.erase(entry->key)` on every standard `SET` without checking whether the entry had an active TTL (`entry->expire_at_ms == 0`), hashing the key and probing `std::unordered_set` unnecessarily.
4. **Per-Write String Variant Reallocation**:
   `Dict::set` assigned `curr->value = std::move(val)` across the `std::variant`, destroying the old string allocation and creating a new one rather than reusing `curr->str().assign(val)`.

### 5.3 Implementation Fixes
1. **Active Replication Guard**:
   Introduced `bool has_active_replication_{false}` on `Server`, initialized to `false` and set to `true` only when a replica connects and executes `PSYNC`. Bypassed all RESP serialization and ring-buffer writes when `!has_active_replication_ && replicas_.empty()`.
2. **Eviction Bypass when Memory is Uncapped**:
   Guarded `eviction_.on_insert`, `eviction_.on_touch`, and `eviction_.on_remove` behind `if (config_.maxmemory > 0 && config_.eviction_policy != EvictPolicy::NoEviction)`.
3. **Zero-Hash Expiry Guard**:
   In `Expirer::clear_expire`:
   ```cpp
   void Expirer::clear_expire(Entry* entry) {
       if (!entry || entry->expire_at_ms == 0) return;
       entry->expire_at_ms = 0;
       if (!expiring_keys_.empty()) {
           expiring_keys_.erase(entry->key);
       }
   }
   ```
4. **In-Place String Buffer Reuse**:
   In `Dict::set`:
   ```cpp
   if (curr->is_str() && std::holds_alternative<string>(val)) {
       curr->str().assign(std::get<string>(val)); // Reuses allocated capacity
   } else {
       curr->value = std::move(val);
   }
   ```

### 5.4 Measured Results After Optimization
Ran the 5-run interleaved benchmark suite (`benchmark/run_benchmarks.py`) back-to-back with official Redis 7.0 (WSL2 Debian 12, pinned via `taskset`, median reported):

| Workload | Engine | SET Ops/sec | SET vs Redis | GET Ops/sec | GET vs Redis |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Standard Key** | **Redis 7.0** | 1,980,198 | Baseline | 2,331,002 | Baseline |
| **Standard Key** | **RedisClone** | **2,493,766** | **125.9%** | **2,801,120** | **120.2%** |
| **Random 1M Keys** | **Redis 7.0** | 919,963 | Baseline | 1,212,121 | Baseline |
| **Random 1M Keys** | **RedisClone** | **766,871** | **83.4%** | **970,874** | **80.1%** |

### 5.5 Ring Buffer Indexing: Line-by-Line Explanation
The circular ring buffer (`ReplicationBacklog`) operates over a preallocated `std::vector<char> buffer_(max_size_)` with two 64-bit monotonically increasing byte counters:
- `master_offset_`: The total logical bytes fed to the backlog since inception.
- `start_offset_`: The earliest logical byte offset still retained in the circular window.

```cpp
void ReplicationBacklog::feed(const std::string& data) {
    size_t len = data.size();
    if (len == 0) return;

    if (len >= max_size_) {
        // Payload exceeds ring size: buffer only the final max_size_ bytes
        const char* src = data.data() + (len - max_size_);
        std::memcpy(buffer_.data(), src, max_size_);
        master_offset_ += len;
        start_offset_ = master_offset_ - max_size_;
        return;
    }

    // Calculate physical write index via modulo arithmetic
    size_t write_idx = static_cast<size_t>(master_offset_ % max_size_);
    size_t first_chunk = std::min(len, max_size_ - write_idx);
    size_t second_chunk = len - first_chunk;

    // First chunk writes up to the buffer boundary
    std::memcpy(&buffer_[write_idx], data.data(), first_chunk);

    // Second chunk wraps around to index 0 if the payload spans the boundary
    if (second_chunk > 0) {
        std::memcpy(&buffer_[0], data.data() + first_chunk, second_chunk);
    }

    master_offset_ += len;
    if (master_offset_ - start_offset_ > static_cast<int64_t>(max_size_)) {
        start_offset_ = master_offset_ - max_size_;
    }
}
```
*Key invariant*: Zero memory movements (`memmove` or reallocations) occur during ingestion; every write is decomposed into at most two fixed-size `memcpy` calls.

