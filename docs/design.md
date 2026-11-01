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

