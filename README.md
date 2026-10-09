# RedisClone

In-memory key-value store in C++17 implementing the RESP2 protocol, incremental hash table rehashing, SkipList sorted sets, and append-only file persistence.

[![CI](https://github.com/JAYASIMHAREDDYK/RedisClone/actions/workflows/ci.yml/badge.svg)](https://github.com/JAYASIMHAREDDYK/RedisClone/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![C++17](https://img.shields.io/badge/Standard-C%2B%2B17-purple.svg)](https://en.wikipedia.org/wiki/C%2B%2B17)
[![Platform: Linux / POSIX](https://img.shields.io/badge/Platform-Linux%20%2F%20POSIX-orange.svg)]()

## Overview

RedisClone is a standalone, single-threaded in-memory datastore engineered around a non-blocking event loop. It speaks standard RESP2 to remain compatible with existing Redis drivers and command-line utilities. Core storage relies on a dual-table progressive hash table to amortize rehashing costs across queries. Persistence is provided through an append-only file with periodic synchronization and copy-on-write background log rewriting.

| Operation | Pipelined (P=16, c=50) | Unpipelined (P=1, c=50) | p50 Latency (P=16) | p99 Latency (P=16) |
| :--- | :--- | :--- | :--- | :--- |
| `SET` | 726,253 ops/sec | 70,289 ops/sec | 0.039 ms | 0.217 ms |
| `GET` | 634,067 ops/sec | 74,711 ops/sec | 0.046 ms | 0.263 ms |
| `ZADD` | 682,808 ops/sec | 73,786 ops/sec | 0.060 ms | 0.158 ms |
| `PING` | 786,961 ops/sec | 76,246 ops/sec | 0.034 ms | 0.192 ms |

Detailed measurement methodology and raw run data are documented in [docs/benchmarks.md](docs/benchmarks.md).

## Features and Status

| Feature Area | Component | Status | Notes |
| :--- | :--- | :--- | :--- |
| Network I/O | Single-threaded Reactor | Implemented | Edge-triggered epoll on Linux; poll fallback on Windows |
| Protocol | RESP2 Parser and Writer | Implemented | Handles bulk strings, arrays, integers, errors, partial reads |
| Key-Value Store | Progressive Hash Table | Implemented | Dual-table progressive rehashing (1 bucket migrated per query) |
| Sorted Sets | SkipList with Level Spans | Implemented | 32-level geometric distribution; O(log N) rank and range scans |
| Expiration | Passive + Active Sampling | Implemented | Lazy eviction on access; 100 ms periodic sampling capped at 10 ms |
| Eviction | O(1) LFU + Sampled LRU | Implemented | Doubly-linked frequency buckets; active on maxmemory cap |
| Persistence | Append-Only File (AOF) | Implemented | always, everysec, and no fsync policies supported |
| Log Compaction | Background AOF Rewrite | Implemented | fork() copy-on-write on Linux; worker snapshot fallback on Windows |
| Clustering / Sentinel| Distributed Consensus | Not Implemented | Out of scope for standalone engine |

## Quick Start

### Build and Run

```bash
git clone https://github.com/JAYASIMHAREDDYK/RedisClone.git
cd RedisClone
make
./redis-server -p 6379 --aof yes --fsync everysec
```

### Example Session

```text
$ redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET user:100 "alice" EX 60
OK
127.0.0.1:6379> GET user:100
"alice"
127.0.0.1:6379> TTL user:100
(integer) 58
127.0.0.1:6379> ZADD leaderboard 1500 "player_alpha"
(integer) 1
127.0.0.1:6379> ZADD leaderboard 2400 "player_beta"
(integer) 1
127.0.0.1:6379> ZRANGE leaderboard 0 -1 WITHSCORES
1) "player_alpha"
2) "1500"
3) "player_beta"
4) "2400"
```

## Supported Commands

| Command | Complexity | Description / Notes |
| :--- | :--- | :--- |
| `PING [msg]` | O(1) | Returns PONG or echoes message argument. |
| `ECHO msg` | O(1) | Echoes the input string. |
| `SET key val [EX sec]` | O(1) amortized | Stores string value with optional TTL. |
| `GET key` | O(1) amortized | Retrieves value; returns nil on missing or expired keys. |
| `DEL key [key ...]` | O(K) | Removes specified keys and returns deleted count. |
| `EXISTS key [key ...]`| O(K) | Checks key existence. |
| `EXPIRE key sec` | O(1) | Sets expiration deadline in seconds. |
| `TTL key` | O(1) | Returns remaining seconds (-2 if missing, -1 if no TTL). |
| `ZADD key score member` | O(log N) | Adds or updates member score in sorted set. |
| `ZRANGE key start stop [WITHSCORES]` | O(log N + M) | Range by rank using skiplist level spans. |
| `ZRANGEBYSCORE key min max [WITHSCORES]`| O(log N + M) | Range by score. |
| `ZSCORE key member` | O(1) | Retrieves member score via auxiliary hash map. |
| `ZCARD key` | O(1) | Returns element count in sorted set. |
| `BGREWRITEAOF` | O(1) trigger | Initiates background copy-on-write log compaction. |
| `INFO` | O(1) | Returns server statistics and keyspace metrics. |

## Configuration Options

| Option Flag | Argument | Default | Description |
| :--- | :--- | :--- | :--- |
| `-p`, `--port` | `<int>` | `6379` | TCP port to listen on. |
| `-h`, `--host` | `<string>` | `0.0.0.0` | Bind IP address. |
| `-m`, `--maxmemory` | `<bytes>` | `0` (unlimited) | Memory ceiling before triggering eviction. |
| `--policy` | `<name>` | `allkeys-lfu` | Eviction policy (`noeviction`, `allkeys-lru`, `volatile-lru`, `allkeys-lfu`, `volatile-lfu`). |
| `--aof` | `yes` / `no` | `yes` | Enables Append-Only File durability. |
| `--fsync` | `always` / `everysec` / `no` | `everysec` | Disk synchronization frequency policy. |

## Architecture

```
                             [ Clients / SDKs / redis-cli ]
                                            |
                                            | Persistent TCP Sockets
                                            v
+-----------------------------------------------------------------------------------+
| Network I/O Core (Single-Threaded Reactor)                                        |
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
|   [ Parent Process ] ---- write() append buffer ------> appendonly.aof            |
|            |                                                                      |
|          fork() (Copy-on-Write)                                                   |
|            v                                                                      |
|   [ Child Process  ] ---- writes memory snapshot -----> temp-rewrite.aof          |
|                                                                                   |
|   On Child Exit: Parent flushes rewrite buffer -> atomic rename(temp, aof)        |
+-----------------------------------------------------------------------------------+
```

Incoming TCP frames are drained into connection-local buffers by edge-triggered epoll. `RespParser` reconstructs array frames, dispatches to `Server::execute()`, and writes replies into connection write buffers. Sockets register for writable events only when unwritten reply data remains.

## Persistence and Crash Recovery

Mutations (`SET`, `DEL`, `EXPIRE`, `ZADD`) append canonical RESP command arrays to disk:

- `always`: Calls synchronous flush immediately on each write. Safest, but disk bound. Zero unacknowledged data loss.
- `everysec` (Default): Appends to OS page cache per query; a background thread calls `fdatasync()` once per second. Up to 1 second of writes can be lost on ungraceful termination.
- `no`: Delegates flush timing to the operating system kernel buffer flusher.

`BGREWRITEAOF` invokes `fork()` on Linux to write a compact memory snapshot to a temporary file. The parent accumulates subsequent mutations in an in-memory buffer. When the child finishes, the parent flushes accumulated mutations and renames the file to `appendonly.aof`.

## Benchmarks

Measurements were captured on an 8-core 12th Gen Intel Core i5-12450H CPU (16 GB RAM) on Windows 11 Build 26200 using GCC 16.1.0 with `-O2`. The benchmark client executed 3 runs per target, reporting median values.

| Scenario | Clone QPS | Clone p50 | Clone p99 | Redis 7.0 QPS | Redis p50 | Redis p99 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `SET` (c=50, P=16, n=100k) | 726,253 | 0.039 ms | 0.217 ms | TBD | TBD | TBD |
| `GET` (c=50, P=16, n=100k) | 634,067 | 0.046 ms | 0.263 ms | TBD | TBD | TBD |
| `SET` (c=50, P=1, n=50k) | 70,289 | 0.545 ms | 2.755 ms | TBD | TBD | TBD |
| `GET` (c=50, P=1, n=50k) | 74,711 | 0.520 ms | 2.613 ms | TBD | TBD | TBD |
| `SET` (c=10000, P=1, n=1M) | TBD | TBD | TBD | TBD | TBD | TBD |

*Note: Redis 7.0 side-by-side figures and c=10000 high-descriptor figures are marked TBD pending execution on an identical Linux host with elevated file descriptor limits. See [docs/benchmarks.md](docs/benchmarks.md) for execution commands.*

## Rehash Latency Profile

Progressive rehashing bounds latency spikes by migrating one bucket per command during hash table resizes. In a benchmark inserting 100,000 keys across continuous table doublings (4 to 131,072 buckets):

```text
Progressive Rehash (Our Engine):
  p50:    0.021 ms
  p99:    0.050 ms
  p99.9:  0.127 ms
  Max:    1.131 ms

Stop-The-World Rehash (Single-step synchronous resize):
  p99.9:  2.348 ms
  Max:   27.156 ms
```

While synchronous table reallocation creates a 27 ms latency spike, progressive rehashing keeps max latency at 1.13 ms. Initial table memory allocation and child process fork still incur minor latency overhead.

## Memory Overhead

The internal entry structure (`Entry`) stores key strings, value variants, TTL deadlines, frequency counters, and bucket pointers.

- Metadata overhead per key: approximately 72 bytes on 64-bit platforms.
- Measured resident memory for 100,000 keys (16-byte key, 64-byte value): TBD (run script in [docs/benchmarks.md](docs/benchmarks.md)).

## Testing and Quality

The test suite covers data structure correctness, framing edge cases, concurrent stress, and durability:

```bash
# Unit tests (hash table, skiplist, parser framing, eviction, ttl)
./unit_tests

# Blackbox client protocol verification
python tests/test_blackbox.py

# Concurrent thread stress test
python tests/test_stress.py

# AOF crash recovery test (SIGKILL mid-write verification)
python tests/test_persistence_sigkill.py
```

Sanitizer status:
- AddressSanitizer (`-fsanitize=address`): TBD (run `make CXXFLAGS="-fsanitize=address -g -O1"`).
- UndefinedBehaviorSanitizer (`-fsanitize=undefined`): TBD.
- Valgrind memory leak verification: TBD.

## Design Trade-Offs

- **Single-Threaded Reactor**: Removes lock contention, mutex overhead, and cache-line invalidation. Memory access remains cache local without synchronization primitives.
- **Edge-Triggered Drain**: Edge-triggered epoll notifications occur only on readiness state transitions. Buffers must drain until `EAGAIN` to prevent socket starvation.
- **Incremental Rehash**: Avoids blocking the event loop on table expansion by migrating one bucket per query. Lookup queries check both active tables during migration.
- **Fork Copy-on-Write**: Snapshot rewrites leverage OS copy-on-write page tables. Heavy write traffic during rewrites duplicates modified pages, increasing memory footprint.

Technical rationale and systems interview questions are documented in [docs/design-notes.md](docs/design-notes.md).

## Project Layout

```text
RedisClone/
├── include/     Header declarations (dict, skiplist, resp, net, evict, expire, aof, server, common)
├── src/         Implementation files
├── tests/       Unit, integration, stress, and durability test suites
├── benchmark/   Throughput and latency benchmark tools
├── docs/        Architecture design notes and benchmark reproduction details
├── CMakeLists.txt CMake build definition
├── Makefile     POSIX Makefile build definition
└── LICENSE      MIT License
```

## Known Limitations

- Standalone single-node engine; does not support Redis Cluster gossip protocol or Redis Sentinel consensus.
- No Lua scripting execution or multi-key transaction rollback (`MULTI`/`EXEC`).
- Background AOF rewrite uses POSIX `fork()` copy-on-write semantics on Linux; Windows uses a snapshot worker thread fallback.

## License

This project is released under the [MIT License](LICENSE).  
Maintained by [Jayasimha Reddy K](https://github.com/JAYASIMHAREDDYK).
