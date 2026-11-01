# RedisClone

> High-performance, single-threaded in-memory key-value store in C++17 implementing the RESP2 protocol, progressive dual-table rehashing, SkipList sorted sets, copy-on-write AOF persistence, and distributed asynchronous primary-replica streaming replication with offset backlog and partial resync.

**GitHub Topics**: `redis`, `cpp17`, `reactor-pattern`, `epoll`, `in-memory-database`, `skiplist`, `distributed-systems`, `replication`, `resp2`, `fuzzing`, `systems-programming`

[![CI](https://github.com/JAYASIMHAREDDYK/RedisClone/actions/workflows/ci.yml/badge.svg)](https://github.com/JAYASIMHAREDDYK/RedisClone/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![C++17](https://img.shields.io/badge/Standard-C%2B%2B17-purple.svg)](https://en.wikipedia.org/wiki/C%2B%2B17)
[![Platform: Linux / POSIX](https://img.shields.io/badge/Platform-Linux%20%2F%20POSIX-orange.svg)]()
[![Docker: Ready](https://img.shields.io/badge/Docker-Ready-blue.svg)](Dockerfile)

---

## Measured Engineering Highlights

- **Measured Head-to-Head Benchmark**: Tested against official Redis 7.0 using identical flags (`redis-benchmark -c 50 -P 16 -n 1000000 -t set,get`), pinned to isolated cores with `taskset` across a 5-run median: achieves **83.8% of official Redis 7.0 throughput on pipelined GET** (1,908,397 vs 2,277,904 ops/sec). Raw stdout logs committed in `benchmark/results/`.
- **Fuzzed RESP Parser (4 Bugs Found & Fixed)**: Hardened via LLVM `libFuzzer` + AddressSanitizer/UBSan across **1,255,730 executions** (~40,500 exec/sec), uncovering and resolving 4 vulnerabilities (OOM DoS via unbounded multi-bulk reserve, `size_t` integer overflow on bulk lengths, connection hang on blank inline commands, and null bulk frame handling). See [docs/fuzzing.md](docs/fuzzing.md).
- **Quantified Failover Write Durability**: Empirically measured acknowledged write loss by killing the primary with `SIGKILL` mid-write and promoting the replica: **lost 5,327 of 10,000 acknowledged writes (53.3% loss)** under pure asynchronous replication, dropping to **0 of 300 writes lost (0.0% loss)** when guarded with the `WAIT` semi-synchronous replication barrier. See [benchmark/results/failover_durability.txt](benchmark/results/failover_durability.txt).

---

## Overview

RedisClone is a standalone, single-threaded in-memory datastore engineered around an edge-triggered non-blocking event loop. It speaks standard RESP2 to remain compatible with existing Redis drivers and command-line utilities. Core storage relies on a dual-table progressive hash table to amortize rehashing costs across queries. Persistence is provided through an append-only file with periodic synchronization and copy-on-write background log rewriting. Distributed high availability is supported through asynchronous primary-replica streaming replication, 1MB replication backlog buffers, and partial resynchronization.

| Operation | Pipelined (P=16, c=50) | Unpipelined (P=1, c=50) | p50 Latency (Pipelined P=16 Only) | p99 Latency (Pipelined P=16 Only) |
| :--- | :--- | :--- | :--- | :--- |
| `SET` | 41,494 ops/sec | 38,200 ops/sec | 13.103 ms | 16.055 ms |
| `GET` | 1,908,397 ops/sec | 74,711 ops/sec | 0.615 ms | 0.780 ms |
| `ZADD` | 682,808 ops/sec | 73,786 ops/sec | 0.060 ms | 0.158 ms |
| `PING` | 786,961 ops/sec | 76,246 ops/sec | 0.034 ms | 0.192 ms |

*Note: Latencies sub-0.3 ms apply to pipelined requests (`P=16`) only. Unpipelined requests incur round-trip TCP network scheduling overhead. Benchmarks executed inside WSL2 Debian 12 (pinned with `taskset`); WSL2 virtualization introduces hypervisor scheduling noise compared to bare-metal Linux.*

---

## Features and Status

| Feature Area | Component | Status | Notes |
| :--- | :--- | :--- | :--- |
| Network I/O | Single-threaded Reactor | Implemented | Edge-triggered epoll on Linux; poll fallback on Windows |
| Protocol | RESP2 Parser and Writer | Implemented | Handles bulk strings, arrays, integers, errors, partial reads |
| Key-Value Store | Progressive Hash Table | Implemented | Dual-table progressive rehashing (bounded bucket migration per query) |
| Sorted Sets | SkipList with Level Spans | Implemented | 32-level geometric distribution; O(log N) rank and range scans |
| Expiration | Passive + Active Sampling | Implemented | Lazy eviction on access; 100 ms periodic sampling capped at 10 ms |
| Eviction | O(1) LFU + Sampled LRU | Implemented | Doubly-linked frequency buckets; active on maxmemory cap |
| Persistence | Append-Only File (AOF) | Implemented | always, everysec, and no fsync policies supported |
| Log Compaction | Background AOF Rewrite | Implemented | `fork()` copy-on-write on Linux; worker snapshot fallback on Windows |
| Replication | Async Primary-Replica | Implemented | 1MB backlog, byte offsets, snapshot full sync, `+CONTINUE` partial resync |
| High Availability | Failover & WAIT | Implemented | `REPLICAOF NO ONE` promotion; `WAIT` semi-synchronous replication barrier |

---

## Quick Start

### Build and Run Locally

```bash
git clone https://github.com/JAYASIMHAREDDYK/RedisClone.git
cd RedisClone
make -j$(nproc)
./redis-server -p 6379 --aof yes --fsync everysec
```

### Run with Docker Compose

```bash
docker compose up -d
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

---

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
| `INFO` | O(1) | Returns server statistics, memory, and replication metrics. |
| `REPLICAOF host port` | O(1) trigger | Configures node as replica and connects to master. |
| `REPLICAOF NO ONE` | O(1) | Promotes replica to standalone primary node. |
| `WAIT numreplicas timeout` | O(N) | Blocks until `numreplicas` acknowledge current write offset. |
| `PSYNC repl_id offset` | O(1) / O(N) | Initiates full snapshot resync or partial backlog resync. |

---

## Replication and Durability

### Async Primary-Replica Synchronization
1. **Initial Full Synchronization**: The replica connects and issues `PSYNC ? -1`. The primary responds with `+FULLRESYNC <repl_id> <offset>`, transmits an in-memory snapshot of all active keys, and registers the replica as online.
2. **Backlog Streaming & Partial Resync**: Mutations are buffered in a 1MB circular replication backlog. If a replica reconnects with a valid offset inside the backlog window, the primary returns `+CONTINUE` and streams only the delta.
3. **Semi-Synchronous Replication (`WAIT`)**: Clients issuing `WAIT 1 <timeout_ms>` block until the replica confirms applying the primary's replication offset, reducing failover data loss to zero.

### Persistence and Crash Recovery
- `always`: Calls synchronous flush immediately on each write. Zero unacknowledged data loss, but throughput is bound by disk I/O.
- `everysec` (Default): Writes append to the OS page cache per query; a dedicated background thread invokes `fdatasync()` once per second. In the event of an ungraceful crash (`SIGKILL`, power failure), up to 1 second of transactions can be lost.
- `no`: Relies on kernel page buffer flushing heuristics.

---

## Head-to-Head Benchmarks

Raw benchmark logs from 5 consecutive runs with `redis-benchmark -c 50 -P 16 -n 1000000 -t set,get` (pinned to Core 2 via `taskset`, benchmark pinned to Cores 3,4):

| Engine | SET Throughput (ops/sec) | SET p50 Latency | GET Throughput (ops/sec) | GET p50 Latency |
| :--- | :--- | :--- | :--- | :--- |
| **Official Redis 7.0** | 1,904,762 | 0.591 ms | 2,277,904 | 0.439 ms |
| **RedisClone** | 41,494 | 26.815 ms | 1,908,397 | 0.615 ms |

- **GET Performance**: RedisClone achieves **83.8% of official Redis 7.0 throughput** on pipelined GET operations with sub-millisecond p50 latency.
- **SET Performance**: SET throughput reflects progressive rehash and eviction book-keeping overhead.
- Raw run outputs are archived in [`benchmark/results/`](benchmark/results/). Full architecture and design details are documented in [`docs/design.md`](docs/design.md).

---

## Testing & Quality Assurance

```bash
# Unit tests (hash table, skiplist, parser framing, eviction, ttl)
./unit_tests

# Asynchronous replication, partial resync, WAIT, and failover test
python3 tests/test_replication.py

# Integration, blackbox, and concurrent stress tests
python3 tests/test_integration.py
python3 tests/test_blackbox.py
python3 tests/test_stress.py

# Fuzz testing RESP parser with LLVM libFuzzer
make fuzz
./fuzz_resp -max_total_time=30
```

---

## License

This project is licensed under the [MIT License](LICENSE).  
Maintained by [Jayasimha Reddy K](https://github.com/JAYASIMHAREDDYK).
