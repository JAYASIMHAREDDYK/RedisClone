# RedisClone

> High-performance, single-threaded in-memory key-value store in C++17 implementing the RESP2 protocol, progressive dual-table rehashing, SkipList sorted sets, copy-on-write AOF persistence, and asynchronous primary-replica streaming replication with offset backlog and partial resync.

[![CI](https://github.com/JAYASIMHAREDDYK/RedisClone/actions/workflows/ci.yml/badge.svg)](https://github.com/JAYASIMHAREDDYK/RedisClone/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![C++17](https://img.shields.io/badge/Standard-C%2B%2B17-purple.svg)](https://en.wikipedia.org/wiki/C%2B%2B17)
[![Platform: Linux / POSIX](https://img.shields.io/badge/Platform-Linux%20%2F%20POSIX-orange.svg)]()
[![Docker: Ready](https://img.shields.io/badge/Docker-Ready-blue.svg)](Dockerfile)

---

## Measured Engineering Highlights

- **Head-to-Head Benchmark vs Official Redis 7.0**: Tested across 5 interleaved runs using identical flags (`redis-benchmark -c 50 -P 16 -n 1000000 -t set,get`), pinned to isolated cores with `taskset` in WSL2 Debian 12 (both in-memory without AOF in the same session):
  - **Standard Key Workload**:
    - **Pipelined SET**: **2,493,766 ops/sec** (**125.9%** of official Redis 1,980,198 ops/sec; p50: 0.495 ms).
    - **Pipelined GET**: **2,801,120 ops/sec** (**120.2%** of official Redis 2,331,002 ops/sec; p50: 0.335 ms).
  - **Random Keyspace Workload (`-r 1,000,000`)**:
    - **Pipelined SET**: **766,871 ops/sec** (**83.4%** of official Redis 919,963 ops/sec; p50: 1.303 ms).
    - **Pipelined GET**: **970,874 ops/sec** (**80.1%** of official Redis 1,212,121 ops/sec; p50: 1.015 ms).
- **Fuzzed RESP Parser (4 Bugs Found & Fixed)**: Hardened via LLVM `libFuzzer` + AddressSanitizer/UBSan across **1,255,730 executions** (~40,500 exec/sec), uncovering and resolving 4 edge-case vulnerabilities (OOM DoS via unbounded multi-bulk array reservation, `size_t` integer overflow on bulk lengths, connection hang on blank inline commands, and null bulk frame handling). See [docs/fuzzing.md](docs/fuzzing.md).
- **Quantified Failover Write Durability**: Empirically measured acknowledged write loss by killing the primary with `SIGKILL` mid-stream:
  - Lost **5,327 of 10,000 acknowledged writes under async failover versus 0 of 300 with WAIT** *when promoting the acknowledging replica*, trading non-blocking write speed for synchronous barrier latency (~1.2 ms per batch). Note: `WAIT` ensures safety only if the acknowledged replica is the one promoted. See [benchmark/results/failover_durability.txt](benchmark/results/failover_durability.txt).

---

## Overview

RedisClone is a standalone, single-threaded in-memory datastore engineered around an edge-triggered non-blocking event loop. It speaks standard RESP2 to remain compatible with existing Redis drivers and command-line utilities. Core storage relies on a dual-table progressive hash table to amortize rehashing costs across queries. Persistence is provided through an append-only file with periodic synchronization and copy-on-write background log rewriting. High availability is supported through asynchronous primary-replica streaming replication, a circular ring replication backlog, and partial resynchronization (`+CONTINUE`).

| Workload Configuration | Operation | RedisClone (Ops/sec) | Official Redis 7.0 (Ops/sec) | % of Redis | p50 Latency (RedisClone) |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Standard Key** | `SET` | **2,493,766** | 1,980,198 | **125.9%** | 0.495 ms |
| **Standard Key** | `GET` | **2,801,120** | 2,331,002 | **120.2%** | 0.335 ms |
| **Random Keyspace (`-r 1M`)** | `SET` | **766,871** | 919,963 | **83.4%** | 1.303 ms |
| **Random Keyspace (`-r 1M`)** | `GET` | **970,874** | 1,212,121 | **80.1%** | 1.015 ms |

*Note: Benchmarks were executed inside WSL2 Debian 12 (pinned with `taskset`); WSL2 virtualization introduces scheduling noise compared to bare-metal Linux. All figures represent medians across 5 interleaved runs with $n = 1,000,000$, $P = 16$, $c = 50$, both operating in-memory (`--aof no` vs `--appendonly no --save ""`). Profile call graphs and flame graph archived in [benchmark/results/flamegraph.svg](benchmark/results/flamegraph.svg); raw outputs archived in `benchmark/results/`.*

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
3. **Semi-Synchronous Replication (`WAIT`)**: Clients issuing `WAIT 1 <timeout_ms>` block until the replica confirms applying the primary's replication offset, ensuring zero acknowledged write loss *when the acknowledging replica is promoted*.

### Persistence and Crash Recovery
- `always`: Calls synchronous flush immediately on each write. Zero unacknowledged data loss, but throughput is bound by disk I/O.
- `everysec` (Default): Writes append to the OS page cache per query; a dedicated background thread invokes `fdatasync()` once per second. In the event of an ungraceful crash (`SIGKILL`, power failure), up to 1 second of transactions can be lost.
- `no`: Relies on kernel page buffer flushing heuristics.

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
