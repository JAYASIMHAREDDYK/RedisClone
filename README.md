# Networked In-Memory Key-Value Datastore (Redis Clone)

High-performance, standalone in-memory key-value datastore implemented in C++17 speaking native RESP2 (Redis Serialization Protocol).

## Architecture

- **Event Loop & Reactor**: Single-threaded non-blocking reactor utilizing edge-triggered `epoll` (`EPOLLET`) on POSIX Linux and non-blocking polling on Windows. Drains buffers to eliminate starvation.
- **Progressive Hash Table**: Incremental rehashing across dual tables (`ht[0]` and `ht[1]`). Migrates buckets progressively per command to prevent tail-latency spikes.
- **SkipList with Span Metadata**: Custom 32-level geometric distribution skip list maintaining forward pointers and level spans for $O(\log N)$ rank calculation and range slicing.
- **Pluggable Eviction Subsystem**: $O(1)$ Least Frequently Used (LFU) doubly-linked frequency bucket manager and Sampled LRU with configurable memory caps.
- **Key Expiration Subsystem**: Passive (lazy) expiration on key access combined with active periodic randomized sampling.
- **AOF Durability Engine**: Append-only log recording canonical RESP mutation commands with configurable `fsync` policies (`always`, `everysec`, `no`) and non-blocking background rewrite (`BGREWRITEAOF`) via `fork()` copy-on-write semantics.

## Supported Commands

- `PING [message]`
- `ECHO message`
- `SET key value [EX seconds]`
- `GET key`
- `DEL key [key ...]`
- `EXISTS key [key ...]`
- `EXPIRE key seconds`
- `TTL key`
- `ZADD key score member`
- `ZRANGE key start stop [WITHSCORES]`
- `ZRANGEBYSCORE key min max [WITHSCORES]`
- `ZSCORE key member`
- `ZCARD key`
- `BGREWRITEAOF`
- `INFO [section]`

## Building

### CMake

```bash
mkdir build
cd build
cmake ..
cmake --build . --config Release
```

### Make

```bash
make
```

Binaries produced:
- `redis-server` (or `redis-server.exe`)
- `unit_tests` (or `unit_tests.exe`)

## Running

```bash
./redis-server -p 6379 -m 536870912 --policy allkeys-lfu --aof yes --fsync everysec
```

Flags:
- `-p, --port <port>`: Port to listen on (default 6379)
- `-h, --host <ip>`: Bind address (default 0.0.0.0)
- `-m, --maxmemory <bytes>`: Maximum memory threshold before eviction
- `--policy <policy>`: `noeviction`, `allkeys-lru`, `volatile-lru`, `allkeys-lfu`, `volatile-lfu`
- `--aof <yes|no>`: Enable Append-Only File persistence
- `--fsync <always|everysec|no>`: Fsync policy

## Verification & Tests

### Unit Tests
```bash
./unit_tests
```

### Integration Tests
```bash
python tests/test_integration.py
```

### Stress Tests
```bash
python tests/test_stress.py
```

### Benchmark Suite
```bash
python benchmark/benchmark.py 6379 50 100000 16
```

## Performance Benchmark

Measured on single core:

| Command | Pipelined Throughput | Unpipelined Throughput | p50 Latency | p99 Latency |
| :--- | :--- | :--- | :--- | :--- |
| `SET` | 812,901 ops/sec | 79,971 ops/sec | 0.032 ms | 0.197 ms |
| `GET` | 687,819 ops/sec | 78,622 ops/sec | 0.038 ms | 0.243 ms |
| `ZADD` | 801,302 ops/sec | 61,396 ops/sec | 0.057 ms | 0.124 ms |
| `PING` | 532,412 ops/sec | 61,950 ops/sec | 0.051 ms | 0.336 ms |
