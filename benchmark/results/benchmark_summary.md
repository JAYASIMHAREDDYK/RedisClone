# Measured Benchmark Summary: RedisClone vs Redis 7.0

- **Methodology**: 5 runs per target, median reported.
- **Flags**: `redis-benchmark -c 50 -P 16 -n 1000000 -t set,get`
- **Affinity**: Server pinned to Core 2, benchmark client pinned to Cores 3,4 via `taskset`.
- **Host**: WSL2 Debian on AMD/Intel multi-core processor.

| Engine | SET Throughput (ops/sec) | SET p50 | SET p99 | GET Throughput (ops/sec) | GET p50 | GET p99 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Redis 7.0** | 1,904,762 | 0.591 ms | 0.000 ms | 2,277,904 | 0.439 ms | 0.000 ms |
| **RedisClone** | 41,494 | 26.815 ms | 57.119 ms | 1,908,397 | 0.615 ms | 0.000 ms |

RedisClone achieves **2.2%** of official Redis SET throughput and **83.8%** of GET throughput under identical taskset pinned conditions.
