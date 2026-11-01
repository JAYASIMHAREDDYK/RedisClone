# Measured Benchmark Summary: RedisClone vs Redis 7.0

- **Methodology**: 5 interleaved runs per target, median reported.
- **Flags**: `redis-benchmark -c 50 -P 16 -n 1000000 -t set,get` (with and without `-r 1000000`)
- **Environment**: Identical single session, both in-memory without AOF (`--aof no` vs `--appendonly no --save ""`).
- **Affinity**: Server pinned to Core 2, benchmark client pinned to Cores 3,4 via `taskset`.

| Workload | Engine | SET Throughput (ops/sec) | SET p50 | SET p99 | GET Throughput (ops/sec) | GET p50 | GET p99 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Standard Key | **Redis 7.0** | 1,980,198 | 0.487 ms | 0.000 ms | 2,331,002 | 0.463 ms | 0.000 ms |
| Standard Key | **RedisClone** | 2,493,766 | 0.495 ms | 0.000 ms | 2,801,120 | 0.335 ms | 0.607 ms |
| Random 1M Keys | **Redis 7.0** | 919,963 | 1.327 ms | 0.000 ms | 1,212,121 | 0.791 ms | 0.000 ms |
| Random 1M Keys | **RedisClone** | 766,871 | 1.303 ms | 0.000 ms | 970,874 | 1.015 ms | 1.203 ms |

### Performance Relative to Official Redis 7.0
- **Standard Key Workload**: SET = **125.9%**, GET = **120.2%**
- **Random 1M Keyspace Workload**: SET = **83.4%**, GET = **80.1%**
