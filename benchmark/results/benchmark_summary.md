# Measured Benchmark Summary: RedisClone vs Redis 7.0

- **Methodology**: 5 interleaved runs per target, median reported.
- **Flags**: `redis-benchmark -c 50 -P 16 -n 1000000 -t set,get` (with and without `-r 1000000`)
- **Environment**: Identical single session, both in-memory without AOF (`--aof no` vs `--appendonly no --save ""`).
- **Affinity**: Server pinned to Core 2, benchmark client pinned to Cores 3,4 via `taskset`.

| Workload | Engine | SET Throughput (ops/sec) | SET p50 | SET p99 | GET Throughput (ops/sec) | GET p50 | GET p99 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Standard Key | **Redis 7.0** | 2,000,000 | 0.503 ms | 0.000 ms | 2,331,002 | 0.407 ms | 0.000 ms |
| Standard Key | **RedisClone** | 1,066,098 | 1.039 ms | 1.807 ms | 2,212,390 | 0.703 ms | 0.000 ms |
| Random 1M Keys | **Redis 7.0** | 976,562 | 1.239 ms | 1.055 ms | 1,250,000 | 0.751 ms | 1.303 ms |
| Random 1M Keys | **RedisClone** | 598,444 | 1.959 ms | 0.000 ms | 823,723 | 1.183 ms | 0.000 ms |

### Performance Relative to Official Redis 7.0
- **Standard Key Workload**: SET = **53.3%**, GET = **94.9%**
- **Random 1M Keyspace Workload**: SET = **61.3%**, GET = **65.9%**
