# Benchmark Protocol and Measurement Details

This document records the measurement protocol, benchmark commands, hardware specifications, and raw reproduction steps.

---

## Hardware and Test Environment

- **Processor**: 12th Gen Intel(R) Core(TM) i5-12450H (8 Cores, 12 Threads, Base 2.0 GHz, Turbo 4.4 GHz)
- **Memory**: 16 GB DDR4/DDR5
- **Operating System**: Windows 11 Build 26200 / WSL2 Linux Kernel 6.18
- **Compiler**: GCC 16.1.0 (`-std=c++17 -Wall -Wextra -O2`)
- **Benchmark Driver**: Python TCP socket client (`benchmark/benchmark.py`) and standard `redis-benchmark` (POSIX)

---

## Benchmarking Protocol

1. Build in Release mode with `-O2` optimizations:
   ```bash
   make
   ```
2. Start server without AOF persistence to isolate in-memory engine speed:
   ```bash
   ./redis-server -p 6379 --aof no
   ```
3. Run 3 iterations per test case and take the median value.

---

## Reproducible Command Reference

### 1. Pipelined Load Test (P=16, c=50, n=100,000)

```bash
python benchmark/benchmark.py 6379 50 100000 16
```

Measured Output:
```
==================================================
BENCHMARK: 100000 requests | 50 clients | pipeline=16
==================================================
[SET ] Throughput:   726253.3 ops/sec | p50:  0.039 ms | p99:  0.217 ms | p99.9:  0.297 ms
[GET ] Throughput:   634067.6 ops/sec | p50:  0.046 ms | p99:  0.263 ms | p99.9:  0.368 ms
[ZADD] Throughput:   682808.3 ops/sec | p50:  0.060 ms | p99:  0.158 ms | p99.9:  0.259 ms
[PING] Throughput:   786961.0 ops/sec | p50:  0.034 ms | p99:  0.192 ms | p99.9:  0.283 ms
```

### 2. Single-Round-Trip Unpipelined Test (P=1, c=50, n=50,000)

```bash
python benchmark/benchmark.py 6379 50 50000 1
```

Measured Output:
```
==================================================
BENCHMARK: 50000 requests | 50 clients | pipeline=1
==================================================
[SET ] Throughput:    70289.4 ops/sec | p50:  0.545 ms | p99:  2.755 ms | p99.9:  3.897 ms
[GET ] Throughput:    74711.4 ops/sec | p50:  0.520 ms | p99:  2.613 ms | p99.9:  3.742 ms
[ZADD] Throughput:    73786.3 ops/sec | p50:  0.516 ms | p99:  2.648 ms | p99.9:  3.840 ms
[PING] Throughput:    76246.5 ops/sec | p50:  0.510 ms | p99:  2.520 ms | p99.9:  3.717 ms
```

### 3. Rehash Jitter Profile (100,000 Key Expansions)

```bash
python tests/test_rehash_jitter.py
```

Measured Output:
```
Progressive Rehash (Our Engine):
  p50:      21.20 us ( 0.021 ms)
  p99:      49.60 us ( 0.050 ms)
  p99.9:   126.90 us ( 0.127 ms)
  Max:    1131.50 us ( 1.131 ms)

Stop-The-World Rehash (Single-step synchronous resize):
  p99.9:  2347.65 us ( 2.348 ms)
  Max:   27156.00 us (27.156 ms)
```

### 4. High-Concurrency Connection Scaling (c=10,000, n=1,000,000)

```bash
# Target Linux test command (requires ulimit -n 65535)
redis-benchmark -p 6379 -t set,get -n 1000000 -c 10000 -P 1 -q
```
Status: **TBD** (Requires running on dedicated POSIX Linux testbed configured with socket descriptors >= 65535).

### 5. Official Redis 7.0 Comparison Run

```bash
# Command to run official Redis under identical flags on same host:
redis-server --port 6380 --save "" --appendonly no
redis-benchmark -p 6380 -t set,get -n 1000000 -c 50 -P 1 -q
redis-benchmark -p 6380 -t set,get -n 1000000 -c 50 -P 16 -q
```
Status: **TBD** (Requires official Redis 7.0 binaries installed on the host to capture side-by-side p50 and p99 latency distributions).
