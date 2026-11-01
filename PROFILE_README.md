# Jayasimha Reddy K

**Systems & Backend Software Engineer** | Bangalore (Bengaluru), India  
[GitHub](https://github.com/JAYASIMHAREDDYK) • [Email](mailto:[EMAIL_ADDRESS]) • [LinkedIn](https://linkedin.com)

Focused on high-performance systems programming, distributed storage engines, low-latency architectures, and concurrency in modern C++ and distributed backend systems.

---

## 📌 Featured Projects (Pinned)

### 1. [RedisClone](https://github.com/JAYASIMHAREDDYK/RedisClone)
*High-Performance In-Memory Key-Value Store & Primary-Replica Replication Engine in C++17*
- **Head-to-Head Benchmark vs Redis 7.0**: Measured **1.07M SET ops/sec** (53.3% of Redis 7.0) and **2.21M GET ops/sec** (94.9% of Redis 7.0) across 5 interleaved runs with $n = 1,000,000$ and $P = 16$; sustains **598k SET ops/sec** across a 1M random keyspace (`-r 1000000`).
- **Reactor Network Architecture**: Single-threaded edge-triggered `epoll` reactor handling non-blocking TCP streams with zero mutex contention and pipelined batching.
- **Progressive Dual-Table Rehashing**: Amortizes table expansion cost across queries ($O(1)$ per request), bounding max resize jitter to under 1.2 ms.
- **SkipList Sorted Sets**: 32-level geometric SkipList with level spans for $O(\log N)$ rank lookups and range scans, backed by an auxiliary hash map for $O(1)$ member lookups.
- **Async Primary-Replica Replication**: Circular ring replication backlog with 64-bit continuous byte offsets, full snapshot synchronization (`PSYNC ? -1`), partial resynchronization (`+CONTINUE`), and `WAIT` semi-synchronous durability barriers.
- **Failover Measurement**: Empirically measured acknowledged write loss under mid-stream `SIGKILL` failover: 53.3% write loss under pure async replication, reduced to 0% with `WAIT` when promoting the acknowledging replica.
- **Security Hardening**: Hardened with LLVM `libFuzzer`, ASan, and UBSan across 1.25M+ executions, identifying and fixing 4 edge-case vulnerabilities.

### 2. [Sluice](https://github.com/JAYASIMHAREDDYK/Sluice)
*Event-Driven Transaction Ingestion Gateway (Fastify, Kafka, Redis, PostgreSQL)*
- Real-time transaction ingestion engine featuring token-bucket rate limiting implemented via atomic Redis Lua scripts.
- Distributed idempotency guard with 30-second in-flight lease locking and SHA-256 payload hashing to prevent duplicate financial processing.
- High-throughput Kafka producer pipeline (`acks: -1`) decoupling ingestion from asynchronous PostgreSQL batch persistence workers with Dead-Letter-Queue (DLQ) support.

### 3. [OrderMatchingEngine (OME)](https://github.com/JAYASIMHAREDDYK/OrderMatchingEngine)
*Deterministic Low-Latency Limit Order Book in Modern C++*
- Nanosecond-scale trade matching engine implementing price-time priority (`FIFO`) execution.
- Cache-friendly contiguous order book data structures minimizing cache-line invalidation and dynamic heap allocations during high-frequency order placement and cancellation.

### 4. [PolyCache](https://github.com/JAYASIMHAREDDYK/PolyCache)
*Multi-Tiered In-Memory Caching Subsystem*
- High-concurrency caching library supporting multiple eviction policies (O(1) LFU frequency lists, Sampled LRU, and FIFO).
- Dual-layer cache invalidation mechanisms designed for read-heavy distributed microservices.

---

## 🛠️ Technical Skills

- **Languages**: C++ (C++17/20), Python, JavaScript / Node.js, SQL, Bash
- **Systems Programming**: Linux `epoll`, Reactor Pattern, POSIX Sockets, Non-blocking I/O, Copy-on-Write (`fork`), Memory Alignment, Cache-Friendly Data Structures
- **Distributed Systems**: Primary-Replica Streaming, Replication Backlogs, WAL / AOF Durability, Semi-Sync Barriers (`WAIT`), Event Sourcing (Apache Kafka)
- **Quality & Performance**: LLVM `libFuzzer`, AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), Valgrind, `taskset` CPU affinity, Benchmarking
- **DevOps & Infrastructure**: Docker, Docker Compose, CMake, Make, GitHub Actions CI/CD, Linux (Debian/Ubuntu)
