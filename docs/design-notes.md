# Design Notes and Systems Architecture

This document covers internal design decisions, trade-offs, and systems programming principles implemented in this datastore.

---

## Technical Questions and Answers

### 1. Why does edge-triggered epoll require reading until EAGAIN?

Level-triggered epoll notifies user space whenever a file descriptor has readable data pending in kernel buffers. In contrast, edge-triggered epoll (`EPOLLET`) only generates an event when the socket transitions readiness states (for example, when new bytes arrive on an idle socket).

If the reader routine consumes only a subset of available bytes and returns to `epoll_wait()`, the kernel will not generate another event for the remaining buffered bytes until new data arrives over TCP. If the client is awaiting a response before transmitting further data, both client and server deadlock.

Therefore, an edge-triggered event handler must drain the socket buffer in a loop until `read()` or `recv()` returns `-1` with `errno == EAGAIN` or `EWOULDBLOCK`.

### 2. What happens if a command arrives split across two read() calls?

TCP is a byte-stream protocol without frame or message boundaries. Segment fragmentation and OS buffer limits can cause a command such as `*3\r\n$3\r\nSET\r\n...` to be split across arbitrary packet boundaries.

The `RespParser` state machine evaluates the input buffer incrementally:
1. It looks for the array length delimited by `\r\n`.
2. For each element, it reads the prefix `$`, parses the bulk string byte length, and verifies whether the complete byte sequence plus the terminating `\r\n` is present in the buffer.
3. If insufficient bytes are present, `next_command()` leaves the buffer intact and returns `false`.
4. The event loop resumes polling and appends the subsequent packet on the next readable notification before retrying the parse.

### 3. Why is incremental rehash needed, and what do lookups do during rehash?

For an in-memory dictionary holding millions of keys, resizing the hash table all at once requires allocating a larger table and rehashing every key synchronously. In a single-threaded server, this stalls the event loop for tens of milliseconds, causing severe latency spikes.

Incremental rehashing maintains two hash tables:
- `ht[0]`: Current active table being drained.
- `ht[1]`: New expanded table (next power of two).

Every query (`GET`, `SET`, `DEL`, `EXISTS`) invokes `step_rehash(1)`, migrating one non-empty bucket from `ht[0]` to `ht[1]`.
During rehashing:
- Probing checks `ht[0]` first. If the key is not present, it probes `ht[1]`.
- Deletions check `ht[0]`, then `ht[1]`.
- Insertions write directly into `ht[1]`. This ensures `ht[0]` monotonically drains until empty, at which point `ht[0]` is replaced with `ht[1]` and rehashing terminates.

### 4. What does fork() copy, and why can a rewrite double memory under heavy writes?

`fork()` duplicates the parent process's page table entries rather than the physical RAM pages. Both parent and child processes initially point to the identical physical memory pages marked read-only with copy-on-write (COW) flags.

When either process writes to a page, the CPU hardware raises a page fault, prompting the OS kernel to allocate a private 4KB copy for that process.

During `BGREWRITEAOF`, the child process traverses the static memory snapshot to generate the compacted AOF file. If the parent experiences high write throughput modifying existing keys during the rewrite window, the OS copies each modified 4KB page. In the worst case where every page is modified, the resident memory footprint approaches twice the original size (2x RSS).

### 5. What does everysec lose on a crash, and why?

With `FsyncPolicy::EverySec`, the main event loop calls `write()` on every mutating command. This writes data to the OS page cache immediately without blocking the event loop on synchronous disk I/O. A dedicated background thread invokes `fdatasync()` once per second.

If the server terminates ungracefully (`SIGKILL`, kernel panic, power loss), any transactions committed to the page cache during the preceding 1-second interval that have not yet been flushed to non-volatile storage will be lost.

### 6. Why maintain a member->score map next to the skiplist?

A SkipList orders elements by `score` to enable $O(\log N)$ range scans and rank lookups. However, locating an element by `member` in a SkipList requires an $O(N)$ linear traversal across all nodes because the index key is the score, not the member name.

Maintaining a hash map (`dict[member] -> score`) alongside the SkipList provides:
- $O(1)$ membership checks (`HEXISTS` equivalent).
- $O(1)$ score lookups (`ZSCORE`).
- $O(\log N)$ score updates in `ZADD`: the old score is retrieved in $O(1)$, the old node is unlinked in $O(\log N)$, and the new node is inserted with the updated score in $O(\log N)$.
