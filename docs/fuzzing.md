# Fuzz Testing Report: RESP Parser

This document details the fuzzing strategy, vulnerabilities identified by `libFuzzer` + AddressSanitizer/UBSan, and their resolutions.

---

## Fuzzer Configuration

- **Target**: `tests/fuzz/fuzz_resp.cpp` (`redis::RespParser::feed`, `redis::RespParser::next_command`)
- **Engine**: LLVM `libFuzzer` with `-fsanitize=fuzzer,address,undefined`
- **Compiler**: Clang 14.0.6 (`-O1 -g`)
- **Total Executions**: 1,255,730 runs over 31 seconds (~40,507 exec/sec)

---

## Bugs Discovered and Fixed

### Bug 1: Remote Denial-of-Service via Unchecked Multi-Bulk Allocation (libFuzzer OOM)
- **Symptom**: `ERROR: libFuzzer: out-of-memory (malloc(7111111104))` in `std::vector::reserve`.
- **Root Cause**: `parse_array` parsed the array count via `std::from_chars` without verifying that all characters up to `\r\n` were valid digits (`p == buffer_.data() + crlf`) and without enforcing a maximum array length ceiling. It immediately executed `parsed.reserve(count)`, attempting a 7.1 GB allocation on untrusted input, crashing the server process with an unhandled `std::bad_alloc` or OOM kill.
- **Fix**:
  1. Verified complete numeric parsing (`p == buffer_.data() + crlf`).
  2. Enforced maximum multi-bulk limit (`count <= 1024 * 1024`).
  3. Bounded vector pre-allocation to prevent speculative memory exhaustion: `parsed.reserve(std::min<size_t>(count, 1024))`.

### Bug 2: Size_t Integer Overflow in Bulk String Length Computation
- **Symptom**: Undefined behavior / heap out-of-bounds read bypass.
- **Root Cause**: Bulk string calculation `data_end = data_start + bulk_len` was prone to arithmetic overflow when `bulk_len` was set near `INT64_MAX`. An overflowing `data_end` wrapped around to a small integer, bypassing the `data_end + 2 > buffer_.size()` boundary check.
- **Fix**:
  1. Enforced maximum bulk size conforming to Redis specification (`MAX_BULK_LEN = 512MB`).
  2. Verified numeric termination (`bp == buffer_.data() + next_crlf`).
  3. Added explicit overflow guard: `if (static_cast<uint64_t>(bulk_len) > (SIZE_MAX - data_start - 2)) return false;`.

### Bug 3: Infinite Connection Stall on Empty Inline Lines (`\r\n` / spaces)
- **Symptom**: Connection buffer permanently wedged upon receiving consecutive telnet newlines or empty ping packets.
- **Root Cause**: When an inline command consisted solely of whitespace or `\r\n`, `parse_inline` extracted zero tokens and returned `false`. `next_command` only erased bytes when `ok == true`, meaning the empty newline remained at the head of `buffer_`. Subsequent calls repeatedly re-parsed the exact same bytes indefinitely.
- **Fix**: Re-architected `next_command()` into a loop that unconditionally advances `buffer_` by `consumed` bytes whenever a newline is recognized, discarding empty lines and continuing to subsequent command frames.

### Bug 4: Handling of Null Bulk Strings (`$-1\r\n`)
- **Symptom**: Malformed frame error on standard null bulk representation inside multi-bulk arrays.
- **Fix**: Explicitly recognized `bulk_len == -1` as null parameter and safely advanced parser cursor.

---

## Verification
Following the fixes, `fuzz_resp` executed **1,255,730 iterations** over a 31-second duration with 0 crashes, 0 memory leaks, 0 OOMs, and 0 sanitizer errors.
