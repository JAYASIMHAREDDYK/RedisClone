#!/usr/bin/env python3
"""
CI Regression Smoke Check:
Verifies that pipelined SET throughput does not collapse due to hot-path regressions
(such as the 1MB memmove replication backlog regression).
Fails if pipelined SET drops below the 200,000 ops/sec loose floor in CI.
"""

import socket
import subprocess
import time
import sys
import os

PORT = 6389
NUM_COMMANDS = 100_000
PIPELINE_BATCH = 50
FLOOR_RPS = 200_000

def run_smoke_check():
    # Start server with --aof no
    proc = subprocess.Popen(["./redis-server", "-p", str(PORT), "--aof", "no"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.5)

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", PORT))
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # Pre-format a single SET command frame
        cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        batch = cmd * PIPELINE_BATCH
        expected_responses = b"+OK\r\n" * PIPELINE_BATCH

        batches = NUM_COMMANDS // PIPELINE_BATCH
        start_time = time.perf_counter()

        for _ in range(batches):
            s.sendall(batch)
            buf = bytearray()
            while len(buf) < len(expected_responses):
                chunk = s.recv(len(expected_responses) - len(buf))
                if not chunk:
                    raise RuntimeError("Server disconnected prematurely during smoke check")
                buf.extend(chunk)

        elapsed = time.perf_counter() - start_time
        rps = NUM_COMMANDS / elapsed
        print(f"[SMOKE CHECK] Completed {NUM_COMMANDS:,} pipelined SETs in {elapsed:.4f}s ({rps:,.0f} ops/sec)")

        if rps < FLOOR_RPS:
            print(f"[FAIL] Pipelined SET throughput {rps:,.0f} ops/sec is below regression floor of {FLOOR_RPS:,.0f} ops/sec!", file=sys.stderr)
            sys.exit(1)
        else:
            print(f"[PASS] Pipelined SET throughput exceeds floor ({rps:,.0f} >= {FLOOR_RPS:,.0f} ops/sec)")
            sys.exit(0)

    finally:
        proc.terminate()
        proc.wait()

if __name__ == "__main__":
    run_smoke_check()
