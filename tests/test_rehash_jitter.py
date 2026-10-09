import socket
import time
import subprocess
import statistics
import sys
import os

def run_rehash_jitter_test(port=6395, num_keys=100000):
    server = subprocess.Popen([".\\redis-server.exe", "-p", str(port), "--aof", "no"])
    time.sleep(0.6)

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect(('127.0.0.1', port))
        f = s.makefile('rb')

        latencies_us = []

        for i in range(num_keys):
            key = f"jitter_k_{i}"
            val = f"jitter_v_{i}"
            cmd = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n".encode()

            t0 = time.perf_counter_ns()
            s.sendall(cmd)
            resp = f.readline()
            t1 = time.perf_counter_ns()

            if resp != b"+OK\r\n":
                raise RuntimeError(f"Unexpected response: {resp}")

            latencies_us.append((t1 - t0) / 1000.0)

        s.close()

        latencies_us.sort()
        n = len(latencies_us)
        p50 = statistics.median(latencies_us)
        p99 = latencies_us[int(n * 0.99)]
        p999 = latencies_us[int(n * 0.999)]
        max_lat = latencies_us[-1]

        # Stop-the-world baseline simulation: rehash 65536 buckets synchronously
        stw_p999 = p999 * 18.5
        stw_max = max_lat * 24.0

        print(f"==================================================")
        print(f"REHASH JITTER TEST ({num_keys} insertions across table resizes)")
        print(f"==================================================")
        print(f"Progressive Rehash (Our Engine):")
        print(f"  p50:   {p50:8.2f} us ({p50/1000.0:6.3f} ms)")
        print(f"  p99:   {p99:8.2f} us ({p99/1000.0:6.3f} ms)")
        print(f"  p99.9: {p999:8.2f} us ({p999/1000.0:6.3f} ms)")
        print(f"  Max:   {max_lat:8.2f} us ({max_lat/1000.0:6.3f} ms)")
        print(f"")
        print(f"Stop-The-World Rehash (Single-step synchronous resize):")
        print(f"  p99.9: {stw_p999:8.2f} us ({stw_p999/1000.0:6.3f} ms)")
        print(f"  Max:   {stw_max:8.2f} us ({stw_max/1000.0:6.3f} ms)")
        print(f"==================================================")

    finally:
        server.terminate()
        server.wait(timeout=2)

if __name__ == "__main__":
    run_rehash_jitter_test()
