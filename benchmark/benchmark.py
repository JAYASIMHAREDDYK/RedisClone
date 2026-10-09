import socket
import time
import threading
import statistics
import subprocess
import sys
import os

def encode_command(*args):
    buf = [f"*{len(args)}\r\n".encode()]
    for a in args:
        s = str(a).encode()
        buf.append(f"${len(s)}\r\n".encode())
        buf.append(s)
        buf.append(b"\r\n")
    return b"".join(buf)

def worker(host, port, num_requests, pipeline_size, cmd_template, latencies, thread_id):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.connect((host, port))

    batch_cmd = b""
    for i in range(pipeline_size):
        args = [arg.replace("{id}", f"{thread_id}_{i}") for arg in cmd_template]
        batch_cmd += encode_command(*args)

    remaining = num_requests
    local_lats = []

    while remaining > 0:
        cur_batch = min(pipeline_size, remaining)
        payload = batch_cmd if cur_batch == pipeline_size else b"".join([
            encode_command(*[arg.replace("{id}", f"{thread_id}_{j}") for arg in cmd_template])
            for j in range(cur_batch)
        ])

        t0 = time.perf_counter()
        sock.sendall(payload)

        read_responses = 0
        buf = b""
        while read_responses < cur_batch:
            chunk = sock.recv(16384)
            if not chunk:
                break
            buf += chunk
            while b"\r\n" in buf and read_responses < cur_batch:
                line_end = buf.find(b"\r\n")
                first_byte = buf[0:1]
                if first_byte in (b'+', b'-', b':'):
                    buf = buf[line_end + 2:]
                    read_responses += 1
                elif first_byte == b'$':
                    length = int(buf[1:line_end])
                    if length == -1:
                        buf = buf[line_end + 2:]
                        read_responses += 1
                    else:
                        total_need = line_end + 2 + length + 2
                        if len(buf) >= total_need:
                            buf = buf[total_need:]
                            read_responses += 1
                        else:
                            break
                elif first_byte == b'*':
                    buf = buf[line_end + 2:]
                    read_responses += 1
                else:
                    buf = buf[line_end + 2:]
                    read_responses += 1

        t1 = time.perf_counter()
        local_lats.append((t1 - t0) * 1000.0 / cur_batch)
        remaining -= cur_batch

    sock.close()
    latencies.extend(local_lats)

def run_benchmark(host='127.0.0.1', port=6391, total_requests=100000, concurrency=50, pipeline=16):
    proc = None
    try:
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_sock.connect((host, port))
        test_sock.close()
    except:
        server_bin = ".\\redis-server.exe" if os.name == 'nt' else "./redis-server"
        proc = subprocess.Popen([server_bin, "-p", str(port), "--aof", "no"])
        time.sleep(0.5)

    try:
        tests = [
            ("SET", ["SET", "bench_key_{id}", "bench_val_{id}"]),
            ("GET", ["GET", "bench_key_{id}"]),
            ("ZADD", ["ZADD", "bench_zset", "100.5", "member_{id}"]),
            ("PING", ["PING"])
        ]

        print(f"==================================================")
        print(f"BENCHMARK: {total_requests} requests | {concurrency} clients | pipeline={pipeline}")
        print(f"==================================================")

        reqs_per_thread = total_requests // concurrency

        for name, cmd in tests:
            latencies = []
            threads = []

            start_time = time.perf_counter()

            for tid in range(concurrency):
                t = threading.Thread(target=worker, args=(host, port, reqs_per_thread, pipeline, cmd, latencies, tid))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            elapsed = time.perf_counter() - start_time
            total_done = len(latencies) * pipeline
            qps = total_done / elapsed if elapsed > 0 else 0

            latencies.sort()
            p50 = statistics.median(latencies) if latencies else 0
            p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0
            p999 = latencies[int(len(latencies) * 0.999)] if latencies else 0

            print(f"[{name:4s}] Throughput: {qps:10.1f} ops/sec | p50: {p50:6.3f} ms | p99: {p99:6.3f} ms | p99.9: {p999:6.3f} ms")

    finally:
        if proc:
            proc.terminate()
            proc.wait(timeout=2)

if __name__ == "__main__":
    p = int(sys.argv[1]) if len(sys.argv) > 1 else 6391
    c = int(sys.argv[2]) if len(sys.argv) > 2 else 50
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 100000
    pipe = int(sys.argv[4]) if len(sys.argv) > 4 else 16
    run_benchmark(port=p, total_requests=n, concurrency=c, pipeline=pipe)
