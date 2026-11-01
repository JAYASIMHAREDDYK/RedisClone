#!/usr/bin/env python3
"""
Automated 5-Run Interleaved Benchmark Suite
Tests RedisClone vs Official Redis 7.0 back-to-back in the exact same session.
Configurations:
  1. Standard Key: redis-benchmark -c 50 -P 16 -n 1000000 -t set,get
  2. Random 1M Keyspace: redis-benchmark -c 50 -P 16 -n 1000000 -r 1000000 -t set,get
Both servers run in-memory (--aof no and --appendonly no --save "").
Pinned with taskset (server on core 2, benchmark on cores 3,4 if available).
"""

import subprocess
import time
import os
import sys
import re
import statistics

RESULTS_DIR = "benchmark/results"
RUNS = 5
N_REQUESTS = 1_000_000
CLIENTS = 50
PIPELINE = 16

def parse_benchmark_output(text):
    results = {}
    current_test = None

    for line in text.splitlines():
        line = line.strip()
        if line.startswith("====== ") and line.endswith(" ======"):
            current_test = line.replace("=", "").strip().upper()
            results[current_test] = {}
        elif current_test:
            m_rps = re.search(r"([\d\.]+)\s+requests per second", line)
            if m_rps:
                results[current_test]["rps"] = float(m_rps.group(1))
            m_p50 = re.search(r"50(?:\.0+)?%\s+<=\s+([\d\.]+)\s+milliseconds", line)
            if m_p50:
                results[current_test]["p50"] = float(m_p50.group(1))
            m_p99 = re.search(r"99(?:\.0+)?%\s+<=\s+([\d\.]+)\s+milliseconds", line)
            if m_p99:
                results[current_test]["p99"] = float(m_p99.group(1))

    return results

def run_single_test(server_cmd, port, bench_extra_flags, log_filename):
    # Kill any lingering instances
    subprocess.run(["pkill", "-9", "-f", "redis-server"], stderr=subprocess.DEVNULL)
    time.sleep(0.3)

    # Detect if taskset is available
    has_taskset = subprocess.run(["which", "taskset"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0

    server_prefix = ["taskset", "-c", "2"] if has_taskset else []
    bench_prefix = ["taskset", "-c", "3,4"] if has_taskset else []

    server_proc = subprocess.Popen(server_prefix + server_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.8)

    bench_cmd = bench_prefix + [
        "redis-benchmark", "-h", "127.0.0.1", "-p", str(port),
        "-c", str(CLIENTS), "-P", str(PIPELINE), "-n", str(N_REQUESTS),
        "-t", "set,get"
    ] + bench_extra_flags

    proc = subprocess.run(bench_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    raw_output = proc.stdout

    filepath = os.path.join(RESULTS_DIR, log_filename)
    with open(filepath, "w") as f:
        f.write(raw_output)

    server_proc.terminate()
    try:
        server_proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        server_proc.kill()
    time.sleep(0.3)

    parsed = parse_benchmark_output(raw_output)
    return parsed

def summarize(name, runs_data):
    def get_metric(cmd, metric):
        vals = [r[cmd][metric] for r in runs_data if cmd in r and metric in r[cmd]]
        return statistics.median(vals) if vals else 0.0

    return {
        "name": name,
        "set_rps": get_metric("SET", "rps"),
        "set_p50": get_metric("SET", "p50"),
        "set_p99": get_metric("SET", "p99"),
        "get_rps": get_metric("GET", "rps"),
        "get_p50": get_metric("GET", "p50"),
        "get_p99": get_metric("GET", "p99"),
    }

def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)

    redis_cmd = ["/usr/bin/redis-server", "--port", "6380", "--save", "", "--appendonly", "no", "--protected-mode", "no"]
    clone_cmd = ["./redis-server", "-p", "6379", "--aof", "no"]

    redis_std_runs = []
    clone_std_runs = []
    redis_rand_runs = []
    clone_rand_runs = []

    print("==========================================================================================")
    print("  Executing 5-Run Interleaved Benchmark Suite (n = 1,000,000, P = 16, c = 50)")
    print("==========================================================================================")

    for run_idx in range(1, RUNS + 1):
        print(f"\n--- Iteration {run_idx}/{RUNS} ---")

        # 1. Official Redis (Standard Key)
        print(f"  [1/4] Redis 7.0 (Standard Key)...")
        r_std = run_single_test(redis_cmd, 6380, [], f"redis_std_run_{run_idx}.txt")
        redis_std_runs.append(r_std)
        print(f"        -> SET: {r_std.get('SET',{}).get('rps',0):,.0f} ops/s, GET: {r_std.get('GET',{}).get('rps',0):,.0f} ops/s")

        # 2. RedisClone (Standard Key)
        print(f"  [2/4] RedisClone (Standard Key)...")
        c_std = run_single_test(clone_cmd, 6379, [], f"redisclone_std_run_{run_idx}.txt")
        clone_std_runs.append(c_std)
        print(f"        -> SET: {c_std.get('SET',{}).get('rps',0):,.0f} ops/s, GET: {c_std.get('GET',{}).get('rps',0):,.0f} ops/s")

        # 3. Official Redis (Random 1M Keyspace)
        print(f"  [3/4] Redis 7.0 (Random 1M Keyspace: -r 1000000)...")
        r_rand = run_single_test(redis_cmd, 6380, ["-r", "1000000"], f"redis_rand_run_{run_idx}.txt")
        redis_rand_runs.append(r_rand)
        print(f"        -> SET: {r_rand.get('SET',{}).get('rps',0):,.0f} ops/s, GET: {r_rand.get('GET',{}).get('rps',0):,.0f} ops/s")

        # 4. RedisClone (Random 1M Keyspace)
        print(f"  [4/4] RedisClone (Random 1M Keyspace: -r 1000000)...")
        c_rand = run_single_test(clone_cmd, 6379, ["-r", "1000000"], f"redisclone_rand_run_{run_idx}.txt")
        clone_rand_runs.append(c_rand)
        print(f"        -> SET: {c_rand.get('SET',{}).get('rps',0):,.0f} ops/s, GET: {c_rand.get('GET',{}).get('rps',0):,.0f} ops/s")

    s_r_std = summarize("Official Redis 7.0 (Std)", redis_std_runs)
    s_c_std = summarize("RedisClone (Std)", clone_std_runs)
    s_r_rand = summarize("Official Redis 7.0 (Rand 1M)", redis_rand_runs)
    s_c_rand = summarize("RedisClone (Rand 1M)", clone_rand_runs)

    std_set_pct = (s_c_std["set_rps"] / s_r_std["set_rps"] * 100.0) if s_r_std["set_rps"] > 0 else 0
    std_get_pct = (s_c_std["get_rps"] / s_r_std["get_rps"] * 100.0) if s_r_std["get_rps"] > 0 else 0
    rand_set_pct = (s_c_rand["set_rps"] / s_r_rand["set_rps"] * 100.0) if s_r_rand["set_rps"] > 0 else 0
    rand_get_pct = (s_c_rand["get_rps"] / s_r_rand["get_rps"] * 100.0) if s_r_rand["get_rps"] > 0 else 0

    print("\n" + "=" * 115)
    print("                                      MEASURED BENCHMARK SUMMARY (5-RUN MEDIANS)")
    print("=" * 115)
    print(f"{'Target':<30} | {'SET QPS':<14} | {'SET p50':<9} | {'SET p99':<9} | {'GET QPS':<14} | {'GET p50':<9} | {'GET p99':<9}")
    print("-" * 115)
    for s in [s_r_std, s_c_std, s_r_rand, s_c_rand]:
        print(f"{s['name']:<30} | {s['set_rps']:<14,.0f} | {s['set_p50']:<7.3f}ms | {s['set_p99']:<7.3f}ms | {s['get_rps']:<14,.0f} | {s['get_p50']:<7.3f}ms | {s['get_p99']:<7.3f}ms")
    print("-" * 115)
    print(f"Standard Workload Comparison:    SET = {std_set_pct:.1f}% of Redis, GET = {std_get_pct:.1f}% of Redis")
    print(f"Random 1M Keyspace Comparison:   SET = {rand_set_pct:.1f}% of Redis, GET = {rand_get_pct:.1f}% of Redis")
    print("=" * 115)

    # Save summary markdown
    summary_path = os.path.join(RESULTS_DIR, "benchmark_summary.md")
    with open(summary_path, "w") as f:
        f.write("# Measured Benchmark Summary: RedisClone vs Redis 7.0\n\n")
        f.write("- **Methodology**: 5 interleaved runs per target, median reported.\n")
        f.write("- **Flags**: `redis-benchmark -c 50 -P 16 -n 1000000 -t set,get` (with and without `-r 1000000`)\n")
        f.write("- **Environment**: Identical single session, both in-memory without AOF (`--aof no` vs `--appendonly no --save \"\"`).\n")
        f.write("- **Affinity**: Server pinned to Core 2, benchmark client pinned to Cores 3,4 via `taskset`.\n\n")
        f.write("| Workload | Engine | SET Throughput (ops/sec) | SET p50 | SET p99 | GET Throughput (ops/sec) | GET p50 | GET p99 |\n")
        f.write("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
        f.write(f"| Standard Key | **Redis 7.0** | {s_r_std['set_rps']:,.0f} | {s_r_std['set_p50']:.3f} ms | {s_r_std['set_p99']:.3f} ms | {s_r_std['get_rps']:,.0f} | {s_r_std['get_p50']:.3f} ms | {s_r_std['get_p99']:.3f} ms |\n")
        f.write(f"| Standard Key | **RedisClone** | {s_c_std['set_rps']:,.0f} | {s_c_std['set_p50']:.3f} ms | {s_c_std['set_p99']:.3f} ms | {s_c_std['get_rps']:,.0f} | {s_c_std['get_p50']:.3f} ms | {s_c_std['get_p99']:.3f} ms |\n")
        f.write(f"| Random 1M Keys | **Redis 7.0** | {s_r_rand['set_rps']:,.0f} | {s_r_rand['set_p50']:.3f} ms | {s_r_rand['set_p99']:.3f} ms | {s_r_rand['get_rps']:,.0f} | {s_r_rand['get_p50']:.3f} ms | {s_r_rand['get_p99']:.3f} ms |\n")
        f.write(f"| Random 1M Keys | **RedisClone** | {s_c_rand['set_rps']:,.0f} | {s_c_rand['set_p50']:.3f} ms | {s_c_rand['set_p99']:.3f} ms | {s_c_rand['get_rps']:,.0f} | {s_c_rand['get_p50']:.3f} ms | {s_c_rand['get_p99']:.3f} ms |\n\n")
        f.write(f"### Performance Relative to Official Redis 7.0\n")
        f.write(f"- **Standard Key Workload**: SET = **{std_set_pct:.1f}%**, GET = **{std_get_pct:.1f}%**\n")
        f.write(f"- **Random 1M Keyspace Workload**: SET = **{rand_set_pct:.1f}%**, GET = **{rand_get_pct:.1f}%**\n")

if __name__ == "__main__":
    main()
