#!/usr/bin/env python3
"""
Automated 5-Run Benchmark Runner
Pins server to Core 2 and redis-benchmark to Cores 3,4 using taskset.
Identical flags: -c 50 -P 16 -n 1000000 -t set,get
Saves all raw outputs to benchmark/results/ and calculates exact medians.
"""

import subprocess
import time
import os
import re
import statistics

RESULTS_DIR = "benchmark/results"
FLAGS = ["-c", "50", "-P", "16", "-n", "1000000", "-t", "set,get"]
RUNS = 5

def parse_benchmark_output(text):
    results = {}
    current_test = None
    
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("====== ") and line.endswith(" ======"):
            current_test = line.replace("=", "").strip().upper()
            results[current_test] = {}
        elif current_test:
            # Match RPS: e.g., "throughput summary: 726253.12 requests per second" or "726253.12 requests per second"
            m_rps = re.search(r"([\d\.]+)\s+requests per second", line)
            if m_rps:
                results[current_test]["rps"] = float(m_rps.group(1))
            # Match p50 and p99: e.g., "50.000% <= 0.039 milliseconds" or "p50=0.039"
            m_p50 = re.search(r"50(?:\.0+)?%\s+<=\s+([\d\.]+)\s+milliseconds", line)
            if m_p50:
                results[current_test]["p50"] = float(m_p50.group(1))
            m_p99 = re.search(r"99(?:\.0+)?%\s+<=\s+([\d\.]+)\s+milliseconds", line)
            if m_p99:
                results[current_test]["p99"] = float(m_p99.group(1))
                
    return results

def run_suite(server_name, start_cmd, port):
    print(f"\n=======================================================")
    print(f"  Benchmarking {server_name} on Port {port} (5 Runs, Pinned with taskset)")
    print(f"=======================================================")
    
    runs_data = []

    for run_idx in range(1, RUNS + 1):
        # Kill any lingering instances
        subprocess.run(["killall", "-9", "redis-server"], stderr=subprocess.DEVNULL)
        time.sleep(0.5)

        # Start server pinned to Core 2
        server_proc = subprocess.Popen(["taskset", "-c", "2"] + start_cmd)
        time.sleep(1.0)

        # Run redis-benchmark pinned to Cores 3,4
        bench_cmd = ["taskset", "-c", "3,4", "redis-benchmark", "-h", "127.0.0.1", "-p", str(port)] + FLAGS
        print(f"[{server_name}] Run {run_idx}/{RUNS} executing: {' '.join(bench_cmd)}...")
        
        proc = subprocess.run(bench_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        raw_output = proc.stdout
        
        # Save raw output
        filename = f"{server_name.lower().replace(' ', '')}_run_{run_idx}.txt"
        filepath = os.path.join(RESULTS_DIR, filename)
        with open(filepath, "w") as f:
            f.write(raw_output)

        parsed = parse_benchmark_output(raw_output)
        runs_data.append(parsed)

        set_rps = parsed.get("SET", {}).get("rps", 0)
        get_rps = parsed.get("GET", {}).get("rps", 0)
        print(f"  -> Run {run_idx}: SET = {set_rps:,.0f} ops/sec, GET = {get_rps:,.0f} ops/sec")

        server_proc.terminate()
        server_proc.wait()
        time.sleep(0.5)

    return runs_data

def summarize(name, runs_data):
    set_rps_list = [r["SET"]["rps"] for r in runs_data if "SET" in r and "rps" in r["SET"]]
    get_rps_list = [r["GET"]["rps"] for r in runs_data if "GET" in r and "rps" in r["GET"]]
    set_p50_list = [r["SET"]["p50"] for r in runs_data if "SET" in r and "p50" in r["SET"]]
    set_p99_list = [r["SET"]["p99"] for r in runs_data if "SET" in r and "p99" in r["SET"]]
    get_p50_list = [r["GET"]["p50"] for r in runs_data if "GET" in r and "p50" in r["GET"]]
    get_p99_list = [r["GET"]["p99"] for r in runs_data if "GET" in r and "p99" in r["GET"]]

    return {
        "name": name,
        "set_rps_median": statistics.median(set_rps_list) if set_rps_list else 0,
        "set_p50_median": statistics.median(set_p50_list) if set_p50_list else 0,
        "set_p99_median": statistics.median(set_p99_list) if set_p99_list else 0,
        "get_rps_median": statistics.median(get_rps_list) if get_rps_list else 0,
        "get_p50_median": statistics.median(get_p50_list) if get_p50_list else 0,
        "get_p99_median": statistics.median(get_p99_list) if get_p99_list else 0,
    }

def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # 1. Official Redis 7.0
    redis_runs = run_suite(
        "Redis",
        ["/usr/bin/redis-server", "--port", "6380", "--save", "", "--appendonly", "no", "--protected-mode", "no"],
        6380
    )

    # 2. RedisClone
    clone_runs = run_suite(
        "RedisClone",
        ["./redis-server", "-p", "6379", "--aof", "no"],
        6379
    )

    s_redis = summarize("Official Redis 7.0", redis_runs)
    s_clone = summarize("RedisClone", clone_runs)

    set_pct = (s_clone["set_rps_median"] / s_redis["set_rps_median"] * 100.0) if s_redis["set_rps_median"] > 0 else 0
    get_pct = (s_clone["get_rps_median"] / s_redis["get_rps_median"] * 100.0) if s_redis["get_rps_median"] > 0 else 0

    print("\n==========================================================================================")
    print("                       BENCHMARK SUMMARY (5-RUN MEDIANS)")
    print("==========================================================================================")
    print(f"{'Target':<20} | {'SET QPS':<14} | {'SET p50':<10} | {'SET p99':<10} | {'GET QPS':<14} | {'GET p50':<10} | {'GET p99':<10}")
    print("-" * 105)
    print(f"{s_redis['name']:<20} | {s_redis['set_rps_median']:<14,.0f} | {s_redis['set_p50_median']:<8.3f}ms | {s_redis['set_p99_median']:<8.3f}ms | {s_redis['get_rps_median']:<14,.0f} | {s_redis['get_p50_median']:<8.3f}ms | {s_redis['get_p99_median']:<8.3f}ms")
    print(f"{s_clone['name']:<20} | {s_clone['set_rps_median']:<14,.0f} | {s_clone['set_p50_median']:<8.3f}ms | {s_clone['set_p99_median']:<8.3f}ms | {s_clone['get_rps_median']:<14,.0f} | {s_clone['get_p50_median']:<8.3f}ms | {s_clone['get_p99_median']:<8.3f}ms")
    print("-" * 105)
    print(f"Honest Performance Relative to Official Redis: SET = {set_pct:.1f}%, GET = {get_pct:.1f}%")
    print("==========================================================================================")

    # Save summary markdown
    with open(os.path.join(RESULTS_DIR, "benchmark_summary.md"), "w") as f:
        f.write("# Measured Benchmark Summary: RedisClone vs Redis 7.0\n\n")
        f.write("- **Methodology**: 5 runs per target, median reported.\n")
        f.write("- **Flags**: `redis-benchmark -c 50 -P 16 -n 1000000 -t set,get`\n")
        f.write("- **Affinity**: Server pinned to Core 2, benchmark client pinned to Cores 3,4 via `taskset`.\n")
        f.write("- **Host**: WSL2 Debian on AMD/Intel multi-core processor.\n\n")
        f.write("| Engine | SET Throughput (ops/sec) | SET p50 | SET p99 | GET Throughput (ops/sec) | GET p50 | GET p99 |\n")
        f.write("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
        f.write(f"| **Redis 7.0** | {s_redis['set_rps_median']:,.0f} | {s_redis['set_p50_median']:.3f} ms | {s_redis['set_p99_median']:.3f} ms | {s_redis['get_rps_median']:,.0f} | {s_redis['get_p50_median']:.3f} ms | {s_redis['get_p99_median']:.3f} ms |\n")
        f.write(f"| **RedisClone** | {s_clone['set_rps_median']:,.0f} | {s_clone['set_p50_median']:.3f} ms | {s_clone['set_p99_median']:.3f} ms | {s_clone['get_rps_median']:,.0f} | {s_clone['get_p50_median']:.3f} ms | {s_clone['get_p99_median']:.3f} ms |\n\n")
        f.write(f"RedisClone achieves **{set_pct:.1f}%** of official Redis SET throughput and **{get_pct:.1f}%** of GET throughput under identical taskset pinned conditions.\n")

if __name__ == "__main__":
    main()
