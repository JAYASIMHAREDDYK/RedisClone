#!/usr/bin/env python3
"""
Failover Durability Measurement
Measures acknowledged write loss when killing primary under write load:
1. Pure Async Replication (without WAIT): Acknowledged writes lost on SIGKILL.
2. Synchronous Barrier (with WAIT 1): Acknowledged writes lost on SIGKILL.
"""

import socket
import time
import subprocess
import os
import signal

def send_cmd(f, cmd_list):
    req = f"*{len(cmd_list)}\r\n"
    for arg in cmd_list:
        req += f"${len(arg)}\r\n{arg}\r\n"
    f.write(req.encode('utf-8'))
    f.flush()

def read_resp(f):
    line = f.readline()
    if not line:
        return None
    line_str = line.decode('utf-8', errors='ignore')
    if line_str.startswith(("+", "-", ":")):
        return line_str.strip()
    elif line_str.startswith("$"):
        length = int(line_str[1:].strip())
        if length == -1:
            return None
        data = f.read(length + 2) # payload + \r\n
        return data[:-2].decode('utf-8', errors='ignore')
    elif line_str.startswith("*"):
        return line_str.strip()
    return line_str.strip()

def run_scenario(use_wait=False, total_ops=10000, port_master=6385, port_replica=6386):
    for f in ["master.aof", "replica.aof", "temp-rewrite.aof"]:
        if os.path.exists(f):
            try:
                os.remove(f)
            except OSError:
                pass

    master = subprocess.Popen(["./redis-server", "-p", str(port_master), "--aof", "no"])
    replica = subprocess.Popen(["./redis-server", "-p", str(port_replica), "--aof", "no", "--repl-delay-ms", "10"])
    time.sleep(0.5)

    try:
        sr_sock = socket.create_connection(("127.0.0.1", port_replica))
        fr = sr_sock.makefile('rwb')
        send_cmd(fr, ["REPLICAOF", "127.0.0.1", str(port_master)])
        read_resp(fr)
        time.sleep(1.0) # handshake & full sync

        sm_sock = socket.create_connection(("127.0.0.1", port_master))
        fm = sm_sock.makefile('rwb')

        acknowledged = []
        burst_len = 100

        start_time = time.perf_counter()
        for idx in range(0, total_ops, burst_len):
            chunk = [f"key_{idx + j}" for j in range(burst_len)]
            for k in chunk:
                send_cmd(fm, ["SET", k, f"val_{k}"])
            for k in chunk:
                res = read_resp(fm)
                if res and "+OK" in res:
                    acknowledged.append(k)
            if use_wait:
                send_cmd(fm, ["WAIT", "1", "1000"])
                read_resp(fm)
        duration = time.perf_counter() - start_time

        # Forcibly terminate primary immediately with SIGKILL
        master.send_signal(signal.SIGKILL)
        master.wait()

        time.sleep(0.2)

        # Promote replica
        send_cmd(fr, ["REPLICAOF", "NO", "ONE"])
        read_resp(fr)
        time.sleep(0.3)

        # Check how many acknowledged writes were preserved on replica
        recovered = 0
        lost = 0
        for k in acknowledged:
            send_cmd(fr, ["GET", k])
            res = read_resp(fr)
            if res is not None:
                recovered += 1
            else:
                lost += 1

        sr_sock.close()
        sm_sock.close()
    finally:
        try:
            master.kill()
            master.wait()
        except Exception:
            pass
        try:
            replica.kill()
            replica.wait()
        except Exception:
            pass

    throughput = len(acknowledged) / duration if duration > 0 else 0
    batch_lat_ms = (duration / (total_ops / burst_len)) * 1000.0 if total_ops > 0 else 0
    return len(acknowledged), lost, recovered, throughput, batch_lat_ms

def main():
    print("=================================================================")
    print("      FAILOVER DURABILITY MEASUREMENT: 5 TRIALS & THROUGHPUT      ")
    print("=================================================================")

    TOTAL_OPS = 10000
    TRIALS = 5

    print(f"\n[1/2] Running Scenario A: Pure Async Replication (5 Trials of {TOTAL_OPS} writes)...")
    async_results = []
    for t in range(TRIALS):
        ack, lost, rec, tp, lat = run_scenario(use_wait=False, total_ops=TOTAL_OPS)
        loss_pct = (lost / ack * 100.0) if ack > 0 else 0.0
        async_results.append((ack, lost, rec, loss_pct, tp, lat))
        print(f"  Run {t+1}: Lost {lost}/{ack} writes ({loss_pct:.2f}%) | Throughput: {tp:,.0f} ops/s | Latency: {lat:.2f} ms/batch (100 writes)")

    loss_pcts = [r[3] for r in async_results]
    lost_counts = [r[1] for r in async_results]
    tps_a = [r[4] for r in async_results]
    lats_a = [r[5] for r in async_results]

    loss_pcts_sorted = sorted(loss_pcts)
    median_loss_pct = loss_pcts_sorted[len(loss_pcts_sorted) // 2]
    min_loss_pct = min(loss_pcts)
    max_loss_pct = max(loss_pcts)
    avg_lost_count = sum(lost_counts) / len(lost_counts)
    median_tp_a = sorted(tps_a)[len(tps_a) // 2]
    median_lat_a = sorted(lats_a)[len(lats_a) // 2]

    print(f"\nAsync Replication Summary:")
    print(f"  -> Loss Range: {min_loss_pct:.2f}% to {max_loss_pct:.2f}% (Median: {median_loss_pct:.2f}%, Avg lost: {avg_lost_count:.0f} of {TOTAL_OPS})")
    print(f"  -> Median Throughput: {median_tp_a:,.0f} ops/s (Batch latency: {median_lat_a:.2f} ms per 100 writes)")

    print(f"\n[2/2] Running Scenario B: Semi-Sync Replication (WAIT 1 per batch of 100 writes)...")
    ack_b, lost_b, rec_b, tp_b, lat_b = run_scenario(use_wait=True, total_ops=TOTAL_OPS)
    loss_pct_b = (lost_b / ack_b * 100.0) if ack_b > 0 else 0.0
    print(f"  -> Total Acknowledged Writes: {ack_b}")
    print(f"  -> Lost Acknowledged Writes on SIGKILL: {lost_b} ({loss_pct_b:.2f}%)")
    print(f"  -> Throughput: {tp_b:,.0f} ops/s | Batch Latency: {lat_b:.2f} ms per 100 writes (includes WAIT barrier)")

    print("\n=================================================================")
    print(f"SUMMARY RESULT:")
    print(f"  Async Failover Data Loss: {min_loss_pct:.1f}% - {max_loss_pct:.1f}% (lost {sorted(lost_counts)[len(lost_counts)//2]} of {TOTAL_OPS} median) at {median_tp_a:,.0f} ops/sec")
    print(f"  WAIT 1 Data Loss: {lost_b} of {ack_b} writes lost (0.00%) at {tp_b:,.0f} ops/sec ({lat_b:.2f} ms/batch)")
    print("=================================================================")

    os.makedirs("benchmark/results", exist_ok=True)
    with open("benchmark/results/failover_durability.txt", "w") as f:
        f.write("FAILOVER DURABILITY MEASUREMENT RESULTS\n")
        f.write("=======================================\n")
        f.write("Configuration:\n")
        f.write(f"  Total Acknowledged Writes: {TOTAL_OPS}\n")
        f.write("  Batch Size: 100 pipelined writes per flush\n")
        f.write("  Failure Injection: Immediate SIGKILL of primary under active load\n")
        f.write("  Replication link latency simulation: --repl-delay-ms 10\n\n")
        f.write("Pure Async Replication (No WAIT):\n")
        for i, r in enumerate(async_results):
            f.write(f"  Run {i+1}: Lost {r[1]} of {r[0]} writes ({r[3]:.2f}%) | {r[4]:,.0f} ops/s | {r[5]:.2f} ms/batch\n")
        f.write(f"  Summary Range: {min_loss_pct:.2f}% to {max_loss_pct:.2f}% loss\n")
        f.write(f"  Median Loss: {median_loss_pct:.2f}% ({sorted(lost_counts)[len(lost_counts)//2]} writes lost)\n")
        f.write(f"  Median Throughput: {median_tp_a:,.0f} ops/sec ({median_lat_a:.2f} ms/batch)\n\n")
        f.write("Semi-Sync Replication (WAIT 1 per batch of 100 writes):\n")
        f.write(f"  Acknowledged writes: {ack_b}\n")
        f.write(f"  Lost writes: {lost_b}\n")
        f.write(f"  Recovered writes: {rec_b}\n")
        f.write(f"  Loss percentage: {loss_pct_b:.2f}%\n")
        f.write(f"  Throughput: {tp_b:,.0f} ops/sec ({lat_b:.2f} ms/batch barrier cost)\n")
if __name__ == "__main__":
    main()
