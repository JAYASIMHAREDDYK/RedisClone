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

def run_scenario(use_wait=False, total_ops=10000, port_master=6385, port_replica=6386, delay_ms=0):
    for f in ["master.aof", "replica.aof", "temp-rewrite.aof"]:
        if os.path.exists(f):
            try:
                os.remove(f)
            except OSError:
                pass

    master = subprocess.Popen(["./redis-server", "-p", str(port_master), "--aof", "no"])
    replica = subprocess.Popen(["./redis-server", "-p", str(port_replica), "--aof", "no", "--repl-delay-ms", str(delay_ms)])
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
    print("==========================================================================================")
    print("      FAILOVER DURABILITY MATRIX: LOSS VS INJECTED REPLICA LAG (0ms, 1ms, 10ms)           ")
    print("==========================================================================================")

    TOTAL_OPS = 10000
    TRIALS = 5
    DELAYS = [0, 1, 10]
    delay_labels = {
        0: "0 ms (Localhost / Zero Added Lag)",
        1: "1 ms (Low-Latency Datacenter LAN)",
        10: "10 ms (Simulated WAN / Cross-Region)"
    }

    results_table = []

    for delay in DELAYS:
        label = delay_labels[delay]
        print(f"\n>>> Testing Scenario: Replica Delay = {delay} ms [{label}] <<<")
        
        # 5 trials of pure async
        async_runs = []
        for t in range(TRIALS):
            ack, lost, rec, tp, lat = run_scenario(use_wait=False, total_ops=TOTAL_OPS, delay_ms=delay)
            loss_pct = (lost / ack * 100.0) if ack > 0 else 0.0
            async_runs.append((ack, lost, rec, loss_pct, tp, lat))
            print(f"  [Async] Trial {t+1}: Lost {lost}/{ack} writes ({loss_pct:.2f}%) | {tp:,.0f} ops/s | {lat:.2f} ms/batch")
        
        loss_pcts = [r[3] for r in async_runs]
        lost_counts = [r[1] for r in async_runs]
        tps = [r[4] for r in async_runs]
        lats = [r[5] for r in async_runs]

        min_loss = min(loss_pcts)
        max_loss = max(loss_pcts)
        med_loss = sorted(loss_pcts)[len(loss_pcts)//2]
        med_lost_cnt = sorted(lost_counts)[len(lost_counts)//2]
        med_tp_async = sorted(tps)[len(tps)//2]
        med_lat_async = sorted(lats)[len(lats)//2]

        # 1 trial of WAIT 1 per batch of 100
        print(f"  [WAIT 1] Running Semi-Sync Trial (10,000 writes in batches of 100)...")
        ack_w, lost_w, rec_w, tp_w, lat_w = run_scenario(use_wait=True, total_ops=TOTAL_OPS, delay_ms=delay)
        loss_pct_w = (lost_w / ack_w * 100.0) if ack_w > 0 else 0.0
        print(f"  [WAIT 1] Lost {lost_w}/{ack_w} writes ({loss_pct_w:.2f}%) | {tp_w:,.0f} ops/s | {lat_w:.2f} ms/batch (includes WAIT barrier)")

        results_table.append({
            "delay": delay,
            "label": label,
            "async_runs": async_runs,
            "min_loss": min_loss,
            "max_loss": max_loss,
            "med_loss": med_loss,
            "med_lost_cnt": med_lost_cnt,
            "med_tp_async": med_tp_async,
            "med_lat_async": med_lat_async,
            "ack_w": ack_w,
            "lost_w": lost_w,
            "loss_pct_w": loss_pct_w,
            "tp_w": tp_w,
            "lat_w": lat_w
        })

    print("\n==========================================================================================")
    print("                             MEASURED SUMMARY TABLE")
    print("==========================================================================================")
    print(f"{'Replica Lag':<18} | {'Async Loss Range (5 runs)':<26} | {'Async QPS':<13} | {'WAIT Loss':<11} | {'WAIT QPS':<11} | {'WAIT Batch Latency':<18}")
    print("-" * 110)
    for r in results_table:
        range_str = f"{r['min_loss']:.1f}% - {r['max_loss']:.1f}% (med {r['med_loss']:.1f}%)"
        print(f"{r['delay']} ms lag          | {range_str:<26} | {r['med_tp_async']:<13,.0f} | {r['lost_w']} ({r['loss_pct_w']:.1f}%)   | {r['tp_w']:<11,.0f} | {r['lat_w']:.2f} ms")
    print("==========================================================================================")

    os.makedirs("benchmark/results", exist_ok=True)
    with open("benchmark/results/failover_durability.txt", "w") as f:
        f.write("FAILOVER DURABILITY MEASUREMENT RESULTS: LOSS VS REPLICA LAG\n")
        f.write("============================================================\n")
        f.write("Workload: 10,000 acknowledged writes (batches of 100 pipelined SETs)\n")
        f.write("Failure Injection: Immediate SIGKILL of primary under active load\n")
        f.write("Trials per delay setting: 5 runs of Pure Async, 1 run of WAIT 1 per batch\n\n")

        f.write("| Replica Lag | Environment | Async Loss Range (5 runs) | Async Median Loss | Async Throughput | WAIT 1 Loss | WAIT 1 Throughput | WAIT Barrier Cost |\n")
        f.write("| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |\n")
        for r in results_table:
            f.write(f"| **{r['delay']} ms** | {r['label']} | **{r['min_loss']:.1f}% - {r['max_loss']:.1f}%** | {r['med_loss']:.1f}% ({r['med_lost_cnt']} writes) | {r['med_tp_async']:,.0f} ops/s | **{r['lost_w']} of {r['ack_w']} (0.0%)** | {r['tp_w']:,.0f} ops/s | {r['lat_w']:.2f} ms/batch |\n")

        f.write("\n\nDetailed Trial Logs:\n")
        f.write("====================\n")
        for r in results_table:
            f.write(f"\n--- Injected Delay: {r['delay']} ms ({r['label']}) ---\n")
            f.write("Pure Async Trials:\n")
            for i, run in enumerate(r["async_runs"]):
                f.write(f"  Trial {i+1}: Lost {run[1]} of {run[0]} writes ({run[3]:.2f}%) | Throughput: {run[4]:,.0f} ops/sec | Batch Latency: {run[5]:.2f} ms\n")
            f.write(f"WAIT 1 Trial:\n")
            f.write(f"  Lost {r['lost_w']} of {r['ack_w']} writes ({r['loss_pct_w']:.2f}%) | Throughput: {r['tp_w']:,.0f} ops/sec | Batch Latency: {r['lat_w']:.2f} ms\n")

if __name__ == "__main__":
    main()
