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

def run_scenario(use_wait=False, total_ops=50000):
    for f in ["master.aof", "replica.aof", "temp-rewrite.aof"]:
        if os.path.exists(f):
            os.remove(f)

    master = subprocess.Popen(["./redis-server", "-p", "6385", "--aof", "no"])
    replica = subprocess.Popen(["./redis-server", "-p", "6386", "--aof", "no", "--repl-delay-ms", "10"])
    time.sleep(0.5)

    sr_sock = socket.create_connection(("127.0.0.1", 6386))
    fr = sr_sock.makefile('rwb')
    send_cmd(fr, ["REPLICAOF", "127.0.0.1", "6385"])
    read_resp(fr)
    time.sleep(1.0) # handshake & full sync

    sm_sock = socket.create_connection(("127.0.0.1", 6385))
    fm = sm_sock.makefile('rwb')

    acknowledged = []
    stop_writer = False

    def writer_worker():
        nonlocal stop_writer
        idx = 0
        while not stop_writer and idx < total_ops:
            burst_len = 100
            chunk = [f"key_{idx + j}" for j in range(burst_len)]
            try:
                for k in chunk:
                    send_cmd(fm, ["SET", k, f"val_{k}"])
                for k in chunk:
                    res = read_resp(fm)
                    if res and "+OK" in res:
                        acknowledged.append(k)
                if use_wait:
                    send_cmd(fm, ["WAIT", "1", "1000"])
                    read_resp(fm)
                idx += burst_len
            except Exception:
                break

    import threading
    t = threading.Thread(target=writer_worker)
    t.start()

    # Allow load to build up actively
    time.sleep(0.08)

    # Forcibly terminate primary mid-stream while writer is actively receiving acks
    master.send_signal(signal.SIGKILL)
    master.wait()

    stop_writer = True
    t.join()

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

    try:
        sr_sock.close()
    except Exception:
        pass
    replica.terminate()
    replica.wait()

    return len(acknowledged), lost, recovered

def main():
    print("=================================================================")
    print("      FAILOVER DURABILITY MEASUREMENT: LOST ACKNOWLEDGED WRITES   ")
    print("=================================================================")

    print("\n[1/2] Running Scenario A: Pure Async Replication (No WAIT)...")
    ack_a, lost_a, rec_a = run_scenario(use_wait=False, total_ops=10000)
    loss_rate_a = (lost_a / ack_a * 100.0) if ack_a > 0 else 0.0
    print(f"  -> Total Acknowledged Writes: {ack_a}")
    print(f"  -> Preserved Writes on Promoted Replica: {rec_a}")
    print(f"  -> Lost Acknowledged Writes on SIGKILL: {lost_a} ({loss_rate_a:.2f}% data loss)")

    print("\n[2/2] Running Scenario B: Synchronous Replication Barrier (WAIT 1)...")
    ack_b, lost_b, rec_b = run_scenario(use_wait=True, total_ops=1000)
    loss_rate_b = (lost_b / ack_b * 100.0) if ack_b > 0 else 0.0
    print(f"  -> Total Acknowledged Writes: {ack_b}")
    print(f"  -> Preserved Writes on Promoted Replica: {rec_b}")
    print(f"  -> Lost Acknowledged Writes on SIGKILL: {lost_b} ({loss_rate_b:.2f}% data loss)")

    print("\n=================================================================")
    print(f"SUMMARY RESULT:")
    print(f"  Async Replication Failover Loss: {lost_a} of {ack_a} acknowledged writes lost ({loss_rate_a:.2f}%)")
    print(f"  WAIT-Guaranteed Replication Loss: {lost_b} of {ack_b} acknowledged writes lost ({loss_rate_b:.2f}%)")
    print("=================================================================")

    os.makedirs("benchmark/results", exist_ok=True)
    with open("benchmark/results/failover_durability.txt", "w") as f:
        f.write("FAILOVER DURABILITY MEASUREMENT RESULTS\n")
        f.write("=======================================\n")
        f.write(f"Async Replication (No WAIT):\n")
        f.write(f"  Acknowledged writes: {ack_a}\n")
        f.write(f"  Lost writes: {lost_a}\n")
        f.write(f"  Recovered writes: {rec_a}\n")
        f.write(f"  Loss percentage: {loss_rate_a:.2f}%\n\n")
        f.write(f"Semi-Sync Replication (WAIT 1):\n")
        f.write(f"  Acknowledged writes: {ack_b}\n")
        f.write(f"  Lost writes: {lost_b}\n")
        f.write(f"  Recovered writes: {rec_b}\n")
        f.write(f"  Loss percentage: {loss_rate_b:.2f}%\n")

if __name__ == "__main__":
    main()
