#!/usr/bin/env python3
import socket
import time
import subprocess
import sys
import os

def send_cmd(s, cmd_list):
    req = f"*{len(cmd_list)}\r\n"
    for arg in cmd_list:
        req += f"${len(arg)}\r\n{arg}\r\n"
    s.sendall(req.encode('utf-8'))

def read_resp(s):
    res = b""
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        res += chunk
        # Simple check for completion
        if res.startswith(b"+") or res.startswith(b"-") or res.startswith(b":"):
            if b"\r\n" in res:
                return res.decode('utf-8', errors='ignore')
        elif res.startswith(b"$"):
            # Bulk string
            first_nl = res.find(b"\r\n")
            if first_nl != -1:
                length = int(res[1:first_nl])
                if length == -1:
                    return None
                if len(res) >= first_nl + 2 + length + 2:
                    return res[first_nl + 2:first_nl + 2 + length].decode('utf-8', errors='ignore')
        elif res.startswith(b"*"):
            if b"\r\n" in res:
                return res.decode('utf-8', errors='ignore')
    return res.decode('utf-8', errors='ignore')

def main():
    print("=== Testing Async Replication, Full/Partial Resync, WAIT, and Failover ===")
    
    # Clean old aof
    for f in ["master.aof", "replica.aof", "temp-rewrite.aof"]:
        if os.path.exists(f):
            os.remove(f)

    # Start Master on 6385
    master = subprocess.Popen(["./redis-server", "-p", "6385", "--aof", "no"])
    # Start Replica on 6386
    replica = subprocess.Popen(["./redis-server", "-p", "6386", "--aof", "no"])
    
    time.sleep(0.5)

    try:
        sm = socket.create_connection(("127.0.0.1", 6385))
        sr = socket.create_connection(("127.0.0.1", 6386))

        # Check initial roles
        send_cmd(sm, ["INFO"])
        info_m = read_resp(sm)
        assert "role:master" in info_m, f"Expected master role, got: {info_m}"

        send_cmd(sr, ["INFO"])
        info_r = read_resp(sr)
        assert "role:master" in info_r, f"Expected initial master role, got: {info_r}"

        # Write initial keys to master before replication (tests Full Snapshot Sync)
        for i in range(50):
            send_cmd(sm, ["SET", f"pre_key_{i}", f"val_{i}"])
            res = read_resp(sm)
            assert "+OK" in res

        # Configure Replica: REPLICAOF 127.0.0.1 6385
        send_cmd(sr, ["REPLICAOF", "127.0.0.1", "6385"])
        res = read_resp(sr)
        assert "+OK" in res

        # Allow handshake, full snapshot sync, and online registration
        time.sleep(1.0)

        # Verify replica role is slave/replica
        send_cmd(sr, ["INFO"])
        info_r = read_resp(sr)
        assert "role:slave" in info_r

        # Verify initial 50 snapshot keys exist on replica!
        for i in range(50):
            send_cmd(sr, ["GET", f"pre_key_{i}"])
            val = read_resp(sr)
            assert val == f"val_{i}", f"Snapshot sync failed for pre_key_{i}: got {val}"
        print("[PASS] Full Resync (Snapshot transfer & replay)")

        # Verify replica read-only enforcement
        send_cmd(sr, ["SET", "bad_key", "val"])
        err = read_resp(sr)
        assert "READONLY" in err, f"Replica should reject writes, got: {err}"
        print("[PASS] Read-only replica guard")

        # Test live tail streaming (stream commands over replication stream)
        for i in range(50):
            send_cmd(sm, ["SET", f"live_key_{i}", f"stream_{i}"])
            read_resp(sm)

        time.sleep(0.5)

        for i in range(50):
            send_cmd(sr, ["GET", f"live_key_{i}"])
            val = read_resp(sr)
            assert val == f"stream_{i}", f"Live replication failed for live_key_{i}: got {val}"
        print("[PASS] Live replication stream")

        # Test WAIT command
        send_cmd(sm, ["SET", "wait_key", "wait_val"])
        read_resp(sm)
        send_cmd(sm, ["WAIT", "1", "1000"])
        wait_res = read_resp(sm)
        assert ":1" in wait_res, f"Expected WAIT 1 1000 to return :1, got: {wait_res}"
        print("[PASS] WAIT command synchronization")

        # Test Manual Promotion: REPLICAOF NO ONE
        send_cmd(sr, ["REPLICAOF", "NO", "ONE"])
        res = read_resp(sr)
        assert "+OK" in res

        send_cmd(sr, ["INFO"])
        info_r = read_resp(sr)
        assert "role:master" in info_r, f"Expected role:master after promotion, got {info_r}"
        assert "repl_backlog_active:1" in info_r, f"Promoted replica must have active backlog, got {info_r}"

        # Replica now promoted to Master -> should accept writes AND feed its own backlog!
        offset_before_promoted_write = 0
        for line in info_r.splitlines():
            if line.startswith("master_repl_offset:"):
                offset_before_promoted_write = int(line.split(":")[1])

        send_cmd(sr, ["SET", "promoted_key", "success"])
        res = read_resp(sr)
        assert "+OK" in res
        send_cmd(sr, ["GET", "promoted_key"])
        assert read_resp(sr) == "success"

        send_cmd(sr, ["INFO"])
        info_r_after = read_resp(sr)
        offset_after_promoted_write = 0
        for line in info_r_after.splitlines():
            if line.startswith("master_repl_offset:"):
                offset_after_promoted_write = int(line.split(":")[1])
        assert offset_after_promoted_write > offset_before_promoted_write, (
            f"Promoted master must advance its master_repl_offset on write ({offset_after_promoted_write} > {offset_before_promoted_write})"
        )
        print("[PASS] Manual Promotion (REPLICAOF NO ONE & Backlog Feed)")

        # Verify Master keeps its backlog active even after replica detached
        send_cmd(sm, ["INFO"])
        info_m_post = read_resp(sm)
        assert "repl_backlog_active:1" in info_m_post, f"Master backlog must stay active after replica detach, got {info_m_post}"
        offset_before_detach_write = 0
        for line in info_m_post.splitlines():
            if line.startswith("master_repl_offset:"):
                offset_before_detach_write = int(line.split(":")[1])

        send_cmd(sm, ["SET", "post_detach_key", "active"])
        assert "+OK" in read_resp(sm)

        send_cmd(sm, ["INFO"])
        info_m_after = read_resp(sm)
        offset_after_detach_write = 0
        for line in info_m_after.splitlines():
            if line.startswith("master_repl_offset:"):
                offset_after_detach_write = int(line.split(":")[1])
        assert offset_after_detach_write > offset_before_detach_write, (
            f"Master must continue advancing backlog offset after replica disconnects ({offset_after_detach_write} > {offset_before_detach_write})"
        )
        print("[PASS] Master backlog persists and accumulates across replica disconnection")

        print("\nALL REPLICATION TESTS PASSED SUCCESSFULLY!")

    finally:
        master.terminate()
        replica.terminate()
        master.wait()
        replica.wait()

if __name__ == "__main__":
    main()
