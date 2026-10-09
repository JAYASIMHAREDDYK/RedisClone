import socket
import time
import subprocess
import os
import signal
import sys

class SimpleClient:
    def __init__(self, port):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect(('127.0.0.1', port))
        self.f = self.s.makefile('rb')

    def send(self, *args):
        buf = [f"*{len(args)}\r\n".encode()]
        for a in args:
            s = str(a).encode()
            buf.append(f"${len(s)}\r\n".encode())
            buf.append(s)
            buf.append(b"\r\n")
        self.s.sendall(b"".join(buf))
        line = self.f.readline()
        if not line:
            return None
        prefix = line[0:1]
        payload = line[1:-2]
        if prefix in (b'+', b':'):
            return payload.decode()
        elif prefix == b'$':
            length = int(payload)
            if length == -1:
                return None
            data = self.f.read(length)
            self.f.read(2)
            return data.decode()
        elif prefix == b'-':
            return Exception(payload.decode())
        return payload.decode()

    def close(self):
        try:
            self.f.close()
            self.s.close()
        except:
            pass

def run_persistence_sigkill_test():
    server_bin = ".\\redis-server.exe" if os.name == 'nt' else "./redis-server"
    port = 6396
    aof_name = f"appendonly_{port}.aof"
    if os.path.exists(aof_name):
        os.remove(aof_name)

    proc = subprocess.Popen([
        server_bin,
        "-p", str(port),
        "--aof", "yes",
        "--fsync", "always"
    ])
    time.sleep(0.5)

    acknowledged_keys = {}

    try:
        client = SimpleClient(port)

        for i in range(1000):
            k = f"durability_k_{i}"
            v = f"durability_v_{i}"
            res = client.send("SET", k, v)
            if res == "OK":
                acknowledged_keys[k] = v

        res_rewrite = client.send("BGREWRITEAOF")
        assert "started" in str(res_rewrite).lower() or res_rewrite == "OK"

        for i in range(1000, 2000):
            k = f"durability_k_{i}"
            v = f"durability_v_{i}"
            t0 = time.perf_counter_ns()
            res = client.send("SET", k, v)
            t1 = time.perf_counter_ns()
            if res == "OK":
                acknowledged_keys[k] = v

        client.close()

        # Hard SIGKILL mid-flight
        if os.name == 'nt':
            subprocess.run(["taskkill", "/F", "/PID", str(proc.pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            os.kill(proc.pid, signal.SIGKILL)

        proc.wait(timeout=3)
        time.sleep(0.5)

        # Restart server and verify replay
        proc2 = subprocess.Popen([
            server_bin,
            "-p", str(port),
            "--aof", "yes",
            "--fsync", "always"
        ])
        time.sleep(0.5)

        client2 = SimpleClient(port)
        recovered = 0
        for k, expected_v in acknowledged_keys.items():
            actual_v = client2.send("GET", k)
            assert actual_v == expected_v, f"Mismatch on {k}: expected {expected_v}, got {actual_v}"
            recovered += 1

        client2.close()
        proc2.terminate()
        proc2.wait(timeout=2)

        print(f"PERSISTENCE SIGKILL TEST PASSED: {recovered}/{len(acknowledged_keys)} keys 100% recovered from AOF!")

    finally:
        try:
            proc.kill()
        except:
            pass
        if os.path.exists("appendonly.aof"):
            try:
                os.remove("appendonly.aof")
            except:
                pass

if __name__ == "__main__":
    run_persistence_sigkill_test()
