import socket
import time
import subprocess
import sys
import os

class RedisClient:
    def __init__(self, host='127.0.0.1', port=6399):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((self.host, self.port))
        self.f = self.sock.makefile('rb')

    def close(self):
        try:
            self.f.close()
            self.sock.close()
        except Exception:
            pass

    def send_command(self, *args):
        buf = [f"*{len(args)}\r\n".encode()]
        for a in args:
            s = str(a).encode()
            buf.append(f"${len(s)}\r\n".encode())
            buf.append(s)
            buf.append(b"\r\n")
        self.sock.sendall(b"".join(buf))
        return self.read_response()

    def read_response(self):
        line = self.f.readline()
        if not line:
            return None
        prefix = line[0:1]
        payload = line[1:-2]

        if prefix == b'+':
            return payload.decode()
        elif prefix == b'-':
            return Exception(payload.decode())
        elif prefix == b':':
            return int(payload)
        elif prefix == b'$':
            length = int(payload)
            if length == -1:
                return None
            data = self.f.read(length)
            self.f.read(2)
            return data.decode()
        elif prefix == b'*':
            count = int(payload)
            if count == -1:
                return None
            items = []
            for _ in range(count):
                items.append(self.read_response())
            return items
        else:
            raise ValueError(f"Unknown RESP byte: {prefix}")

def wait_for_server(port, timeout=5.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            c = RedisClient(port=port)
            res = c.send_command("PING")
            c.close()
            if res == "PONG":
                return True
        except Exception:
            time.sleep(0.05)
    return False

def run_tests():
    port = 6399
    server_bin = ".\\redis-server.exe" if os.name == 'nt' else "./redis-server"
    proc = subprocess.Popen([
        server_bin,
        "-p", str(port),
        "--aof", "no"
    ])

    try:
        assert wait_for_server(port), "Server failed to start in time"
        c1 = RedisClient(port=port)
        c2 = RedisClient(port=port)

        print("=== Running Transaction & Optimistic Locking Test Suite ===")

        # 1. State errors outside MULTI
        res = c1.send_command("EXEC")
        assert isinstance(res, Exception) and "without MULTI" in str(res)
        res = c1.send_command("DISCARD")
        assert isinstance(res, Exception) and "without MULTI" in str(res)
        print("[PASS] EXEC / DISCARD without MULTI rejected")

        # 2. Empty MULTI/EXEC
        assert c1.send_command("MULTI") == "OK"
        res = c1.send_command("EXEC")
        assert res == []
        print("[PASS] Empty MULTI / EXEC returns empty array")

        # 3. Basic Transaction execution
        assert c1.send_command("MULTI") == "OK"
        assert c1.send_command("SET", "k1", "v1") == "QUEUED"
        assert c1.send_command("SET", "k2", "v2") == "QUEUED"
        assert c1.send_command("GET", "k1") == "QUEUED"
        assert c1.send_command("PING") == "QUEUED"

        # Ensure c2 sees nothing before EXEC
        assert c2.send_command("GET", "k1") is None

        res = c1.send_command("EXEC")
        assert res == ["OK", "OK", "v1", "PONG"]
        assert c2.send_command("GET", "k1") == "v1"
        assert c2.send_command("GET", "k2") == "v2"
        print("[PASS] Multi-command atomic transaction executed and committed")

        # 4. DISCARD aborts queued writes
        assert c1.send_command("MULTI") == "OK"
        assert c1.send_command("SET", "k1", "discarded_val") == "QUEUED"
        assert c1.send_command("DISCARD") == "OK"
        assert c2.send_command("GET", "k1") == "v1"
        print("[PASS] DISCARD aborts and preserves previous state")

        # 5. Nested MULTI causes EXECABORT
        assert c1.send_command("MULTI") == "OK"
        res = c1.send_command("MULTI")
        assert isinstance(res, Exception) and "not be nested" in str(res)
        res = c1.send_command("EXEC")
        assert isinstance(res, Exception) and "EXECABORT" in str(res)
        print("[PASS] Nested MULTI triggers EXECABORT")

        # 6. Syntax / arity error during queuing causes EXECABORT
        assert c1.send_command("MULTI") == "OK"
        res = c1.send_command("SET")  # wrong arity
        assert isinstance(res, Exception) and "wrong number of arguments" in str(res)
        assert c1.send_command("SET", "aborted_key", "aborted_val") == "QUEUED"
        res = c1.send_command("EXEC")
        assert isinstance(res, Exception) and "EXECABORT" in str(res)
        assert c2.send_command("GET", "aborted_key") is None
        print("[PASS] Queue-time arity error triggers EXECABORT and rolls back")

        # 7. Runtime error does not abort subsequent commands
        c1.send_command("SET", "str_target", "a_string")
        assert c1.send_command("MULTI") == "OK"
        assert c1.send_command("SET", "tx_item1", "first") == "QUEUED"
        assert c1.send_command("ZADD", "str_target", "1.0", "elem") == "QUEUED"  # runtime WRONGTYPE
        assert c1.send_command("SET", "tx_item2", "second") == "QUEUED"

        res = c1.send_command("EXEC")
        assert len(res) == 3
        assert res[0] == "OK"
        assert isinstance(res[1], Exception) and "WRONGTYPE" in str(res[1])
        assert res[2] == "OK"
        assert c2.send_command("GET", "tx_item1") == "first"
        assert c2.send_command("GET", "tx_item2") == "second"
        print("[PASS] Runtime error within transaction does not abort other commands")

        # 8. WATCH optimistic locking with concurrent client modification
        c1.send_command("SET", "balance", "100")
        assert c1.send_command("WATCH", "balance") == "OK"

        # c2 modifies the watched key
        assert c2.send_command("SET", "balance", "200") == "OK"

        # c1 attempts transaction
        assert c1.send_command("MULTI") == "OK"
        assert c1.send_command("SET", "balance", "150") == "QUEUED"
        res = c1.send_command("EXEC")
        assert res is None, f"Expected None on WATCH conflict, got {res}"
        assert c2.send_command("GET", "balance") == "200"
        print("[PASS] WATCH detects concurrent modification and aborts transaction (nil reply)")

        # 9. UNWATCH clears watched keys
        c1.send_command("WATCH", "balance")
        assert c1.send_command("UNWATCH") == "OK"
        c2.send_command("SET", "balance", "300")

        assert c1.send_command("MULTI") == "OK"
        assert c1.send_command("SET", "balance", "350") == "QUEUED"
        res = c1.send_command("EXEC")
        assert res == ["OK"]
        assert c2.send_command("GET", "balance") == "350"
        print("[PASS] UNWATCH allows transaction to succeed despite key modification")

        # 10. WATCH inside MULTI rejected
        assert c1.send_command("MULTI") == "OK"
        res = c1.send_command("WATCH", "balance")
        assert isinstance(res, Exception) and "inside MULTI" in str(res)
        res = c1.send_command("EXEC")
        assert isinstance(res, Exception) and "EXECABORT" in str(res)
        print("[PASS] WATCH inside MULTI rejected and causes EXECABORT")

        c1.close()
        c2.close()
        print("\nALL TRANSACTION TESTS PASSED SUCCESSFULLY!")

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()

if __name__ == "__main__":
    run_tests()
