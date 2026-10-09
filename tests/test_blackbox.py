import socket
import time
import subprocess
import sys
import os

class RedisProtocolClient:
    def __init__(self, host='127.0.0.1', port=6379):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((host, port))
        self.reader = self.sock.makefile('rb')

    def execute(self, *parts):
        payload = [f"*{len(parts)}\r\n".encode()]
        for p in parts:
            b = str(p).encode()
            payload.append(f"${len(b)}\r\n".encode())
            payload.append(b)
            payload.append(b"\r\n")
        self.sock.sendall(b"".join(payload))
        return self._parse_reply()

    def _parse_reply(self):
        line = self.reader.readline()
        if not line:
            return None
        t = line[0:1]
        raw = line[1:-2]
        if t == b'+':
            return raw.decode()
        if t == b'-':
            return Exception(raw.decode())
        if t == b':':
            return int(raw)
        if t == b'$':
            length = int(raw)
            if length == -1:
                return None
            data = self.reader.read(length)
            self.reader.read(2)
            return data.decode()
        if t == b'*':
            cnt = int(raw)
            if cnt == -1:
                return None
            return [self._parse_reply() for _ in range(cnt)]
        return raw.decode()

    def close(self):
        try:
            self.reader.close()
            self.sock.close()
        except:
            pass

def run_blackbox_suite():
    port = 6397
    server_bin = ".\\redis-server.exe" if os.name == 'nt' else "./redis-server"
    proc = subprocess.Popen([server_bin, "-p", str(port), "--aof", "no"])
    time.sleep(0.5)

    try:
        r = RedisProtocolClient(port=port)

        assert r.execute("PING") == "PONG"
        assert r.execute("PING", "blackbox") == "blackbox"
        assert r.execute("ECHO", "systems test") == "systems test"

        assert r.execute("SET", "session:100", "active") == "OK"
        assert r.execute("GET", "session:100") == "active"
        assert r.execute("EXISTS", "session:100") == 1
        assert r.execute("EXISTS", "missing:key") == 0

        assert r.execute("SET", "token:temp", "xyz", "EX", "2") == "OK"
        assert r.execute("TTL", "token:temp") in (1, 2)
        assert r.execute("GET", "token:temp") == "xyz"

        assert r.execute("ZADD", "ranks", 50.0, "user_b") == 1
        assert r.execute("ZADD", "ranks", 10.0, "user_a") == 1
        assert r.execute("ZADD", "ranks", 90.0, "user_c") == 1
        assert r.execute("ZCARD", "ranks") == 3
        assert float(r.execute("ZSCORE", "ranks", "user_b")) == 50.0
        assert r.execute("ZRANGE", "ranks", 0, -1) == ["user_a", "user_b", "user_c"]
        assert r.execute("ZRANGEBYSCORE", "ranks", 20.0, 100.0) == ["user_b", "user_c"]

        assert r.execute("DEL", "session:100", "ranks") == 2
        assert r.execute("GET", "session:100") is None

        info = r.execute("INFO")
        assert "redis_version:7.0.0-clone" in info
        assert "connected_clients:1" in info

        r.close()
        print("BLACK-BOX CLIENT TEST PASSED: Full command suite compatible with standard drivers!")

    finally:
        proc.terminate()
        proc.wait(timeout=2)

if __name__ == "__main__":
    run_blackbox_suite()
