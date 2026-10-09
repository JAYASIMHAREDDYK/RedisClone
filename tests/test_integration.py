import socket
import time
import subprocess
import sys
import os

class RedisClient:
    def __init__(self, host='127.0.0.1', port=6379):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self.f = self.sock.makefile('rb')

    def close(self):
        try:
            self.f.close()
            self.sock.close()
        except:
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

def run_tests():
    port = 6388
    aof_file = f"test_append_{port}.aof"
    if os.path.exists(aof_file):
        os.remove(aof_file)

    proc = subprocess.Popen([
        ".\\redis-server.exe",
        "-p", str(port),
        "--aof", "yes"
    ])

    time.sleep(0.5)

    try:
        client = RedisClient(port=port)

        res = client.send_command("PING")
        assert res == "PONG", f"Expected PONG, got {res}"

        res = client.send_command("PING", "systems")
        assert res == "systems", f"Expected 'systems', got {res}"

        res = client.send_command("ECHO", "hello")
        assert res == "hello", f"Expected 'hello', got {res}"

        res = client.send_command("SET", "user:1", "alice")
        assert res == "OK"

        res = client.send_command("GET", "user:1")
        assert res == "alice"

        res = client.send_command("EXISTS", "user:1", "user:2")
        assert res == 1

        res = client.send_command("DEL", "user:1")
        assert res == 1

        res = client.send_command("GET", "user:1")
        assert res is None

        res = client.send_command("SET", "temp", "val", "EX", "1")
        assert res == "OK"
        res = client.send_command("TTL", "temp")
        assert res >= 1
        time.sleep(1.2)
        res = client.send_command("GET", "temp")
        assert res is None
        res = client.send_command("TTL", "temp")
        assert res == -2

        res = client.send_command("ZADD", "leaderboard", 100.0, "player1")
        assert res == 1
        res = client.send_command("ZADD", "leaderboard", 200.0, "player2")
        assert res == 1
        res = client.send_command("ZADD", "leaderboard", 150.0, "player3")
        assert res == 1
        res = client.send_command("ZADD", "leaderboard", 250.0, "player1")
        assert res == 0

        res = client.send_command("ZCARD", "leaderboard")
        assert res == 3

        res = client.send_command("ZSCORE", "leaderboard", "player1")
        assert float(res) == 250.0

        res = client.send_command("ZRANGE", "leaderboard", 0, -1)
        assert res == ["player3", "player2", "player1"], f"Got {res}"

        res = client.send_command("ZRANGEBYSCORE", "leaderboard", 140.0, 210.0)
        assert res == ["player3", "player2"]

        res = client.send_command("INFO")
        assert "redis_version" in res
        assert "connected_clients:1" in res

        client.send_command("SET", "persist1", "val1")
        client.send_command("SET", "persist2", "val2")
        client.close()

        proc.terminate()
        proc.wait(timeout=3)

        time.sleep(0.5)
        proc2 = subprocess.Popen([
            ".\\redis-server.exe",
            "-p", str(port),
            "--aof", "yes"
        ])
        time.sleep(0.5)

        client2 = RedisClient(port=port)
        r1 = client2.send_command("GET", "persist1")
        r2 = client2.send_command("GET", "persist2")
        assert r1 == "val1", f"AOF recovery failed for persist1: {r1}"
        assert r2 == "val2", f"AOF recovery failed for persist2: {r2}"
        client2.close()

        proc2.terminate()
        proc2.wait(timeout=3)

        print("ALL INTEGRATION TESTS PASSED SUCCESSFULLY!")

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
    run_tests()
