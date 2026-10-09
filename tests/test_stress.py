import socket
import time
import subprocess
import threading
import sys
import os

def client_worker(port, thread_id, num_ops, error_list):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('127.0.0.1', port))
        f = s.makefile('rb')

        for i in range(num_ops):
            key = f"key_{thread_id}_{i}"
            val = f"val_{thread_id}_{i}"
            set_cmd = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n".encode()
            s.sendall(set_cmd)
            resp = f.readline()
            if resp != b"+OK\r\n":
                error_list.append(f"Unexpected SET response: {resp}")
                break

            get_cmd = f"*2\r\n$3\r\nGET\r\n${len(key)}\r\n{key}\r\n".encode()
            s.sendall(get_cmd)
            line1 = f.readline()
            data = f.read(len(val))
            f.read(2)
            if data.decode() != val:
                error_list.append(f"Mismatch for {key}: expected {val}, got {data.decode()}")
                break

        s.close()
    except Exception as e:
        error_list.append(str(e))

def run_stress():
    port = 6389
    server_bin = ".\\redis-server.exe" if os.name == 'nt' else "./redis-server"
    proc = subprocess.Popen([
        server_bin,
        "-p", str(port),
        "--aof", "no"
    ])
    time.sleep(0.5)

    num_threads = 20
    ops_per_thread = 500
    errors = []

    try:
        threads = []
        for tid in range(num_threads):
            t = threading.Thread(target=client_worker, args=(port, tid, ops_per_thread, errors))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        if errors:
            print(f"STRESS TEST FAILED with {len(errors)} errors:")
            for e in errors[:5]:
                print(f"  {e}")
            sys.exit(1)
        else:
            print(f"STRESS TEST PASSED: {num_threads * ops_per_thread * 2} ops completed across {num_threads} concurrent threads with 0 errors!")
    finally:
        proc.terminate()
        proc.wait(timeout=2)

if __name__ == "__main__":
    run_stress()
