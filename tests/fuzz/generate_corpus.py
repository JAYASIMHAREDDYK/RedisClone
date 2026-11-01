#!/usr/bin/env python3
import os

corpus_dir = os.path.join(os.path.dirname(__file__), "corpus")
os.makedirs(corpus_dir, exist_ok=True)

seeds = {
    "ping": b"*1\r\n$4\r\nPING\r\n",
    "set": b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
    "get": b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
    "inline_ping": b"PING\r\n",
    "inline_set": b"SET mykey myval\r\n",
    "simple_string": b"+OK\r\n",
    "error_string": b"-ERR unknown command 'FOO'\r\n",
    "integer_val": b":1000\r\n",
    "null_bulk": b"$-1\r\n",
    "empty_bulk": b"$0\r\n\r\n",
    "empty_array": b"*0\r\n",
    "null_array": b"*-1\r\n",
    "pipelined": b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n",
    "split_bulk": b"*2\r\n$4\r\nECHO\r\n$10\r\n12345",
    "nested_array": b"*2\r\n*2\r\n$4\r\nECHO\r\n$2\r\nHI\r\n*1\r\n$4\r\nPING\r\n",
    "zadd": b"*4\r\n$4\r\nZADD\r\n$4\r\nzset\r\n$3\r\n100\r\n$6\r\nmember\r\n",
    "replicaof": b"*3\r\n$9\r\nREPLICAOF\r\n$9\r\n127.0.0.1\r\n$4\r\n6379\r\n",
}

for name, content in seeds.items():
    filepath = os.path.join(corpus_dir, f"{name}.bin")
    with open(filepath, "wb") as f:
        f.write(content)

print(f"Generated {len(seeds)} corpus seeds in {corpus_dir}")
