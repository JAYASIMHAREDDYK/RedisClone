#!/usr/bin/env bash
set -e

apt-get update -qq
apt-get install -y -qq redis-tools linux-perf git procps > /dev/null 2>&1

if [ ! -d "/tmp/FlameGraph" ]; then
    git clone --depth 1 https://github.com/brendangregg/FlameGraph.git /tmp/FlameGraph > /dev/null 2>&1
fi

make clean
make -j4

# Kill any existing server
pkill -9 -f redis-server || true
sleep 0.5

mkdir -p benchmark/results

echo "Starting redis-server on port 6379 with --aof no..."
./redis-server -p 6379 --aof no > /dev/null 2>&1 &
SERVER_PID=$!
sleep 1

echo "Profiling SET benchmark (n=500,000, c=50, P=16) with perf record..."
perf record -F 999 -g -o /tmp/perf.data -p $SERVER_PID -- redis-benchmark -h 127.0.0.1 -p 6379 -c 50 -P 16 -n 500000 -t set -q

kill -9 $SERVER_PID || true
sleep 0.5

echo "Generating Flame Graph SVG..."
perf script -i /tmp/perf.data | /tmp/FlameGraph/stackcollapse-perf.pl | /tmp/FlameGraph/flamegraph.pl --title "RedisClone SET Flame Graph" --width 1200 > benchmark/results/flamegraph.svg

echo "Flame Graph written to benchmark/results/flamegraph.svg"
echo ""
echo "=== Top 30 Symbols by CPU Overhead ==="
perf report -i /tmp/perf.data --stdio -n --no-children --max-stack=5 | head -n 35
