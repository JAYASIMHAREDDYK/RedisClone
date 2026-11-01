FROM gcc:12-bookworm AS builder

WORKDIR /app
COPY . .
RUN make clean && make -j$(nproc) CXXFLAGS="-std=c++17 -Wall -Wextra -O2 -Iinclude"

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /app/redis-server /usr/local/bin/redis-server

EXPOSE 6379

ENTRYPOINT ["redis-server"]
CMD ["-p", "6379", "--aof", "yes", "--fsync", "everysec"]
