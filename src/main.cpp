#include "server.h"
#include "common.h"
#include <iostream>
#include <csignal>
#include <memory>

using std::string;
using std::unique_ptr;
using std::make_unique;

static unique_ptr<redis::Server> g_server;

static void handle_signal(int sig) {
    (void)sig;
    if (g_server) {
        g_server->stop();
    }
}

int main(int argc, char* argv[]) {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    redis::Config config;

    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];
        if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
            auto port = redis::parse_int(argv[++i]);
            if (port && *port > 0 && *port <= 65535) {
                config.port = static_cast<int>(*port);
            }
        } else if ((arg == "-h" || arg == "--host") && i + 1 < argc) {
            config.bind_ip = argv[++i];
        } else if ((arg == "-m" || arg == "--maxmemory") && i + 1 < argc) {
            auto mem = redis::parse_int(argv[++i]);
            if (mem && *mem >= 0) {
                config.maxmemory = static_cast<size_t>(*mem);
            }
        } else if (arg == "--policy" && i + 1 < argc) {
            string pol = redis::to_lower(argv[++i]);
            if (pol == "noeviction") config.eviction_policy = redis::EvictPolicy::NoEviction;
            else if (pol == "allkeys-lru") config.eviction_policy = redis::EvictPolicy::AllKeysLRU;
            else if (pol == "volatile-lru") config.eviction_policy = redis::EvictPolicy::VolatileLRU;
            else if (pol == "allkeys-lfu") config.eviction_policy = redis::EvictPolicy::AllKeysLFU;
            else if (pol == "volatile-lfu") config.eviction_policy = redis::EvictPolicy::VolatileLFU;
        } else if (arg == "--aof" && i + 1 < argc) {
            string val = redis::to_lower(argv[++i]);
            config.aof_enabled = (val == "yes" || val == "1" || val == "true");
        } else if (arg == "--fsync" && i + 1 < argc) {
            string fs = redis::to_lower(argv[++i]);
            if (fs == "always") config.aof_fsync = redis::FsyncPolicy::Always;
            else if (fs == "everysec") config.aof_fsync = redis::FsyncPolicy::EverySec;
            else if (fs == "no") config.aof_fsync = redis::FsyncPolicy::No;
        }
    }

    g_server = make_unique<redis::Server>(config);

    if (!g_server->start()) {
        std::cerr << "Failed to start server on " << config.bind_ip << ":" << config.port << std::endl;
        return 1;
    }

    return 0;
}
