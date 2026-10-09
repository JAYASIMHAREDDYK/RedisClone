#include "server.h"
#include "util.h"
#include <iostream>
#include <csignal>
#include <memory>

static std::unique_ptr<redis::Server> g_server;

static void handleSignal(int sig) {
    (void)sig;
    if (g_server) {
        g_server->stop();
    }
}

int main(int argc, char* argv[]) {
    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    redis::ServerConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
            auto port = redis::parseInteger(argv[++i]);
            if (port && *port > 0 && *port <= 65535) {
                config.port = static_cast<int>(*port);
            }
        } else if ((arg == "-h" || arg == "--host") && i + 1 < argc) {
            config.bind_ip = argv[++i];
        } else if ((arg == "-m" || arg == "--maxmemory") && i + 1 < argc) {
            auto mem = redis::parseInteger(argv[++i]);
            if (mem && *mem >= 0) {
                config.maxmemory = static_cast<size_t>(*mem);
            }
        } else if (arg == "--policy" && i + 1 < argc) {
            std::string pol = redis::toLower(argv[++i]);
            if (pol == "noeviction") config.eviction_policy = redis::EvictionPolicy::NoEviction;
            else if (pol == "allkeys-lru") config.eviction_policy = redis::EvictionPolicy::AllKeysLRU;
            else if (pol == "volatile-lru") config.eviction_policy = redis::EvictionPolicy::VolatileLRU;
            else if (pol == "allkeys-lfu") config.eviction_policy = redis::EvictionPolicy::AllKeysLFU;
            else if (pol == "volatile-lfu") config.eviction_policy = redis::EvictionPolicy::VolatileLFU;
        } else if (arg == "--aof" && i + 1 < argc) {
            std::string val = redis::toLower(argv[++i]);
            config.aof_enabled = (val == "yes" || val == "1" || val == "true");
        } else if (arg == "--fsync" && i + 1 < argc) {
            std::string fs = redis::toLower(argv[++i]);
            if (fs == "always") config.aof_fsync = redis::AofFsyncPolicy::Always;
            else if (fs == "everysec") config.aof_fsync = redis::AofFsyncPolicy::EverySec;
            else if (fs == "no") config.aof_fsync = redis::AofFsyncPolicy::No;
        }
    }

    g_server = std::make_unique<redis::Server>(config);

    if (!g_server->start()) {
        std::cerr << "Failed to start server on " << config.bind_ip << ":" << config.port << std::endl;
        return 1;
    }

    return 0;
}
