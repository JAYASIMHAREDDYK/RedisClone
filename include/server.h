#pragma once

#include "dict.h"
#include "skiplist.h"
#include "resp.h"
#include "evict.h"
#include "expire.h"
#include "aof.h"
#include "net.h"
#include <string>
#include <vector>
#include <memory>
#include <cstdint>
#include <atomic>

namespace redis {

struct Config {
    std::string bind_ip{"0.0.0.0"};
    int port{6379};
    size_t maxmemory{0};
    EvictPolicy eviction_policy{EvictPolicy::AllKeysLFU};
    bool aof_enabled{true};
    std::string aof_filename{"appendonly.aof"};
    FsyncPolicy aof_fsync{FsyncPolicy::EverySec};
    uint32_t client_timeout{0};
};

struct Stats {
    uint64_t start_time_sec{0};
    std::atomic<uint64_t> total_commands_processed{0};
    std::atomic<uint64_t> total_connections_received{0};
    std::atomic<size_t> used_memory_bytes{0};
};

class Server {
public:
    explicit Server(Config config = Config{});
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    bool start();
    void stop();

    void execute(Client& client, const std::vector<std::string>& args);

    size_t memory_used() const { return stats_.used_memory_bytes.load(); }
    size_t key_count() const { return dict_.size(); }

    Dict& dict() { return dict_; }

private:
    Config config_;
    Stats stats_;
    bool running_{false};

    Dict dict_;
    Evictor eviction_;
    Expirer expire_;
    Aof aof_;
    EventLoop loop_;

    void setup_handlers();
    void cron();

    void evict_if_needed();
    bool del_key(const std::string& key);

    void cmd_ping(Client& client, const std::vector<std::string>& args);
    void cmd_echo(Client& client, const std::vector<std::string>& args);
    void cmd_set(Client& client, const std::vector<std::string>& args);
    void cmd_get(Client& client, const std::vector<std::string>& args);
    void cmd_del(Client& client, const std::vector<std::string>& args);
    void cmd_exists(Client& client, const std::vector<std::string>& args);
    void cmd_expire(Client& client, const std::vector<std::string>& args);
    void cmd_ttl(Client& client, const std::vector<std::string>& args);
    void cmd_zadd(Client& client, const std::vector<std::string>& args);
    void cmd_zrange(Client& client, const std::vector<std::string>& args);
    void cmd_zrangebyscore(Client& client, const std::vector<std::string>& args);
    void cmd_zscore(Client& client, const std::vector<std::string>& args);
    void cmd_zcard(Client& client, const std::vector<std::string>& args);
    void cmd_bgrewriteaof(Client& client, const std::vector<std::string>& args);
    void cmd_info(Client& client, const std::vector<std::string>& args);
    void cmd_command(Client& client, const std::vector<std::string>& args);

    size_t estimate_size(const std::string& key, const Value& value);
};

}
