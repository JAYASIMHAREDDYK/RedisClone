#pragma once

#include "dict.h"
#include "skiplist.h"
#include "resp.h"
#include "evict.h"
#include "expire.h"
#include "aof.h"
#include "net.h"
#include "replication.h"
#include <string>
#include <vector>
#include <memory>
#include <cstdint>
#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

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
    std::string replicaof_host{""};
    int replicaof_port{0};
    int repl_delay_ms{0};
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
    void execute_internal(Client& client, const std::vector<std::string>& args, bool from_master);

    size_t memory_used() const { return stats_.used_memory_bytes.load(); }
    size_t key_count() const { return dict_.size(); }

    Dict& dict() { return dict_; }
    NodeRole role() const { return role_; }

    std::string dump_snapshot();

private:
    Config config_;
    Stats stats_;
    bool running_{false};
    std::mutex server_mutex_;

    Dict dict_;
    Evictor eviction_;
    Expirer expire_;
    Aof aof_;
    EventLoop loop_;

    // Replication Subsystem
    NodeRole role_{NodeRole::Master};
    std::string repl_id_;
    ReplicationBacklog backlog_;
    std::unordered_map<socket_t, ReplicaPeer> replicas_;
    bool has_active_replication_{false};

    // Replica state
    std::string master_host_;
    int master_port_{0};
    std::atomic<bool> stop_replica_thread_{false};
    std::atomic<socket_t> replica_sock_{INVALID_SOCK_REPL};
    std::thread replica_thread_;
    std::string cached_master_repl_id_;
    int64_t replica_applied_offset_{0};
    std::atomic<uint64_t> last_master_io_time_{0};

    void setup_handlers();
    void cron();

    void evict_if_needed();
    bool del_key(const std::string& key);

    void propagate_to_replicas(const std::vector<std::string>& args);

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

    // Transaction Commands
    void cmd_multi(Client& client, const std::vector<std::string>& args);
    void cmd_exec(Client& client, const std::vector<std::string>& args);
    void cmd_discard(Client& client, const std::vector<std::string>& args);
    void cmd_watch(Client& client, const std::vector<std::string>& args);
    void cmd_unwatch(Client& client, const std::vector<std::string>& args);

    void dispatch_command(Client& client, const std::vector<std::string>& args, bool from_master);
    void touch_watched_key(const std::string& key);
    bool is_watch_dirty(socket_t fd) const;
    void unwatch_client(socket_t fd);

    // Replication Commands
    void cmd_replicaof(Client& client, const std::vector<std::string>& args);
    void cmd_wait(Client& client, const std::vector<std::string>& args);
    void cmd_psync(Client& client, const std::vector<std::string>& args);
    void cmd_replconf(Client& client, const std::vector<std::string>& args);

    void start_replication(const std::string& host, int port);
    void stop_replication();
    void replica_sync_worker();

    size_t estimate_size(const std::string& key, const std::string& val);
    size_t estimate_size(const std::string& key, const Value& value);

    // Transaction WATCH state
    std::unordered_map<std::string, std::unordered_set<socket_t>> watched_keys_;
    std::unordered_map<socket_t, std::unordered_set<std::string>> client_watched_keys_;
    std::unordered_map<socket_t, bool> client_watch_dirty_;
};

}
