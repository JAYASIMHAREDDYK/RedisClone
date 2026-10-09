#pragma once

#include "dict.h"
#include "skiplist.h"
#include "resp.h"
#include "eviction.h"
#include "expire.h"
#include "aof.h"
#include "net.h"
#include <string>
#include <vector>
#include <memory>
#include <cstdint>
#include <atomic>

namespace redis {

struct ServerConfig {
    std::string bind_ip{"0.0.0.0"};
    int port{6379};
    size_t maxmemory{0};
    EvictionPolicy eviction_policy{EvictionPolicy::AllKeysLFU};
    bool aof_enabled{true};
    std::string aof_filename{"appendonly.aof"};
    AofFsyncPolicy aof_fsync{AofFsyncPolicy::EverySec};
    uint32_t client_timeout{0};
};

struct ServerStats {
    uint64_t start_time_sec{0};
    std::atomic<uint64_t> total_commands_processed{0};
    std::atomic<uint64_t> total_connections_received{0};
    std::atomic<size_t> used_memory_bytes{0};
};

class Server {
public:
    explicit Server(ServerConfig config = ServerConfig{});
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    bool start();
    void stop();

    void executeCommand(ClientConnection& client, const std::vector<std::string>& args);

    size_t getMemoryUsage() const { return stats_.used_memory_bytes.load(); }
    size_t getKeyCount() const { return dict_.size(); }

    ProgressiveDict& getDict() { return dict_; }

private:
    ServerConfig config_;
    ServerStats stats_;
    bool running_{false};

    ProgressiveDict dict_;
    EvictionEngine eviction_;
    ExpirationManager expire_;
    AofEngine aof_;
    EventReactor reactor_;

    void registerHandlers();
    void handlePeriodicTasks();

    void checkEviction();
    bool deleteKeyInternal(const std::string& key);

    void cmdPing(ClientConnection& client, const std::vector<std::string>& args);
    void cmdEcho(ClientConnection& client, const std::vector<std::string>& args);
    void cmdSet(ClientConnection& client, const std::vector<std::string>& args);
    void cmdGet(ClientConnection& client, const std::vector<std::string>& args);
    void cmdDel(ClientConnection& client, const std::vector<std::string>& args);
    void cmdExists(ClientConnection& client, const std::vector<std::string>& args);
    void cmdExpire(ClientConnection& client, const std::vector<std::string>& args);
    void cmdTtl(ClientConnection& client, const std::vector<std::string>& args);
    void cmdZAdd(ClientConnection& client, const std::vector<std::string>& args);
    void cmdZRange(ClientConnection& client, const std::vector<std::string>& args);
    void cmdZRangeByScore(ClientConnection& client, const std::vector<std::string>& args);
    void cmdZScore(ClientConnection& client, const std::vector<std::string>& args);
    void cmdZCard(ClientConnection& client, const std::vector<std::string>& args);
    void cmdBgRewriteAof(ClientConnection& client, const std::vector<std::string>& args);
    void cmdInfo(ClientConnection& client, const std::vector<std::string>& args);
    void cmdCommand(ClientConnection& client, const std::vector<std::string>& args);

    size_t estimateEntrySize(const std::string& key, const DictValue& value);
};

}
