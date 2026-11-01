#pragma once

#include "resp.h"
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <cstdint>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
using socket_t = SOCKET;
constexpr socket_t INVALID_SOCK_REPL = INVALID_SOCKET;
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
using socket_t = int;
constexpr socket_t INVALID_SOCK_REPL = -1;
#endif

namespace redis {

enum class NodeRole {
    Master,
    Replica
};

struct ReplicaPeer {
    socket_t fd{INVALID_SOCK_REPL};
    int64_t ack_offset{0};
    uint64_t last_ack_time{0};
    bool is_online{false};
};

class ReplicationBacklog {
public:
    explicit ReplicationBacklog(size_t max_size = 1024 * 1024);

    void feed(const std::string& raw_resp);
    bool can_partial_resync(const std::string& req_id, int64_t req_offset, const std::string& my_id) const;
    std::string get_data(int64_t from_offset) const;

    int64_t master_offset() const { return master_offset_; }
    int64_t start_offset() const { return start_offset_; }
    size_t size() const { return buffer_.size(); }

    void set_master_offset(int64_t offset) { master_offset_ = offset; }

private:
    size_t max_size_;
    std::string buffer_;
    int64_t master_offset_{0};
    int64_t start_offset_{0};
};

std::string generate_replication_id();

}
