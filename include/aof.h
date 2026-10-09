#pragma once

#include "dict.h"
#include <string>
#include <vector>
#include <functional>
#include <atomic>
#include <thread>
#include <cstdint>

namespace redis {

enum class FsyncPolicy {
    Always,
    EverySec,
    No
};

class Aof {
public:
    using CommandCallback = std::function<void(const std::vector<std::string>&)>;

    explicit Aof(std::string filename = "appendonly.aof", 
                 FsyncPolicy policy = FsyncPolicy::EverySec);
    ~Aof();

    Aof(const Aof&) = delete;
    Aof& operator=(const Aof&) = delete;

    bool open();
    void close();

    void append(const std::vector<std::string>& args);
    bool rewrite_bg(Dict& dict);
    void check_rewrite();
    bool is_rewriting() const { return rewriting_; }

    bool load(const CommandCallback& callback);

    void set_policy(FsyncPolicy policy) { policy_ = policy; }
    FsyncPolicy policy() const { return policy_; }

    size_t size() const;

private:
    std::string filename_;
    std::string temp_filename_{"temp-rewrite.aof"};
    FsyncPolicy policy_;
    int fd_{-1};

    std::atomic<bool> rewriting_{false};
    std::atomic<bool> rewrite_done_{false};
    int child_pid_{-1};
    std::string rewrite_buf_;

    std::atomic<bool> stop_fsync_thread_{false};
    std::thread fsync_thread_;
    std::atomic<bool> needs_fsync_{false};

    void fsync_worker();
    void sync();
    void write_raw(int target_fd, const std::string& data);
    void write_dump(int target_fd, Dict& dict);
    void finish_rewrite();
};

}
