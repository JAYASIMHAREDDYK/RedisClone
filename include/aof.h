#pragma once

#include "dict.h"
#include <string>
#include <vector>
#include <functional>
#include <atomic>
#include <thread>
#include <mutex>
#include <cstdint>

namespace redis {

enum class AofFsyncPolicy {
    Always,
    EverySec,
    No
};

class AofEngine {
public:
    using CommandCallback = std::function<void(const std::vector<std::string>&)>;

    explicit AofEngine(std::string filename = "appendonly.aof", 
                       AofFsyncPolicy policy = AofFsyncPolicy::EverySec);
    ~AofEngine();

    AofEngine(const AofEngine&) = delete;
    AofEngine& operator=(const AofEngine&) = delete;

    bool open();
    void close();

    void appendCommand(const std::vector<std::string>& args);

    bool startBackgroundRewrite(ProgressiveDict& dict);
    void checkBackgroundRewriteStatus();

    bool isRewriteInProgress() const { return rewrite_in_progress_; }

    bool loadAof(const CommandCallback& callback);

    void setFsyncPolicy(AofFsyncPolicy policy) { policy_ = policy; }
    AofFsyncPolicy getFsyncPolicy() const { return policy_; }

    size_t getFileSize() const;

private:
    std::string filename_;
    std::string temp_filename_{"temp-rewrite.aof"};
    AofFsyncPolicy policy_;
    int fd_{-1};

    std::atomic<bool> rewrite_in_progress_{false};
    int child_pid_{-1};
    std::string rewrite_buffer_;

    std::atomic<bool> stop_fsync_thread_{false};
    std::thread fsync_thread_;
    std::atomic<bool> needs_fsync_{false};

    void fsyncWorkerLoop();
    void performFsync();
    void writeToFile(int target_fd, const std::string& data);
    void writeDumpToFile(int target_fd, ProgressiveDict& dict);
    void finishBackgroundRewrite();
};

}
