#include "aof.h"
#include "resp.h"
#include "util.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <chrono>
#include <iostream>
#include <cstdio>
#include <cstring>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
static inline int osOpen(const char* path, int flags, int mode = 0) {
    return _open(path, flags, mode);
}
static inline int osClose(int fd) {
    return _close(fd);
}
static inline int osWrite(int fd, const void* buf, unsigned int count) {
    return _write(fd, buf, count);
}
static inline int osRead(int fd, void* buf, unsigned int count) {
    return _read(fd, buf, count);
}
static inline int osFsync(int fd) {
    return _commit(fd);
}
#else
#include <unistd.h>
#include <sys/wait.h>
static inline int osOpen(const char* path, int flags, int mode = 0644) {
    return ::open(path, flags, mode);
}
static inline int osClose(int fd) {
    return ::close(fd);
}
static inline int osWrite(int fd, const void* buf, size_t count) {
    return ::write(fd, buf, count);
}
static inline int osRead(int fd, void* buf, size_t count) {
    return ::read(fd, buf, count);
}
static inline int osFsync(int fd) {
    return fdatasync(fd);
}
#endif

namespace redis {

AofEngine::AofEngine(std::string filename, AofFsyncPolicy policy)
    : filename_(std::move(filename)), policy_(policy) {
    temp_filename_ = filename_ + ".temp";
}

AofEngine::~AofEngine() {
    close();
}

bool AofEngine::open() {
    if (fd_ != -1) return true;

#ifdef _WIN32
    fd_ = osOpen(filename_.c_str(), _O_CREAT | _O_RDWR | _O_APPEND | _O_BINARY, _S_IREAD | _S_IWRITE);
#else
    fd_ = osOpen(filename_.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
#endif

    if (fd_ < 0) {
        return false;
    }

    if (policy_ == AofFsyncPolicy::EverySec) {
        stop_fsync_thread_ = false;
        fsync_thread_ = std::thread(&AofEngine::fsyncWorkerLoop, this);
    }

    return true;
}

void AofEngine::close() {
    if (stop_fsync_thread_ == false) {
        stop_fsync_thread_ = true;
        if (fsync_thread_.joinable()) {
            fsync_thread_.join();
        }
    }

    if (fd_ >= 0) {
        performFsync();
        osClose(fd_);
        fd_ = -1;
    }
}

void AofEngine::performFsync() {
    if (fd_ >= 0) {
        osFsync(fd_);
    }
}

void AofEngine::fsyncWorkerLoop() {
    while (!stop_fsync_thread_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        if (needs_fsync_.exchange(false)) {
            performFsync();
        }
    }
}

void AofEngine::writeToFile(int target_fd, const std::string& data) {
    if (target_fd < 0 || data.empty()) return;

    size_t written = 0;
    while (written < data.size()) {
        int n = osWrite(target_fd, data.data() + written, static_cast<unsigned int>(data.size() - written));
        if (n <= 0) {
            break;
        }
        written += n;
    }
}

void AofEngine::appendCommand(const std::vector<std::string>& args) {
    if (args.empty()) return;

    std::string payload = RespEncoder::array(args);

    if (fd_ >= 0) {
        writeToFile(fd_, payload);

        if (policy_ == AofFsyncPolicy::Always) {
            performFsync();
        } else if (policy_ == AofFsyncPolicy::EverySec) {
            needs_fsync_ = true;
        }
    }

    if (rewrite_in_progress_) {
        rewrite_buffer_.append(payload);
    }
}

void AofEngine::writeDumpToFile(int target_fd, ProgressiveDict& dict) {
    dict.forEach([this, target_fd](DictEntry* entry) {
        if (!entry) return;

        if (entry->isString()) {
            std::vector<std::string> set_cmd{"SET", entry->key, entry->getString()};
            if (entry->expire_at_ms > 0) {
                uint64_t now = getUnixTimeMs();
                if (entry->expire_at_ms > now) {
                    uint64_t ttl_sec = (entry->expire_at_ms - now + 999) / 1000;
                    set_cmd.push_back("EX");
                    set_cmd.push_back(std::to_string(ttl_sec));
                } else {
                    return;
                }
            }
            std::string payload = RespEncoder::array(set_cmd);
            writeToFile(target_fd, payload);
        } else if (entry->isZSet()) {
            auto zset = entry->getZSet();
            if (zset && zset->size() > 0) {
                for (const auto& [member, score] : zset->dict) {
                    std::vector<std::string> zadd_cmd{"ZADD", entry->key, std::to_string(score), member};
                    std::string payload = RespEncoder::array(zadd_cmd);
                    writeToFile(target_fd, payload);
                }
            }
        }
    });
}

bool AofEngine::startBackgroundRewrite(ProgressiveDict& dict) {
    if (rewrite_in_progress_) {
        return false;
    }

    rewrite_buffer_.clear();
    rewrite_in_progress_ = true;

#ifndef _WIN32
    pid_t pid = fork();
    if (pid < 0) {
        rewrite_in_progress_ = false;
        return false;
    }

    if (pid == 0) {
        int temp_fd = osOpen(temp_filename_.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (temp_fd < 0) {
            _exit(1);
        }

        writeDumpToFile(temp_fd, dict);
        osFsync(temp_fd);
        osClose(temp_fd);
        _exit(0);
    } else {
        child_pid_ = pid;
        return true;
    }
#else
    std::thread([this, &dict]() {
        int temp_fd = osOpen(temp_filename_.c_str(), _O_CREAT | _O_WRONLY | _O_TRUNC | _O_BINARY, _S_IREAD | _S_IWRITE);
        if (temp_fd >= 0) {
            writeDumpToFile(temp_fd, dict);
            osFsync(temp_fd);
            osClose(temp_fd);
        }
        finishBackgroundRewrite();
    }).detach();

    return true;
#endif
}

void AofEngine::checkBackgroundRewriteStatus() {
    if (!rewrite_in_progress_) return;

#ifndef _WIN32
    if (child_pid_ <= 0) return;

    int status = 0;
    pid_t result = waitpid(child_pid_, &status, WNOHANG);
    if (result == child_pid_) {
        child_pid_ = -1;
        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
            finishBackgroundRewrite();
        } else {
            rewrite_in_progress_ = false;
            rewrite_buffer_.clear();
            std::remove(temp_filename_.c_str());
        }
    }
#endif
}

void AofEngine::finishBackgroundRewrite() {
    if (!rewrite_buffer_.empty()) {
#ifdef _WIN32
        int temp_fd = osOpen(temp_filename_.c_str(), _O_WRONLY | _O_APPEND | _O_BINARY, _S_IREAD | _S_IWRITE);
#else
        int temp_fd = osOpen(temp_filename_.c_str(), O_WRONLY | O_APPEND, 0644);
#endif
        if (temp_fd >= 0) {
            writeToFile(temp_fd, rewrite_buffer_);
            performFsync();
            osClose(temp_fd);
        }
    }

    rewrite_buffer_.clear();

    if (fd_ >= 0) {
        osClose(fd_);
        fd_ = -1;
    }

#ifdef _WIN32
    std::remove(filename_.c_str());
    std::rename(temp_filename_.c_str(), filename_.c_str());
#else
    std::rename(temp_filename_.c_str(), filename_.c_str());
#endif

    rewrite_in_progress_ = false;
    open();
}

bool AofEngine::loadAof(const CommandCallback& callback) {
#ifdef _WIN32
    int read_fd = osOpen(filename_.c_str(), _O_RDONLY | _O_BINARY, _S_IREAD);
#else
    int read_fd = osOpen(filename_.c_str(), O_RDONLY);
#endif

    if (read_fd < 0) {
        return false;
    }

    RespParser parser;
    char buf[8192];
    int bytes_read = 0;

    while ((bytes_read = osRead(read_fd, buf, sizeof(buf))) > 0) {
        parser.feed(buf, bytes_read);
        std::vector<std::string> args;
        while (parser.nextCommand(args)) {
            callback(args);
        }
    }

    osClose(read_fd);
    return true;
}

size_t AofEngine::getFileSize() const {
    struct stat st;
    if (stat(filename_.c_str(), &st) == 0) {
        return static_cast<size_t>(st.st_size);
    }
    return 0;
}

}
