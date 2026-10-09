#include "aof.h"
#include "resp.h"
#include "common.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <chrono>
#include <cstdio>
#include <cstring>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
static inline int os_open(const char* path, int flags, int mode = 0) {
    return _open(path, flags, mode);
}
static inline int os_close(int fd) {
    return _close(fd);
}
static inline int os_write(int fd, const void* buf, unsigned int count) {
    return _write(fd, buf, count);
}
static inline int os_read(int fd, void* buf, unsigned int count) {
    return _read(fd, buf, count);
}
static inline int os_fsync(int fd) {
    return _commit(fd);
}
#else
#include <unistd.h>
#include <sys/wait.h>
static inline int os_open(const char* path, int flags, int mode = 0644) {
    return ::open(path, flags, mode);
}
static inline int os_close(int fd) {
    return ::close(fd);
}
static inline int os_write(int fd, const void* buf, size_t count) {
    return ::write(fd, buf, count);
}
static inline int os_read(int fd, void* buf, size_t count) {
    return ::read(fd, buf, count);
}
static inline int os_fsync(int fd) {
    return fdatasync(fd);
}
#endif

using std::string;
using std::vector;

namespace redis {

Aof::Aof(string filename, FsyncPolicy policy)
    : filename_(std::move(filename)), policy_(policy) {
    temp_filename_ = filename_ + ".temp";
}

Aof::~Aof() {
    close();
}

bool Aof::open() {
    if (fd_ != -1) return true;

#ifdef _WIN32
    fd_ = os_open(filename_.c_str(), _O_CREAT | _O_RDWR | _O_APPEND | _O_BINARY, _S_IREAD | _S_IWRITE);
#else
    fd_ = os_open(filename_.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
#endif

    if (fd_ < 0) {
        return false;
    }

    if (policy_ == FsyncPolicy::EverySec) {
        stop_fsync_thread_ = false;
        fsync_thread_ = std::thread(&Aof::fsync_worker, this);
    }

    return true;
}

void Aof::close() {
    if (stop_fsync_thread_ == false) {
        stop_fsync_thread_ = true;
        if (fsync_thread_.joinable()) {
            fsync_thread_.join();
        }
    }

    if (fd_ >= 0) {
        sync();
        os_close(fd_);
        fd_ = -1;
    }
}

void Aof::sync() {
    if (fd_ >= 0) {
        os_fsync(fd_);
    }
}

void Aof::fsync_worker() {
    while (!stop_fsync_thread_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        if (needs_fsync_.exchange(false)) {
            sync();
        }
    }
}

void Aof::write_raw(int target_fd, const string& data) {
    if (target_fd < 0 || data.empty()) return;

    size_t written = 0;
    while (written < data.size()) {
        int n = os_write(target_fd, data.data() + written, static_cast<unsigned int>(data.size() - written));
        if (n <= 0) break;
        written += n;
    }
}

void Aof::append(const vector<string>& args) {
    if (args.empty()) return;

    string payload = RespWriter::array(args);

    if (fd_ >= 0) {
        write_raw(fd_, payload);

        if (policy_ == FsyncPolicy::Always) {
            sync();
        } else if (policy_ == FsyncPolicy::EverySec) {
            needs_fsync_ = true;
        }
    }

    if (rewriting_) {
        rewrite_buf_.append(payload);
    }
}

void Aof::write_dump(int target_fd, Dict& dict) {
    dict.scan([this, target_fd](Entry* entry) {
        if (!entry) return;

        if (entry->is_str()) {
            vector<string> set_cmd{"SET", entry->key, entry->str()};
            if (entry->expire_at_ms > 0) {
                uint64_t now = unix_time_ms();
                if (entry->expire_at_ms > now) {
                    uint64_t ttl = (entry->expire_at_ms - now + 999) / 1000;
                    set_cmd.push_back("EX");
                    set_cmd.push_back(std::to_string(ttl));
                } else {
                    return;
                }
            }
            write_raw(target_fd, RespWriter::array(set_cmd));
        } else if (entry->is_zset()) {
            auto zset = entry->zset();
            if (zset && zset->size() > 0) {
                for (const auto& [member, score] : zset->dict) {
                    vector<string> zadd_cmd{"ZADD", entry->key, std::to_string(score), member};
                    write_raw(target_fd, RespWriter::array(zadd_cmd));
                }
            }
        }
    });
}

bool Aof::rewrite_bg(Dict& dict) {
    if (rewriting_) {
        return false;
    }

    rewrite_buf_.clear();
    rewrite_done_ = false;
    rewriting_ = true;

#ifndef _WIN32
    pid_t pid = fork();
    if (pid < 0) {
        rewriting_ = false;
        return false;
    }

    if (pid == 0) {
        int temp_fd = os_open(temp_filename_.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (temp_fd < 0) _exit(1);

        write_dump(temp_fd, dict);
        os_fsync(temp_fd);
        os_close(temp_fd);
        _exit(0);
    } else {
        child_pid_ = pid;
        return true;
    }
#else
    // Windows fallback: dump snapshot in background thread without touching active event loop state
    std::thread([this, &dict]() {
        int temp_fd = os_open(temp_filename_.c_str(), _O_CREAT | _O_WRONLY | _O_TRUNC | _O_BINARY, _S_IREAD | _S_IWRITE);
        if (temp_fd >= 0) {
            write_dump(temp_fd, dict);
            os_fsync(temp_fd);
            os_close(temp_fd);
        }
        rewrite_done_ = true;
    }).detach();

    return true;
#endif
}

void Aof::check_rewrite() {
    if (!rewriting_) return;

#ifndef _WIN32
    if (child_pid_ <= 0) return;

    int status = 0;
    pid_t result = waitpid(child_pid_, &status, WNOHANG);
    if (result == child_pid_) {
        child_pid_ = -1;
        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
            finish_rewrite();
        } else {
            rewriting_ = false;
            rewrite_buf_.clear();
            std::remove(temp_filename_.c_str());
        }
    }
#else
    if (rewrite_done_.exchange(false)) {
        finish_rewrite();
    }
#endif
}

void Aof::finish_rewrite() {
    // Flush mutations accumulated while child was dumping snapshot
    if (!rewrite_buf_.empty()) {
#ifdef _WIN32
        int temp_fd = os_open(temp_filename_.c_str(), _O_WRONLY | _O_APPEND | _O_BINARY, _S_IREAD | _S_IWRITE);
#else
        int temp_fd = os_open(temp_filename_.c_str(), O_WRONLY | O_APPEND, 0644);
#endif
        if (temp_fd >= 0) {
            write_raw(temp_fd, rewrite_buf_);
            os_fsync(temp_fd);
            os_close(temp_fd);
        }
    }

    rewrite_buf_.clear();

    if (fd_ >= 0) {
        os_close(fd_);
        fd_ = -1;
    }

#ifdef _WIN32
    std::remove(filename_.c_str());
    std::rename(temp_filename_.c_str(), filename_.c_str());
#else
    std::rename(temp_filename_.c_str(), filename_.c_str());
#endif

    rewriting_ = false;
    open();
}

bool Aof::load(const CommandCallback& callback) {
#ifdef _WIN32
    int read_fd = os_open(filename_.c_str(), _O_RDONLY | _O_BINARY, _S_IREAD);
#else
    int read_fd = os_open(filename_.c_str(), O_RDONLY);
#endif

    if (read_fd < 0) {
        return false;
    }

    RespParser parser;
    char buf[8192];
    int bytes_read = 0;

    while ((bytes_read = os_read(read_fd, buf, sizeof(buf))) > 0) {
        parser.feed(buf, bytes_read);
        vector<string> args;
        while (parser.next_command(args)) {
            callback(args);
        }
    }

    os_close(read_fd);
    return true;
}

size_t Aof::size() const {
    struct stat st;
    if (stat(filename_.c_str(), &st) == 0) {
        return static_cast<size_t>(st.st_size);
    }
    return 0;
}

}
