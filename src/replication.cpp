#include "replication.h"
#include <random>
#include <sstream>
#include <iomanip>

namespace redis {

ReplicationBacklog::ReplicationBacklog(size_t max_size)
    : max_size_(max_size) {}

void ReplicationBacklog::feed(const std::string& raw_resp) {
    if (raw_resp.empty()) return;

    buffer_.append(raw_resp);
    master_offset_ += static_cast<int64_t>(raw_resp.size());

    if (buffer_.size() > max_size_) {
        size_t overflow = buffer_.size() - max_size_;
        buffer_.erase(0, overflow);
        start_offset_ = master_offset_ - static_cast<int64_t>(buffer_.size());
    }
}

bool ReplicationBacklog::can_partial_resync(const std::string& req_id, int64_t req_offset, const std::string& my_id) const {
    if (req_id.empty() || req_id != my_id) {
        return false;
    }
    if (req_offset < 0) {
        return false;
    }
    return (req_offset >= start_offset_ && req_offset <= master_offset_);
}

std::string ReplicationBacklog::get_data(int64_t from_offset) const {
    if (from_offset < start_offset_ || from_offset > master_offset_) {
        return "";
    }
    size_t idx = static_cast<size_t>(from_offset - start_offset_);
    if (idx >= buffer_.size()) {
        return "";
    }
    return buffer_.substr(idx);
}

std::string generate_replication_id() {
    static const char hex_chars[] = "0123456789abcdef";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);

    std::string id;
    id.reserve(40);
    for (int i = 0; i < 40; ++i) {
        id.push_back(hex_chars[dis(gen)]);
    }
    return id;
}

}
