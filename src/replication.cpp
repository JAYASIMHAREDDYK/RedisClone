#include "replication.h"
#include <cstring>
#include <random>
#include <sstream>
#include <iomanip>

namespace redis {

ReplicationBacklog::ReplicationBacklog(size_t max_size)
    : max_size_(max_size) {
    if (max_size_ == 0) max_size_ = 1024 * 1024;
    buffer_.resize(max_size_);
}

void ReplicationBacklog::feed(const std::string& raw_resp) {
    feed(raw_resp.data(), raw_resp.size());
}

void ReplicationBacklog::feed(const char* data, size_t len) {
    if (!data || len == 0) return;

    if (len >= max_size_) {
        // Discard prefix that falls outside the circular window.
        // We only keep the last max_size_ bytes of this write.
        const char* kept_data = data + (len - max_size_);
        int64_t kept_start_offset = master_offset_ + static_cast<int64_t>(len - max_size_);
        size_t write_idx = static_cast<size_t>(kept_start_offset % max_size_);

        size_t chunk1 = std::min(max_size_, max_size_ - write_idx);
        std::memcpy(&buffer_[write_idx], kept_data, chunk1);

        size_t chunk2 = max_size_ - chunk1;
        if (chunk2 > 0) {
            std::memcpy(&buffer_[0], kept_data + chunk1, chunk2);
        }

        master_offset_ += static_cast<int64_t>(len);
        stored_bytes_ = max_size_;
        start_offset_ = kept_start_offset;
        write_idx_ = static_cast<size_t>(master_offset_ % max_size_);
        return;
    }

    size_t write_idx = static_cast<size_t>(master_offset_ % max_size_);
    size_t chunk1 = std::min(len, max_size_ - write_idx);
    std::memcpy(&buffer_[write_idx], data, chunk1);

    size_t chunk2 = len - chunk1;
    if (chunk2 > 0) {
        std::memcpy(&buffer_[0], data + chunk1, chunk2);
    }

    master_offset_ += static_cast<int64_t>(len);
    write_idx_ = static_cast<size_t>(master_offset_ % max_size_);
    stored_bytes_ = std::min(stored_bytes_ + len, max_size_);
    start_offset_ = master_offset_ - static_cast<int64_t>(stored_bytes_);
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
    size_t bytes_to_read = static_cast<size_t>(master_offset_ - from_offset);
    if (bytes_to_read == 0) return "";

    std::string result;
    result.resize(bytes_to_read);

    size_t ring_idx = static_cast<size_t>(from_offset % max_size_);

    size_t chunk1 = std::min(bytes_to_read, max_size_ - ring_idx);
    std::memcpy(&result[0], &buffer_[ring_idx], chunk1);

    size_t chunk2 = bytes_to_read - chunk1;
    if (chunk2 > 0) {
        std::memcpy(&result[chunk1], &buffer_[0], chunk2);
    }

    return result;
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
