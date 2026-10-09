#pragma once

#include "dict.h"
#include <string>
#include <unordered_set>
#include <vector>
#include <cstdint>
#include <functional>

namespace redis {

class Expirer {
public:
    using DeleteCallback = std::function<void(const std::string&)>;

    Expirer() = default;
    ~Expirer() = default;

    void set_expire(Entry* entry, uint64_t expire_at_ms);
    void clear_expire(Entry* entry);

    bool is_expired(Entry* entry) const;
    int64_t ttl_sec(Entry* entry) const;

    int sample_expired(Dict& dict, const DeleteCallback& on_delete, int max_ms = 10);
    void on_delete(const std::string& key);

    size_t size() const { return expiring_keys_.size(); }
    void clear() { expiring_keys_.clear(); }

private:
    std::unordered_set<std::string> expiring_keys_;
};

}
