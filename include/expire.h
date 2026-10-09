#pragma once

#include "dict.h"
#include <string>
#include <unordered_set>
#include <vector>
#include <cstdint>
#include <functional>

namespace redis {

class ExpirationManager {
public:
    using DeleteCallback = std::function<void(const std::string&)>;

    ExpirationManager() = default;
    ~ExpirationManager() = default;

    void setExpire(DictEntry* entry, uint64_t expire_at_ms);
    void clearExpire(DictEntry* entry);

    bool isExpired(DictEntry* entry) const;

    int64_t getTtlSeconds(DictEntry* entry) const;

    int activeExpireCycle(ProgressiveDict& dict, const DeleteCallback& on_delete, int max_ms = 10);

    void onKeyDeleted(const std::string& key);

    size_t expiringKeysCount() const { return expiring_keys_.size(); }

    void clear() { expiring_keys_.clear(); }

private:
    std::unordered_set<std::string> expiring_keys_;
};

}
