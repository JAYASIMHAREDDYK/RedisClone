#pragma once

#include "skiplist.h"
#include <string>
#include <string_view>
#include <variant>
#include <memory>
#include <vector>
#include <functional>
#include <cstdint>

namespace redis {

using DictValue = std::variant<std::string, std::shared_ptr<SortedSet>>;

struct DictEntry {
    std::string key;
    DictValue value;
    uint64_t expire_at_ms{0};
    uint32_t frequency{1};
    uint32_t last_accessed_time{0};
    DictEntry* next{nullptr};

    DictEntry* lfu_prev{nullptr};
    DictEntry* lfu_next{nullptr};
    void* frequency_bucket{nullptr};

    DictEntry(std::string k, DictValue v);
    ~DictEntry() = default;

    bool isString() const {
        return std::holds_alternative<std::string>(value);
    }

    bool isZSet() const {
        return std::holds_alternative<std::shared_ptr<SortedSet>>(value);
    }

    const std::string& getString() const {
        return std::get<std::string>(value);
    }

    std::string& getString() {
        return std::get<std::string>(value);
    }

    std::shared_ptr<SortedSet> getZSet() const {
        return std::get<std::shared_ptr<SortedSet>>(value);
    }
};

struct DictTable {
    DictEntry** table{nullptr};
    size_t size{0};
    size_t sizemask{0};
    size_t used{0};
};

class ProgressiveDict {
public:
    DictTable ht[2];
    int64_t rehashidx{-1};

    ProgressiveDict();
    ~ProgressiveDict();

    ProgressiveDict(const ProgressiveDict&) = delete;
    ProgressiveDict& operator=(const ProgressiveDict&) = delete;

    bool isRehashing() const {
        return rehashidx != -1;
    }

    void stepRehash(int n = 1);
    void rehashMilliseconds(int ms);

    bool set(const std::string& key, DictValue val);
    DictEntry* find(std::string_view key);
    bool erase(std::string_view key);

    size_t size() const {
        return ht[0].used + ht[1].used;
    }

    DictEntry* getRandomEntry();

    void forEach(const std::function<void(DictEntry*)>& callback);

    void clear();

private:
    void expandIfNeeded();
    void resize(size_t new_size);
    static size_t nextPowerOf2(size_t size);
    void freeTable(DictTable& t);
};

}
