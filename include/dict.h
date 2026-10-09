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

class ZSet;
using Value = std::variant<std::string, std::shared_ptr<ZSet>>;

struct Entry {
    std::string key;
    Value value;
    uint64_t expire_at_ms{0};
    uint32_t frequency{1};
    uint32_t last_accessed_time{0};
    Entry* next{nullptr};

    Entry* lfu_prev{nullptr};
    Entry* lfu_next{nullptr};
    void* frequency_bucket{nullptr};

    Entry(std::string k, Value v);

    bool is_str() const {
        return std::holds_alternative<std::string>(value);
    }

    bool is_zset() const {
        return std::holds_alternative<std::shared_ptr<ZSet>>(value);
    }

    const std::string& str() const {
        return std::get<std::string>(value);
    }

    std::string& str() {
        return std::get<std::string>(value);
    }

    std::shared_ptr<ZSet> zset() const {
        return std::get<std::shared_ptr<ZSet>>(value);
    }
};

struct Table {
    Entry** table{nullptr};
    size_t size{0};
    size_t sizemask{0};
    size_t used{0};
};

class Dict {
public:
    Table ht[2];
    int64_t rehashidx{-1};

    Dict();
    ~Dict();

    Dict(const Dict&) = delete;
    Dict& operator=(const Dict&) = delete;

    bool is_rehashing() const {
        return rehashidx != -1;
    }

    void step_rehash(int n = 1);
    void rehash_ms(int ms);

    bool set(const std::string& key, Value val);
    Entry* find(std::string_view key);
    bool erase(std::string_view key);

    size_t size() const {
        return ht[0].used + ht[1].used;
    }

    Entry* random_entry();
    void scan(const std::function<void(Entry*)>& callback);
    void clear();

private:
    void expand_if_needed();
    void resize(size_t new_size);
    static size_t next_pow2(size_t size);
    void free_table(Table& t);
};

}
