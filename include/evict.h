#pragma once

#include "dict.h"
#include <string>
#include <cstdint>

namespace redis {

enum class EvictPolicy {
    NoEviction,
    AllKeysLRU,
    VolatileLRU,
    AllKeysLFU,
    VolatileLFU
};

struct FreqBucket {
    uint32_t frequency{1};
    Entry* head{nullptr};
    Entry* tail{nullptr};
    FreqBucket* prev{nullptr};
    FreqBucket* next{nullptr};

    explicit FreqBucket(uint32_t freq);
};

class Lfu {
public:
    Lfu();
    ~Lfu();

    Lfu(const Lfu&) = delete;
    Lfu& operator=(const Lfu&) = delete;

    void add(Entry* entry);
    void touch(Entry* entry);
    void remove(Entry* entry);
    Entry* victim();
    void clear();

private:
    FreqBucket* head_{nullptr};
    FreqBucket* tail_{nullptr};

    void insert_after(FreqBucket* prev_bucket, FreqBucket* new_bucket);
    void insert_head(FreqBucket* new_bucket);
    void remove_bucket(FreqBucket* bucket);
};

class Evictor {
public:
    explicit Evictor(EvictPolicy policy = EvictPolicy::AllKeysLFU);
    ~Evictor() = default;

    void set_policy(EvictPolicy policy) { policy_ = policy; }
    EvictPolicy policy() const { return policy_; }

    void on_insert(Entry* entry);
    void on_touch(Entry* entry);
    void on_remove(Entry* entry);

    std::string pick_victim(Dict& dict, int sample_size = 5);
    void clear();

private:
    EvictPolicy policy_;
    Lfu lfu_;

    std::string sample_lru(Dict& dict, bool volatile_only, int sample_size);
};

}
