#pragma once

#include "dict.h"
#include <string>
#include <vector>
#include <cstdint>
#include <memory>

namespace redis {

enum class EvictionPolicy {
    NoEviction,
    AllKeysLRU,
    VolatileLRU,
    AllKeysLFU,
    VolatileLFU
};

struct FrequencyBucket {
    uint32_t frequency{1};
    DictEntry* head{nullptr};
    DictEntry* tail{nullptr};
    FrequencyBucket* prev{nullptr};
    FrequencyBucket* next{nullptr};

    explicit FrequencyBucket(uint32_t freq);
};

class LfuManager {
public:
    LfuManager();
    ~LfuManager();

    LfuManager(const LfuManager&) = delete;
    LfuManager& operator=(const LfuManager&) = delete;

    void addKey(DictEntry* entry);
    void accessKey(DictEntry* entry);
    void removeKey(DictEntry* entry);
    DictEntry* getEvictionCandidate();
    void clear();

private:
    FrequencyBucket* bucket_head_{nullptr};
    FrequencyBucket* bucket_tail_{nullptr};

    void insertBucketAfter(FrequencyBucket* prev_bucket, FrequencyBucket* new_bucket);
    void insertBucketAtHead(FrequencyBucket* new_bucket);
    void removeBucket(FrequencyBucket* bucket);
};

class EvictionEngine {
public:
    explicit EvictionEngine(EvictionPolicy policy = EvictionPolicy::AllKeysLFU);
    ~EvictionEngine() = default;

    void setPolicy(EvictionPolicy policy) { policy_ = policy; }
    EvictionPolicy getPolicy() const { return policy_; }

    void onKeyInserted(DictEntry* entry);
    void onKeyAccessed(DictEntry* entry);
    void onKeyRemoved(DictEntry* entry);

    std::string selectEvictionCandidate(ProgressiveDict& dict, int sample_size = 5);

    void clear();

private:
    EvictionPolicy policy_;
    LfuManager lfu_;

    std::string sampleLruCandidate(ProgressiveDict& dict, bool volatile_only, int sample_size);
};

}
