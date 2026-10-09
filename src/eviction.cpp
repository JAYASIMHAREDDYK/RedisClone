#include "eviction.h"
#include "util.h"
#include <limits>

namespace redis {

FrequencyBucket::FrequencyBucket(uint32_t freq) : frequency(freq) {}

LfuManager::LfuManager() = default;

LfuManager::~LfuManager() {
    clear();
}

void LfuManager::clear() {
    FrequencyBucket* curr = bucket_head_;
    while (curr) {
        FrequencyBucket* next = curr->next;
        delete curr;
        curr = next;
    }
    bucket_head_ = nullptr;
    bucket_tail_ = nullptr;
}

void LfuManager::insertBucketAtHead(FrequencyBucket* new_bucket) {
    new_bucket->prev = nullptr;
    new_bucket->next = bucket_head_;
    if (bucket_head_) {
        bucket_head_->prev = new_bucket;
    } else {
        bucket_tail_ = new_bucket;
    }
    bucket_head_ = new_bucket;
}

void LfuManager::insertBucketAfter(FrequencyBucket* prev_bucket, FrequencyBucket* new_bucket) {
    new_bucket->prev = prev_bucket;
    new_bucket->next = prev_bucket->next;
    if (prev_bucket->next) {
        prev_bucket->next->prev = new_bucket;
    } else {
        bucket_tail_ = new_bucket;
    }
    prev_bucket->next = new_bucket;
}

void LfuManager::removeBucket(FrequencyBucket* bucket) {
    if (bucket->prev) {
        bucket->prev->next = bucket->next;
    } else {
        bucket_head_ = bucket->next;
    }

    if (bucket->next) {
        bucket->next->prev = bucket->prev;
    } else {
        bucket_tail_ = bucket->prev;
    }

    delete bucket;
}

void LfuManager::addKey(DictEntry* entry) {
    if (!entry) return;

    entry->frequency = 1;

    FrequencyBucket* target_bucket = bucket_head_;
    if (!target_bucket || target_bucket->frequency != 1) {
        target_bucket = new FrequencyBucket(1);
        insertBucketAtHead(target_bucket);
    }

    entry->frequency_bucket = target_bucket;
    entry->lfu_prev = nullptr;
    entry->lfu_next = target_bucket->head;

    if (target_bucket->head) {
        target_bucket->head->lfu_prev = entry;
    } else {
        target_bucket->tail = entry;
    }
    target_bucket->head = entry;
}

void LfuManager::accessKey(DictEntry* entry) {
    if (!entry) return;

    auto* curr_bucket = static_cast<FrequencyBucket*>(entry->frequency_bucket);
    if (!curr_bucket) {
        addKey(entry);
        return;
    }

    uint32_t next_freq = entry->frequency + 1;
    entry->frequency = next_freq;

    if (entry->lfu_prev) {
        entry->lfu_prev->lfu_next = entry->lfu_next;
    } else {
        curr_bucket->head = entry->lfu_next;
    }

    if (entry->lfu_next) {
        entry->lfu_next->lfu_prev = entry->lfu_prev;
    } else {
        curr_bucket->tail = entry->lfu_prev;
    }

    FrequencyBucket* next_bucket = curr_bucket->next;
    if (!next_bucket || next_bucket->frequency != next_freq) {
        next_bucket = new FrequencyBucket(next_freq);
        insertBucketAfter(curr_bucket, next_bucket);
    }

    entry->frequency_bucket = next_bucket;
    entry->lfu_prev = nullptr;
    entry->lfu_next = next_bucket->head;

    if (next_bucket->head) {
        next_bucket->head->lfu_prev = entry;
    } else {
        next_bucket->tail = entry;
    }
    next_bucket->head = entry;

    if (!curr_bucket->head) {
        removeBucket(curr_bucket);
    }
}

void LfuManager::removeKey(DictEntry* entry) {
    if (!entry || !entry->frequency_bucket) return;

    auto* bucket = static_cast<FrequencyBucket*>(entry->frequency_bucket);

    if (entry->lfu_prev) {
        entry->lfu_prev->lfu_next = entry->lfu_next;
    } else {
        bucket->head = entry->lfu_next;
    }

    if (entry->lfu_next) {
        entry->lfu_next->lfu_prev = entry->lfu_prev;
    } else {
        bucket->tail = entry->lfu_prev;
    }

    entry->frequency_bucket = nullptr;
    entry->lfu_prev = nullptr;
    entry->lfu_next = nullptr;

    if (!bucket->head) {
        removeBucket(bucket);
    }
}

DictEntry* LfuManager::getEvictionCandidate() {
    if (!bucket_head_ || !bucket_head_->tail) {
        return nullptr;
    }
    return bucket_head_->tail;
}

EvictionEngine::EvictionEngine(EvictionPolicy policy) : policy_(policy) {}

void EvictionEngine::onKeyInserted(DictEntry* entry) {
    if (policy_ == EvictionPolicy::AllKeysLFU || policy_ == EvictionPolicy::VolatileLFU) {
        if (policy_ == EvictionPolicy::VolatileLFU && entry->expire_at_ms == 0) {
            return;
        }
        lfu_.addKey(entry);
    }
}

void EvictionEngine::onKeyAccessed(DictEntry* entry) {
    entry->last_accessed_time = getUnixTimeSec();
    if (policy_ == EvictionPolicy::AllKeysLFU || policy_ == EvictionPolicy::VolatileLFU) {
        if (policy_ == EvictionPolicy::VolatileLFU && entry->expire_at_ms == 0) {
            return;
        }
        lfu_.accessKey(entry);
    }
}

void EvictionEngine::onKeyRemoved(DictEntry* entry) {
    if (policy_ == EvictionPolicy::AllKeysLFU || policy_ == EvictionPolicy::VolatileLFU) {
        lfu_.removeKey(entry);
    }
}

std::string EvictionEngine::sampleLruCandidate(ProgressiveDict& dict, bool volatile_only, int sample_size) {
    DictEntry* best_entry = nullptr;
    uint32_t oldest_time = std::numeric_limits<uint32_t>::max();

    for (int i = 0; i < sample_size * 2; ++i) {
        DictEntry* e = dict.getRandomEntry();
        if (!e) break;

        if (volatile_only && e->expire_at_ms == 0) {
            continue;
        }

        if (e->last_accessed_time < oldest_time) {
            oldest_time = e->last_accessed_time;
            best_entry = e;
        }
    }

    if (best_entry) {
        return best_entry->key;
    }
    return "";
}

std::string EvictionEngine::selectEvictionCandidate(ProgressiveDict& dict, int sample_size) {
    switch (policy_) {
        case EvictionPolicy::NoEviction:
            return "";

        case EvictionPolicy::AllKeysLFU: {
            DictEntry* e = lfu_.getEvictionCandidate();
            return e ? e->key : "";
        }

        case EvictionPolicy::VolatileLFU: {
            DictEntry* e = lfu_.getEvictionCandidate();
            return e ? e->key : "";
        }

        case EvictionPolicy::AllKeysLRU:
            return sampleLruCandidate(dict, false, sample_size);

        case EvictionPolicy::VolatileLRU:
            return sampleLruCandidate(dict, true, sample_size);
    }
    return "";
}

void EvictionEngine::clear() {
    lfu_.clear();
}

}
