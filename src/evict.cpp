#include "evict.h"
#include "common.h"
#include <limits>

using std::string;

namespace redis {

FreqBucket::FreqBucket(uint32_t freq) : frequency(freq) {}

Lfu::Lfu() = default;

Lfu::~Lfu() {
    clear();
}

void Lfu::clear() {
    FreqBucket* curr = head_;
    while (curr) {
        FreqBucket* next = curr->next;
        delete curr;
        curr = next;
    }
    head_ = nullptr;
    tail_ = nullptr;
}

void Lfu::insert_head(FreqBucket* new_bucket) {
    new_bucket->prev = nullptr;
    new_bucket->next = head_;
    if (head_) {
        head_->prev = new_bucket;
    } else {
        tail_ = new_bucket;
    }
    head_ = new_bucket;
}

void Lfu::insert_after(FreqBucket* prev_bucket, FreqBucket* new_bucket) {
    new_bucket->prev = prev_bucket;
    new_bucket->next = prev_bucket->next;
    if (prev_bucket->next) {
        prev_bucket->next->prev = new_bucket;
    } else {
        tail_ = new_bucket;
    }
    prev_bucket->next = new_bucket;
}

void Lfu::remove_bucket(FreqBucket* bucket) {
    if (bucket->prev) {
        bucket->prev->next = bucket->next;
    } else {
        head_ = bucket->next;
    }

    if (bucket->next) {
        bucket->next->prev = bucket->prev;
    } else {
        tail_ = bucket->prev;
    }

    delete bucket;
}

void Lfu::add(Entry* entry) {
    if (!entry) return;

    entry->frequency = 1;

    FreqBucket* target = head_;
    if (!target || target->frequency != 1) {
        target = new FreqBucket(1);
        insert_head(target);
    }

    entry->frequency_bucket = target;
    entry->lfu_prev = nullptr;
    entry->lfu_next = target->head;

    if (target->head) {
        target->head->lfu_prev = entry;
    } else {
        target->tail = entry;
    }
    target->head = entry;
}

void Lfu::touch(Entry* entry) {
    if (!entry) return;

    auto* curr_bucket = static_cast<FreqBucket*>(entry->frequency_bucket);
    if (!curr_bucket) {
        add(entry);
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

    FreqBucket* next_bucket = curr_bucket->next;
    if (!next_bucket || next_bucket->frequency != next_freq) {
        next_bucket = new FreqBucket(next_freq);
        insert_after(curr_bucket, next_bucket);
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
        remove_bucket(curr_bucket);
    }
}

void Lfu::remove(Entry* entry) {
    if (!entry || !entry->frequency_bucket) return;

    auto* bucket = static_cast<FreqBucket*>(entry->frequency_bucket);

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
        remove_bucket(bucket);
    }
}

Entry* Lfu::victim() {
    if (!head_ || !head_->tail) {
        return nullptr;
    }
    // Evict oldest node in lowest frequency bucket
    return head_->tail;
}

Evictor::Evictor(EvictPolicy policy) : policy_(policy) {}

void Evictor::on_insert(Entry* entry) {
    if (policy_ == EvictPolicy::AllKeysLFU || policy_ == EvictPolicy::VolatileLFU) {
        if (policy_ == EvictPolicy::VolatileLFU && entry->expire_at_ms == 0) {
            return;
        }
        lfu_.add(entry);
    }
}

void Evictor::on_touch(Entry* entry) {
    entry->last_accessed_time = unix_time_sec();
    if (policy_ == EvictPolicy::AllKeysLFU || policy_ == EvictPolicy::VolatileLFU) {
        if (policy_ == EvictPolicy::VolatileLFU && entry->expire_at_ms == 0) {
            return;
        }
        lfu_.touch(entry);
    }
}

void Evictor::on_remove(Entry* entry) {
    if (policy_ == EvictPolicy::AllKeysLFU || policy_ == EvictPolicy::VolatileLFU) {
        lfu_.remove(entry);
    }
}

string Evictor::sample_lru(Dict& dict, bool volatile_only, int sample_size) {
    Entry* best = nullptr;
    uint32_t oldest = std::numeric_limits<uint32_t>::max();

    for (int i = 0; i < sample_size * 2; ++i) {
        Entry* e = dict.random_entry();
        if (!e) break;

        if (volatile_only && e->expire_at_ms == 0) {
            continue;
        }

        if (e->last_accessed_time < oldest) {
            oldest = e->last_accessed_time;
            best = e;
        }
    }

    return best ? best->key : "";
}

string Evictor::pick_victim(Dict& dict, int sample_size) {
    switch (policy_) {
        case EvictPolicy::NoEviction:
            return "";

        case EvictPolicy::AllKeysLFU:
        case EvictPolicy::VolatileLFU: {
            Entry* e = lfu_.victim();
            return e ? e->key : "";
        }

        case EvictPolicy::AllKeysLRU:
            return sample_lru(dict, false, sample_size);

        case EvictPolicy::VolatileLRU:
            return sample_lru(dict, true, sample_size);
    }
    return "";
}

void Evictor::clear() {
    lfu_.clear();
}

}
