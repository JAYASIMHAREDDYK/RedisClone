#include "expire.h"
#include "common.h"
#include <random>
#include <algorithm>

using std::string;
using std::vector;

namespace redis {

void Expirer::set_expire(Entry* entry, uint64_t expire_at_ms) {
    if (!entry) return;
    entry->expire_at_ms = expire_at_ms;
    expiring_keys_.insert(entry->key);
}

void Expirer::clear_expire(Entry* entry) {
    if (!entry) return;
    entry->expire_at_ms = 0;
    expiring_keys_.erase(entry->key);
}

bool Expirer::is_expired(Entry* entry) const {
    if (!entry || entry->expire_at_ms == 0) return false;
    return unix_time_ms() >= entry->expire_at_ms;
}

int64_t Expirer::ttl_sec(Entry* entry) const {
    if (!entry) return -2;
    if (entry->expire_at_ms == 0) return -1;

    uint64_t now = unix_time_ms();
    if (now >= entry->expire_at_ms) {
        return -2;
    }

    uint64_t diff_ms = entry->expire_at_ms - now;
    return static_cast<int64_t>((diff_ms + 999) / 1000);
}

void Expirer::on_delete(const string& key) {
    expiring_keys_.erase(key);
}

int Expirer::sample_expired(Dict& dict, const DeleteCallback& on_delete, int max_ms) {
    if (expiring_keys_.empty()) return 0;

    uint64_t start = monotonic_time_ms();
    int total_deleted = 0;

    static thread_local std::mt19937_64 rng(101);

    while (true) {
        if (expiring_keys_.empty()) break;

        size_t sample_size = std::min(static_cast<size_t>(20), expiring_keys_.size());
        vector<string> sampled;
        sampled.reserve(sample_size);

        size_t skip = rng() % expiring_keys_.size();
        auto it = expiring_keys_.begin();
        std::advance(it, skip);

        while (sampled.size() < sample_size) {
            if (it == expiring_keys_.end()) {
                it = expiring_keys_.begin();
            }
            sampled.push_back(*it);
            ++it;
        }

        int expired_in_sample = 0;
        for (const auto& key : sampled) {
            Entry* entry = dict.find(key);
            if (!entry) {
                expiring_keys_.erase(key);
                continue;
            }

            if (is_expired(entry)) {
                on_delete(key);
                expired_in_sample++;
                total_deleted++;
            }
        }

        // Active expiry stopping condition: if <= 25% of sampled keys expired, stop loop
        if (expired_in_sample <= static_cast<int>(sample_size) / 4) {
            break;
        }

        // Hard time cap to avoid blocking the reactor loop
        if (static_cast<int>(monotonic_time_ms() - start) >= max_ms) {
            break;
        }
    }

    return total_deleted;
}

}
