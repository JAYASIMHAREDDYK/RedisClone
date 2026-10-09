#include "dict.h"
#include "util.h"
#include <cstdlib>
#include <cstring>
#include <random>

namespace redis {

DictEntry::DictEntry(std::string k, DictValue v)
    : key(std::move(k)), value(std::move(v)), last_accessed_time(getUnixTimeSec()) {}

ProgressiveDict::ProgressiveDict() {
    ht[0] = DictTable{};
    ht[1] = DictTable{};
    rehashidx = -1;
}

ProgressiveDict::~ProgressiveDict() {
    clear();
}

size_t ProgressiveDict::nextPowerOf2(size_t size) {
    if (size <= 4) return 4;
    size_t p = 1;
    while (p < size) {
        p <<= 1;
    }
    return p;
}

void ProgressiveDict::freeTable(DictTable& t) {
    if (!t.table) return;
    for (size_t i = 0; i < t.size; ++i) {
        DictEntry* curr = t.table[i];
        while (curr) {
            DictEntry* next = curr->next;
            delete curr;
            curr = next;
        }
    }
    free(t.table);
    t.table = nullptr;
    t.size = 0;
    t.sizemask = 0;
    t.used = 0;
}

void ProgressiveDict::clear() {
    freeTable(ht[0]);
    freeTable(ht[1]);
    rehashidx = -1;
}

void ProgressiveDict::resize(size_t new_size) {
    if (isRehashing() || ht[0].used > new_size) return;
    new_size = nextPowerOf2(new_size);

    DictTable new_table;
    new_table.size = new_size;
    new_table.sizemask = new_size - 1;
    new_table.used = 0;
    new_table.table = static_cast<DictEntry**>(calloc(new_size, sizeof(DictEntry*)));

    if (!ht[0].table) {
        ht[0] = new_table;
        return;
    }

    ht[1] = new_table;
    rehashidx = 0;
}

void ProgressiveDict::expandIfNeeded() {
    if (isRehashing()) return;
    if (ht[0].size == 0) {
        resize(4);
        return;
    }
    if (ht[0].used >= ht[0].size) {
        resize(ht[0].used * 2);
    }
}

void ProgressiveDict::stepRehash(int n) {
    if (!isRehashing()) return;

    while (n-- && ht[0].used != 0) {
        while (rehashidx < static_cast<int64_t>(ht[0].size) && ht[0].table[rehashidx] == nullptr) {
            rehashidx++;
        }

        if (rehashidx >= static_cast<int64_t>(ht[0].size)) {
            break;
        }

        DictEntry* curr = ht[0].table[rehashidx];
        while (curr) {
            DictEntry* next = curr->next;
            size_t idx = hashKey(curr->key) & ht[1].sizemask;
            curr->next = ht[1].table[idx];
            ht[1].table[idx] = curr;
            ht[0].used--;
            ht[1].used++;
            curr = next;
        }
        ht[0].table[rehashidx] = nullptr;
        rehashidx++;
    }

    if (ht[0].used == 0) {
        free(ht[0].table);
        ht[0] = ht[1];
        ht[1] = DictTable{};
        rehashidx = -1;
    }
}

void ProgressiveDict::rehashMilliseconds(int ms) {
    uint64_t start = getMonotonicTimeMs();
    while (isRehashing()) {
        stepRehash(100);
        if (static_cast<int>(getMonotonicTimeMs() - start) >= ms) {
            break;
        }
    }
}

bool ProgressiveDict::set(const std::string& key, DictValue val) {
    if (isRehashing()) {
        stepRehash(1);
    }

    DictEntry* existing = find(key);
    if (existing) {
        existing->value = std::move(val);
        existing->last_accessed_time = getUnixTimeSec();
        if (existing->frequency < 0xFFFFFFFF) {
            existing->frequency++;
        }
        return false;
    }

    expandIfNeeded();

    DictTable* target = isRehashing() ? &ht[1] : &ht[0];
    size_t idx = hashKey(key) & target->sizemask;

    auto* entry = new DictEntry(key, std::move(val));
    entry->next = target->table[idx];
    target->table[idx] = entry;
    target->used++;

    return true;
}

DictEntry* ProgressiveDict::find(std::string_view key) {
    if (ht[0].size == 0) return nullptr;

    if (isRehashing()) {
        stepRehash(1);
    }

    uint64_t h = hashKey(key);
    for (int table_idx = 0; table_idx <= 1; ++table_idx) {
        DictTable& t = ht[table_idx];
        if (t.size == 0) break;

        size_t idx = h & t.sizemask;
        DictEntry* curr = t.table[idx];
        while (curr) {
            if (curr->key == key) {
                return curr;
            }
            curr = curr->next;
        }
        if (!isRehashing()) break;
    }

    return nullptr;
}

bool ProgressiveDict::erase(std::string_view key) {
    if (ht[0].size == 0) return false;

    if (isRehashing()) {
        stepRehash(1);
    }

    uint64_t h = hashKey(key);
    for (int table_idx = 0; table_idx <= 1; ++table_idx) {
        DictTable& t = ht[table_idx];
        if (t.size == 0) break;

        size_t idx = h & t.sizemask;
        DictEntry* curr = t.table[idx];
        DictEntry* prev = nullptr;

        while (curr) {
            if (curr->key == key) {
                if (prev) {
                    prev->next = curr->next;
                } else {
                    t.table[idx] = curr->next;
                }
                delete curr;
                t.used--;
                return true;
            }
            prev = curr;
            curr = curr->next;
        }

        if (!isRehashing()) break;
    }

    return false;
}

DictEntry* ProgressiveDict::getRandomEntry() {
    if (size() == 0) return nullptr;

    if (isRehashing()) {
        stepRehash(1);
    }

    static thread_local std::mt19937_64 rng(42);

    int table_idx = 0;
    if (isRehashing() && ht[1].used > 0) {
        table_idx = (rng() % (ht[0].used + ht[1].used) >= ht[0].used) ? 1 : 0;
    }

    DictTable& t = ht[table_idx];
    if (t.size == 0 || t.used == 0) {
        if (table_idx == 0 && ht[1].used > 0) {
            t = ht[1];
        } else {
            return nullptr;
        }
    }

    size_t start_idx = rng() % t.size;
    for (size_t i = 0; i < t.size; ++i) {
        size_t idx = (start_idx + i) & t.sizemask;
        DictEntry* curr = t.table[idx];
        if (curr) {
            size_t count = 0;
            for (DictEntry* p = curr; p; p = p->next) count++;
            size_t pick = rng() % count;
            for (size_t j = 0; j < pick; ++j) curr = curr->next;
            return curr;
        }
    }

    return nullptr;
}

void ProgressiveDict::forEach(const std::function<void(DictEntry*)>& callback) {
    for (int t = 0; t <= 1; ++t) {
        if (ht[t].table) {
            for (size_t i = 0; i < ht[t].size; ++i) {
                DictEntry* curr = ht[t].table[i];
                while (curr) {
                    DictEntry* next = curr->next;
                    callback(curr);
                    curr = next;
                }
            }
        }
        if (!isRehashing()) break;
    }
}

}
