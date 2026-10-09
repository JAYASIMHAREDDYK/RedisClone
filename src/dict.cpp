#include "dict.h"
#include "common.h"
#include <cstdlib>
#include <random>

using std::string;
using std::string_view;
using std::function;

namespace redis {

Entry::Entry(string k, Value v)
    : key(std::move(k)), value(std::move(v)), last_accessed_time(unix_time_sec()) {}

Dict::Dict() {
    ht[0] = Table{};
    ht[1] = Table{};
    rehashidx = -1;
}

Dict::~Dict() {
    clear();
}

size_t Dict::next_pow2(size_t size) {
    if (size <= 4) return 4;
    size_t p = 1;
    while (p < size) p <<= 1;
    return p;
}

void Dict::free_table(Table& t) {
    if (!t.table) return;
    for (size_t i = 0; i < t.size; ++i) {
        Entry* curr = t.table[i];
        while (curr) {
            Entry* next = curr->next;
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

void Dict::clear() {
    free_table(ht[0]);
    free_table(ht[1]);
    rehashidx = -1;
}

void Dict::resize(size_t new_size) {
    if (is_rehashing() || ht[0].used > new_size) return;
    new_size = next_pow2(new_size);

    Table new_table;
    new_table.size = new_size;
    new_table.sizemask = new_size - 1;
    new_table.used = 0;
    new_table.table = static_cast<Entry**>(calloc(new_size, sizeof(Entry*)));

    if (!ht[0].table) {
        ht[0] = new_table;
        return;
    }

    ht[1] = new_table;
    rehashidx = 0;
}

void Dict::expand_if_needed() {
    if (is_rehashing()) return;
    if (ht[0].size == 0) {
        resize(4);
        return;
    }
    if (ht[0].used >= ht[0].size) {
        resize(ht[0].used * 2);
    }
}

void Dict::step_rehash(int n) {
    if (!is_rehashing()) return;

    while (n-- && ht[0].used != 0) {
        while (rehashidx < static_cast<int64_t>(ht[0].size) && ht[0].table[rehashidx] == nullptr) {
            rehashidx++;
        }

        if (rehashidx >= static_cast<int64_t>(ht[0].size)) {
            break;
        }

        Entry* curr = ht[0].table[rehashidx];
        while (curr) {
            Entry* next = curr->next;
            size_t idx = hash_key(curr->key) & ht[1].sizemask;
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
        ht[1] = Table{};
        rehashidx = -1;
    }
}

void Dict::rehash_ms(int ms) {
    uint64_t start = monotonic_time_ms();
    while (is_rehashing()) {
        step_rehash(100);
        if (static_cast<int>(monotonic_time_ms() - start) >= ms) {
            break;
        }
    }
}

bool Dict::set(const string& key, Value val) {
    if (is_rehashing()) {
        step_rehash(1);
    }

    Entry* existing = find(key);
    if (existing) {
        existing->value = std::move(val);
        existing->last_accessed_time = unix_time_sec();
        if (existing->frequency < 0xFFFFFFFF) {
            existing->frequency++;
        }
        return false;
    }

    expand_if_needed();

    // New keys are placed into ht[1] during rehash so ht[0] continuously drains
    Table* target = is_rehashing() ? &ht[1] : &ht[0];
    size_t idx = hash_key(key) & target->sizemask;

    auto* entry = new Entry(key, std::move(val));
    entry->next = target->table[idx];
    target->table[idx] = entry;
    target->used++;

    return true;
}

Entry* Dict::find(string_view key) {
    if (ht[0].size == 0) return nullptr;

    if (is_rehashing()) {
        step_rehash(1);
    }

    uint64_t h = hash_key(key);
    for (int table_idx = 0; table_idx <= 1; ++table_idx) {
        Table& t = ht[table_idx];
        if (t.size == 0) break;

        size_t idx = h & t.sizemask;
        Entry* curr = t.table[idx];
        while (curr) {
            if (curr->key == key) {
                return curr;
            }
            curr = curr->next;
        }
        if (!is_rehashing()) break;
    }

    return nullptr;
}

bool Dict::erase(string_view key) {
    if (ht[0].size == 0) return false;

    if (is_rehashing()) {
        step_rehash(1);
    }

    uint64_t h = hash_key(key);
    for (int table_idx = 0; table_idx <= 1; ++table_idx) {
        Table& t = ht[table_idx];
        if (t.size == 0) break;

        size_t idx = h & t.sizemask;
        Entry* curr = t.table[idx];
        Entry* prev = nullptr;

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

        if (!is_rehashing()) break;
    }

    return false;
}

Entry* Dict::random_entry() {
    if (size() == 0) return nullptr;

    if (is_rehashing()) {
        step_rehash(1);
    }

    static thread_local std::mt19937_64 rng(42);

    int table_idx = 0;
    if (is_rehashing() && ht[1].used > 0) {
        table_idx = (rng() % (ht[0].used + ht[1].used) >= ht[0].used) ? 1 : 0;
    }

    Table& t = ht[table_idx];
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
        Entry* curr = t.table[idx];
        if (curr) {
            size_t count = 0;
            for (Entry* p = curr; p; p = p->next) count++;
            size_t pick = rng() % count;
            for (size_t j = 0; j < pick; ++j) curr = curr->next;
            return curr;
        }
    }

    return nullptr;
}

void Dict::scan(const function<void(Entry*)>& callback) {
    for (int t = 0; t <= 1; ++t) {
        if (ht[t].table) {
            for (size_t i = 0; i < ht[t].size; ++i) {
                Entry* curr = ht[t].table[i];
                while (curr) {
                    Entry* next = curr->next;
                    callback(curr);
                    curr = next;
                }
            }
        }
        if (!is_rehashing()) break;
    }
}

}
