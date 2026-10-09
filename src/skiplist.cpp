#include "skiplist.h"
#include <random>
#include <algorithm>

using std::string;
using std::vector;
using std::pair;
using std::optional;

namespace redis {

SkipNode::SkipNode(int lvl, double s, string m)
    : member(std::move(m)), score(s), level(lvl) {}

SkipList::SkipList() {
    header = new SkipNode(SKIPLIST_MAXLEVEL, 0.0, "");
    for (int i = 0; i < SKIPLIST_MAXLEVEL; ++i) {
        header->level[i].forward = nullptr;
        header->level[i].span = 0;
    }
    tail = nullptr;
    length = 0;
    max_level = 1;
}

SkipList::~SkipList() {
    clear();
    delete header;
}

SkipList::SkipList(SkipList&& other) noexcept 
    : header(other.header), tail(other.tail), length(other.length), max_level(other.max_level) {
    other.header = nullptr;
    other.tail = nullptr;
    other.length = 0;
    other.max_level = 1;
}

SkipList& SkipList::operator=(SkipList&& other) noexcept {
    if (this != &other) {
        clear();
        delete header;

        header = other.header;
        tail = other.tail;
        length = other.length;
        max_level = other.max_level;

        other.header = nullptr;
        other.tail = nullptr;
        other.length = 0;
        other.max_level = 1;
    }
    return *this;
}

int SkipList::random_level() {
    static thread_local std::mt19937 gen(1337);
    static thread_local std::uniform_real_distribution<double> dist(0.0, 1.0);

    int lvl = 1;
    while (dist(gen) < SKIPLIST_P && lvl < SKIPLIST_MAXLEVEL) {
        lvl++;
    }
    return lvl;
}

SkipNode* SkipList::insert(double score, const string& member) {
    SkipNode* update[SKIPLIST_MAXLEVEL];
    uint32_t rank[SKIPLIST_MAXLEVEL];
    SkipNode* x = header;

    for (int i = max_level - 1; i >= 0; --i) {
        rank[i] = (i == max_level - 1) ? 0 : rank[i + 1];
        while (x->level[i].forward &&
               (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score && x->level[i].forward->member < member))) {
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    int lvl = random_level();
    if (lvl > max_level) {
        for (int i = max_level; i < lvl; ++i) {
            rank[i] = 0;
            update[i] = header;
            header->level[i].span = static_cast<uint32_t>(length);
        }
        max_level = lvl;
    }

    x = new SkipNode(lvl, score, member);
    for (int i = 0; i < lvl; ++i) {
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;

        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    for (int i = lvl; i < max_level; ++i) {
        update[i]->level[i].span++;
    }

    x->backward = (update[0] == header) ? nullptr : update[0];
    if (x->level[0].forward) {
        x->level[0].forward->backward = x;
    } else {
        tail = x;
    }

    length++;
    return x;
}

bool SkipList::erase(double score, const string& member) {
    SkipNode* update[SKIPLIST_MAXLEVEL];
    SkipNode* x = header;

    for (int i = max_level - 1; i >= 0; --i) {
        while (x->level[i].forward &&
               (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score && x->level[i].forward->member < member))) {
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    x = x->level[0].forward;
    if (x && x->score == score && x->member == member) {
        for (int i = 0; i < max_level; ++i) {
            if (update[i]->level[i].forward == x) {
                update[i]->level[i].span += x->level[i].span - 1;
                update[i]->level[i].forward = x->level[i].forward;
            } else {
                update[i]->level[i].span -= 1;
            }
        }

        if (x->level[0].forward) {
            x->level[0].forward->backward = x->backward;
        } else {
            tail = x->backward;
        }

        while (max_level > 1 && header->level[max_level - 1].forward == nullptr) {
            max_level--;
        }

        delete x;
        length--;
        return true;
    }
    return false;
}

uint32_t SkipList::rank_of(double score, const string& member) const {
    uint32_t rank = 0;
    SkipNode* x = header;

    for (int i = max_level - 1; i >= 0; --i) {
        while (x->level[i].forward &&
               (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score && x->level[i].forward->member <= member))) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }
        if (x && x->member == member) {
            return rank;
        }
    }
    return 0;
}

SkipNode* SkipList::node_at_rank(uint32_t rank) const {
    if (rank == 0 || rank > length) return nullptr;

    uint32_t traversed = 0;
    SkipNode* x = header;

    for (int i = max_level - 1; i >= 0; --i) {
        while (x->level[i].forward && traversed + x->level[i].span <= rank) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank) {
            return x;
        }
    }
    return nullptr;
}

SkipNode* SkipList::first_in_range(double min_score, double max_score, 
                                   bool min_inclusive, bool max_inclusive) const {
    if (length == 0) return nullptr;

    SkipNode* x = header;
    for (int i = max_level - 1; i >= 0; --i) {
        while (x->level[i].forward) {
            double next_score = x->level[i].forward->score;
            if (min_inclusive ? (next_score < min_score) : (next_score <= min_score)) {
                x = x->level[i].forward;
            } else {
                break;
            }
        }
    }

    x = x->level[0].forward;
    if (!x) return nullptr;

    if (max_inclusive ? (x->score > max_score) : (x->score >= max_score)) {
        return nullptr;
    }

    return x;
}

void SkipList::clear() {
    if (!header) return;
    SkipNode* curr = header->level[0].forward;
    while (curr) {
        SkipNode* next = curr->level[0].forward;
        delete curr;
        curr = next;
    }
    for (int i = 0; i < SKIPLIST_MAXLEVEL; ++i) {
        header->level[i].forward = nullptr;
        header->level[i].span = 0;
    }
    tail = nullptr;
    length = 0;
    max_level = 1;
}

bool ZSet::add(double score, const string& member) {
    auto it = dict.find(member);
    if (it != dict.end()) {
        if (it->second == score) {
            return false;
        }
        skiplist.erase(it->second, member);
        dict[member] = score;
        skiplist.insert(score, member);
        return false;
    }

    dict[member] = score;
    skiplist.insert(score, member);
    return true;
}

bool ZSet::remove(const string& member) {
    auto it = dict.find(member);
    if (it == dict.end()) {
        return false;
    }
    skiplist.erase(it->second, member);
    dict.erase(it);
    return true;
}

optional<double> ZSet::score_of(const string& member) const {
    auto it = dict.find(member);
    if (it != dict.end()) {
        return it->second;
    }
    return std::nullopt;
}

optional<uint32_t> ZSet::rank_of(const string& member) const {
    auto it = dict.find(member);
    if (it == dict.end()) {
        return std::nullopt;
    }
    // rank_of returns 1-indexed rank; return 0-indexed rank for Redis ZRANK
    uint32_t r = skiplist.rank_of(it->second, member);
    if (r > 0) {
        return r - 1;
    }
    return std::nullopt;
}

vector<pair<string, double>> ZSet::range(int64_t start, int64_t stop, bool with_scores) const {
    (void)with_scores;
    vector<pair<string, double>> result;
    int64_t total = static_cast<int64_t>(skiplist.length);
    if (total == 0) return result;

    if (start < 0) start = total + start;
    if (stop < 0) stop = total + stop;
    if (start < 0) start = 0;

    if (start > stop || start >= total) return result;
    if (stop >= total) stop = total - 1;

    uint32_t current_rank = static_cast<uint32_t>(start + 1);
    SkipNode* node = skiplist.node_at_rank(current_rank);
    int64_t count = stop - start + 1;

    while (node && count-- > 0) {
        result.emplace_back(node->member, node->score);
        node = node->level[0].forward;
    }

    return result;
}

vector<pair<string, double>> ZSet::range_by_score(
    double min_score, double max_score, 
    bool min_inclusive, bool max_inclusive,
    int64_t offset, int64_t count,
    bool with_scores) const {
    (void)with_scores;
    vector<pair<string, double>> result;
    if (skiplist.length == 0) return result;

    SkipNode* node = skiplist.first_in_range(min_score, max_score, min_inclusive, max_inclusive);
    while (node && offset > 0) {
        offset--;
        node = node->level[0].forward;
    }

    while (node) {
        if (max_inclusive ? (node->score > max_score) : (node->score >= max_score)) {
            break;
        }
        result.emplace_back(node->member, node->score);
        if (count > 0 && static_cast<int64_t>(result.size()) >= count) {
            break;
        }
        node = node->level[0].forward;
    }

    return result;
}

}
