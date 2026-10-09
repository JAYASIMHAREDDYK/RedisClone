#include "skiplist.h"
#include <random>
#include <cmath>
#include <algorithm>

namespace redis {

SkipListNode::SkipListNode(int lvl, double s, std::string m)
    : member(std::move(m)), score(s), level(lvl) {}

SkipList::SkipList() {
    header = new SkipListNode(SKIPLIST_MAXLEVEL, 0.0, "");
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

int SkipList::randomLevel() {
    static thread_local std::mt19937 gen(1337);
    static thread_local std::uniform_real_distribution<double> dist(0.0, 1.0);

    int lvl = 1;
    while (dist(gen) < SKIPLIST_P && lvl < SKIPLIST_MAXLEVEL) {
        lvl++;
    }
    return lvl;
}

SkipListNode* SkipList::insert(double score, const std::string& member) {
    SkipListNode* update[SKIPLIST_MAXLEVEL];
    uint32_t rank[SKIPLIST_MAXLEVEL];
    SkipListNode* x = header;

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

    int lvl = randomLevel();
    if (lvl > max_level) {
        for (int i = max_level; i < lvl; ++i) {
            rank[i] = 0;
            update[i] = header;
            header->level[i].span = static_cast<uint32_t>(length);
        }
        max_level = lvl;
    }

    x = new SkipListNode(lvl, score, member);
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

bool SkipList::erase(double score, const std::string& member) {
    SkipListNode* update[SKIPLIST_MAXLEVEL];
    SkipListNode* x = header;

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

uint32_t SkipList::getRank(double score, const std::string& member) const {
    uint32_t rank = 0;
    SkipListNode* x = header;

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

SkipListNode* SkipList::getNodeByRank(uint32_t rank) const {
    if (rank == 0 || rank > length) return nullptr;

    uint32_t traversed = 0;
    SkipListNode* x = header;

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

SkipListNode* SkipList::getFirstInRange(double min_score, double max_score, 
                                        bool min_inclusive, bool max_inclusive) const {
    if (length == 0) return nullptr;

    SkipListNode* x = header;
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
    SkipListNode* curr = header->level[0].forward;
    while (curr) {
        SkipListNode* next = curr->level[0].forward;
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

bool SortedSet::add(double score, const std::string& member) {
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

bool SortedSet::remove(const std::string& member) {
    auto it = dict.find(member);
    if (it == dict.end()) {
        return false;
    }
    skiplist.erase(it->second, member);
    dict.erase(it);
    return true;
}

std::optional<double> SortedSet::getScore(const std::string& member) const {
    auto it = dict.find(member);
    if (it != dict.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::optional<uint32_t> SortedSet::getRank(const std::string& member) const {
    auto it = dict.find(member);
    if (it == dict.end()) {
        return std::nullopt;
    }
    uint32_t rank = skiplist.getRank(it->second, member);
    if (rank > 0) {
        return rank - 1;
    }
    return std::nullopt;
}

std::vector<std::pair<std::string, double>> SortedSet::range(int64_t start, int64_t stop, bool with_scores) const {
    std::vector<std::pair<std::string, double>> result;
    int64_t total = static_cast<int64_t>(skiplist.length);
    if (total == 0) return result;

    if (start < 0) start = total + start;
    if (stop < 0) stop = total + stop;
    if (start < 0) start = 0;

    if (start > stop || start >= total) return result;
    if (stop >= total) stop = total - 1;

    uint32_t current_rank = static_cast<uint32_t>(start + 1);
    SkipListNode* node = skiplist.getNodeByRank(current_rank);
    int64_t count = stop - start + 1;

    while (node && count-- > 0) {
        result.emplace_back(node->member, node->score);
        node = node->level[0].forward;
    }

    return result;
}

std::vector<std::pair<std::string, double>> SortedSet::rangeByScore(
    double min_score, double max_score, 
    bool min_inclusive, bool max_inclusive,
    int64_t offset, int64_t count,
    bool with_scores) const {
    std::vector<std::pair<std::string, double>> result;
    if (skiplist.length == 0) return result;

    SkipListNode* node = skiplist.getFirstInRange(min_score, max_score, min_inclusive, max_inclusive);
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
