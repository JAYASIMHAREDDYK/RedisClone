#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <optional>
#include <cstdint>

namespace redis {

constexpr int SKIPLIST_MAXLEVEL = 32;
constexpr double SKIPLIST_P = 0.25;

struct SkipListNode;

struct SkipListLevel {
    SkipListNode* forward{nullptr};
    uint32_t span{0};
};

struct SkipListNode {
    std::string member;
    double score{0.0};
    SkipListNode* backward{nullptr};
    std::vector<SkipListLevel> level;

    SkipListNode(int lvl, double s, std::string m);
};

class SkipList {
public:
    SkipListNode* header{nullptr};
    SkipListNode* tail{nullptr};
    size_t length{0};
    int max_level{1};

    SkipList();
    ~SkipList();

    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;
    SkipList(SkipList&& other) noexcept;
    SkipList& operator=(SkipList&& other) noexcept;

    SkipListNode* insert(double score, const std::string& member);
    bool erase(double score, const std::string& member);
    uint32_t getRank(double score, const std::string& member) const;
    SkipListNode* getNodeByRank(uint32_t rank) const;
    SkipListNode* getFirstInRange(double min_score, double max_score, 
                                  bool min_inclusive = true, bool max_inclusive = true) const;

    void clear();

private:
    int randomLevel();
    void freeNode(SkipListNode* node);
};

class SortedSet {
public:
    SkipList skiplist;
    std::unordered_map<std::string, double> dict;

    bool add(double score, const std::string& member);
    bool remove(const std::string& member);
    std::optional<double> getScore(const std::string& member) const;
    std::optional<uint32_t> getRank(const std::string& member) const;
    size_t size() const { return dict.size(); }

    std::vector<std::pair<std::string, double>> range(int64_t start, int64_t stop, bool with_scores = false) const;
    std::vector<std::pair<std::string, double>> rangeByScore(
        double min_score, double max_score, 
        bool min_inclusive = true, bool max_inclusive = true,
        int64_t offset = 0, int64_t count = -1,
        bool with_scores = false) const;
};

}
