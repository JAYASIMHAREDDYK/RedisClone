#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <optional>
#include <cstdint>

namespace redis {

constexpr int SKIPLIST_MAXLEVEL = 32;
constexpr double SKIPLIST_P = 0.25;

struct SkipNode;

struct SkipLevel {
    SkipNode* forward{nullptr};
    uint32_t span{0};
};

struct SkipNode {
    std::string member;
    double score{0.0};
    SkipNode* backward{nullptr};
    std::vector<SkipLevel> level;

    SkipNode(int lvl, double s, std::string m);
};

class SkipList {
public:
    SkipNode* header{nullptr};
    SkipNode* tail{nullptr};
    size_t length{0};
    int max_level{1};

    SkipList();
    ~SkipList();

    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;
    SkipList(SkipList&& other) noexcept;
    SkipList& operator=(SkipList&& other) noexcept;

    SkipNode* insert(double score, const std::string& member);
    bool erase(double score, const std::string& member);
    uint32_t rank_of(double score, const std::string& member) const;
    SkipNode* node_at_rank(uint32_t rank) const;
    SkipNode* first_in_range(double min_score, double max_score, 
                             bool min_inclusive = true, bool max_inclusive = true) const;

    void clear();

private:
    int random_level();
};

class ZSet {
public:
    SkipList skiplist;
    std::unordered_map<std::string, double> dict;

    bool add(double score, const std::string& member);
    bool remove(const std::string& member);
    std::optional<double> score_of(const std::string& member) const;
    std::optional<uint32_t> rank_of(const std::string& member) const;
    size_t size() const { return dict.size(); }

    std::vector<std::pair<std::string, double>> range(int64_t start, int64_t stop, bool with_scores = false) const;
    std::vector<std::pair<std::string, double>> range_by_score(
        double min_score, double max_score, 
        bool min_inclusive = true, bool max_inclusive = true,
        int64_t offset = 0, int64_t count = -1,
        bool with_scores = false) const;
};

}
