#include "common.h"
#include <chrono>
#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstring>

using std::string;
using std::string_view;
using std::vector;
using std::optional;

namespace redis {

uint64_t unix_time_ms() {
    auto now = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

uint32_t unix_time_sec() {
    auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<uint32_t>(std::chrono::duration_cast<std::chrono::seconds>(now).count());
}

uint64_t monotonic_time_ms() {
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

uint64_t murmur_hash64(const void* key, size_t len, uint64_t seed) {
    const uint64_t m = 0xc6a4a7935bd1e995ULL;
    const int r = 47;

    uint64_t h = seed ^ (len * m);
    const auto* data = static_cast<const uint64_t*>(key);
    const auto* end = data + (len / 8);

    while (data != end) {
        uint64_t k;
        std::memcpy(&k, data, sizeof(uint64_t));
        data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const auto* data2 = reinterpret_cast<const unsigned char*>(data);

    // Handle tail bytes when len is not a multiple of 8
    switch (len & 7) {
        case 7: h ^= static_cast<uint64_t>(data2[6]) << 48; [[fallthrough]];
        case 6: h ^= static_cast<uint64_t>(data2[5]) << 40; [[fallthrough]];
        case 5: h ^= static_cast<uint64_t>(data2[4]) << 32; [[fallthrough]];
        case 4: h ^= static_cast<uint64_t>(data2[3]) << 24; [[fallthrough]];
        case 3: h ^= static_cast<uint64_t>(data2[2]) << 16; [[fallthrough]];
        case 2: h ^= static_cast<uint64_t>(data2[1]) << 8;  [[fallthrough]];
        case 1: h ^= static_cast<uint64_t>(data2[0]);
                h *= m;
    }

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

void to_upper(string& s) {
    for (char& c : s) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
}

string to_upper(string_view s) {
    string res(s);
    to_upper(res);
    return res;
}

void to_lower(string& s) {
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
}

string to_lower(string_view s) {
    string res(s);
    to_lower(res);
    return res;
}

optional<int64_t> parse_int(string_view s) {
    if (s.empty()) return std::nullopt;
    int64_t val = 0;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), val);
    if (ec == std::errc() && ptr == s.data() + s.size()) {
        return val;
    }
    return std::nullopt;
}

optional<double> parse_double(string_view s) {
    if (s.empty()) return std::nullopt;
    string str(s);
    char* endptr = nullptr;
    double val = std::strtod(str.c_str(), &endptr);
    if (endptr == str.c_str() + str.size()) {
        return val;
    }
    return std::nullopt;
}

vector<string> split(string_view s, char delim) {
    vector<string> result;
    size_t start = 0;
    while (start < s.size()) {
        size_t end = s.find(delim, start);
        if (end == string_view::npos) {
            result.emplace_back(s.substr(start));
            break;
        }
        result.emplace_back(s.substr(start, end - start));
        start = end + 1;
    }
    return result;
}

}
