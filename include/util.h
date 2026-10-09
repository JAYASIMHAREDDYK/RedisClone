#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <cstdint>
#include <optional>

namespace redis {

uint64_t getUnixTimeMs();
uint32_t getUnixTimeSec();
uint64_t getMonotonicTimeMs();

uint64_t murmurHash64(const void* key, size_t len, uint64_t seed = 0xadc83b19ULL);

inline uint64_t hashKey(std::string_view key) {
    return murmurHash64(key.data(), key.size());
}

void toUpper(std::string& s);
std::string toUpper(std::string_view s);

std::optional<int64_t> parseInteger(std::string_view s);
std::optional<double> parseDouble(std::string_view s);

std::vector<std::string> split(std::string_view s, char delim);

}
