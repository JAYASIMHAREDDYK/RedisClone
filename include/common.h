#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <cstdint>
#include <optional>

namespace redis {

uint64_t unix_time_ms();
uint32_t unix_time_sec();
uint64_t monotonic_time_ms();

uint64_t murmur_hash64(const void* key, size_t len, uint64_t seed = 0xadc83b19ULL);

inline uint64_t hash_key(std::string_view key) {
    return murmur_hash64(key.data(), key.size());
}

void to_upper(std::string& s);
std::string to_upper(std::string_view s);

void to_lower(std::string& s);
std::string to_lower(std::string_view s);

std::optional<int64_t> parse_int(std::string_view s);
std::optional<double> parse_double(std::string_view s);

std::vector<std::string> split(std::string_view s, char delim);

}
