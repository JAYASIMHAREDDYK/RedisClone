#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <cstdint>

namespace redis {

enum class RespType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
    Null
};

class RespWriter {
public:
    static std::string status(std::string_view s);
    static std::string error(std::string_view msg);
    static std::string error_with_code(std::string_view code, std::string_view msg);
    static std::string integer(int64_t val);
    static std::string bulk(std::string_view s);
    static std::string null_bulk();
    static std::string null_array();
    static std::string array(const std::vector<std::string>& elements);
    static std::string empty_array();
    static std::string ok();
    static std::string pong();
};

class RespParser {
public:
    RespParser() = default;

    void feed(const char* data, size_t len);
    void feed(std::string_view s);

    bool next_command(std::vector<std::string>& args);

    size_t size() const { return buffer_.size(); }
    void clear() { buffer_.clear(); }

private:
    std::string buffer_;

    bool parse_array(std::vector<std::string>& args, size_t& consumed);
    bool parse_inline(std::vector<std::string>& args, size_t& consumed);
};

}
