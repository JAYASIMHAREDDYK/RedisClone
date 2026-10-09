#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <cstdint>
#include <optional>

namespace redis {

enum class RespType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
    Null
};

class RespEncoder {
public:
    static std::string simpleString(std::string_view s);
    static std::string error(std::string_view msg);
    static std::string customError(std::string_view prefix, std::string_view msg);
    static std::string integer(int64_t val);
    static std::string bulkString(std::string_view s);
    static std::string nullBulkString();
    static std::string nullArray();
    static std::string array(const std::vector<std::string>& elements);
    static std::string emptyArray();
    static std::string ok();
    static std::string pong();
};

class RespParser {
public:
    RespParser() = default;

    void feed(const char* data, size_t len);
    void feed(std::string_view s);

    bool hasCompleteCommand();
    bool nextCommand(std::vector<std::string>& args);

    size_t bufferSize() const { return buffer_.size(); }
    void clear() { buffer_.clear(); }

private:
    std::string buffer_;

    bool parseRespArray(std::vector<std::string>& args, size_t& consumed);
    bool parseInlineCommand(std::vector<std::string>& args, size_t& consumed);
};

}
