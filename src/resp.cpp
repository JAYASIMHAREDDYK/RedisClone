#include "resp.h"
#include <charconv>
#include <sstream>

namespace redis {

std::string RespEncoder::simpleString(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 3);
    out.push_back('+');
    out.append(s);
    out.append("\r\n");
    return out;
}

std::string RespEncoder::error(std::string_view msg) {
    std::string out;
    out.reserve(msg.size() + 7);
    out.append("-ERR ");
    out.append(msg);
    out.append("\r\n");
    return out;
}

std::string RespEncoder::customError(std::string_view prefix, std::string_view msg) {
    std::string out;
    out.reserve(prefix.size() + msg.size() + 4);
    out.push_back('-');
    out.append(prefix);
    out.push_back(' ');
    out.append(msg);
    out.append("\r\n");
    return out;
}

std::string RespEncoder::integer(int64_t val) {
    std::string out;
    out.push_back(':');
    out.append(std::to_string(val));
    out.append("\r\n");
    return out;
}

std::string RespEncoder::bulkString(std::string_view s) {
    std::string out;
    std::string len_str = std::to_string(s.size());
    out.reserve(1 + len_str.size() + 2 + s.size() + 2);
    out.push_back('$');
    out.append(len_str);
    out.append("\r\n");
    out.append(s);
    out.append("\r\n");
    return out;
}

std::string RespEncoder::nullBulkString() {
    return "$-1\r\n";
}

std::string RespEncoder::nullArray() {
    return "*-1\r\n";
}

std::string RespEncoder::array(const std::vector<std::string>& elements) {
    std::string out;
    std::string count_str = std::to_string(elements.size());
    out.push_back('*');
    out.append(count_str);
    out.append("\r\n");

    for (const auto& elem : elements) {
        out.append(bulkString(elem));
    }
    return out;
}

std::string RespEncoder::emptyArray() {
    return "*0\r\n";
}

std::string RespEncoder::ok() {
    return "+OK\r\n";
}

std::string RespEncoder::pong() {
    return "+PONG\r\n";
}

void RespParser::feed(const char* data, size_t len) {
    buffer_.append(data, len);
}

void RespParser::feed(std::string_view s) {
    buffer_.append(s);
}

bool RespParser::hasCompleteCommand() {
    std::vector<std::string> dummy;
    size_t consumed = 0;
    if (buffer_.empty()) return false;

    if (buffer_[0] == '*') {
        return parseRespArray(dummy, consumed);
    }
    return parseInlineCommand(dummy, consumed);
}

bool RespParser::nextCommand(std::vector<std::string>& args) {
    args.clear();
    if (buffer_.empty()) return false;

    size_t consumed = 0;
    bool ok = false;

    if (buffer_[0] == '*') {
        ok = parseRespArray(args, consumed);
    } else {
        ok = parseInlineCommand(args, consumed);
    }

    if (ok && consumed > 0) {
        buffer_.erase(0, consumed);
        return true;
    }

    return false;
}

bool RespParser::parseRespArray(std::vector<std::string>& args, size_t& consumed) {
    consumed = 0;
    size_t crlf_pos = buffer_.find("\r\n");
    if (crlf_pos == std::string::npos) {
        return false;
    }

    int64_t element_count = 0;
    auto [p, ec] = std::from_chars(buffer_.data() + 1, buffer_.data() + crlf_pos, element_count);
    if (ec != std::errc() || element_count < 0) {
        return false;
    }

    size_t current_pos = crlf_pos + 2;
    std::vector<std::string> parsed;
    parsed.reserve(element_count);

    for (int64_t i = 0; i < element_count; ++i) {
        if (current_pos >= buffer_.size()) return false;

        if (buffer_[current_pos] != '$') {
            return false;
        }

        size_t next_crlf = buffer_.find("\r\n", current_pos);
        if (next_crlf == std::string::npos) {
            return false;
        }

        int64_t bulk_len = 0;
        auto [bp, bec] = std::from_chars(buffer_.data() + current_pos + 1, buffer_.data() + next_crlf, bulk_len);
        if (bec != std::errc() || bulk_len < 0) {
            return false;
        }

        size_t data_start = next_crlf + 2;
        size_t data_end = data_start + bulk_len;

        if (data_end + 2 > buffer_.size()) {
            return false;
        }

        if (buffer_[data_end] != '\r' || buffer_[data_end + 1] != '\n') {
            return false;
        }

        parsed.emplace_back(buffer_.substr(data_start, bulk_len));
        current_pos = data_end + 2;
    }

    consumed = current_pos;
    args = std::move(parsed);
    return true;
}

bool RespParser::parseInlineCommand(std::vector<std::string>& args, size_t& consumed) {
    consumed = 0;
    size_t newline = buffer_.find('\n');
    if (newline == std::string::npos) {
        return false;
    }

    size_t line_len = newline;
    if (line_len > 0 && buffer_[line_len - 1] == '\r') {
        line_len--;
    }

    std::string_view line(buffer_.data(), line_len);
    consumed = newline + 1;

    std::vector<std::string> parsed;
    size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) {
            i++;
        }
        if (i >= line.size()) break;

        if (line[i] == '"' || line[i] == '\'') {
            char quote = line[i++];
            size_t start = i;
            while (i < line.size() && line[i] != quote) {
                i++;
            }
            parsed.emplace_back(line.substr(start, i - start));
            if (i < line.size()) i++;
        } else {
            size_t start = i;
            while (i < line.size() && line[i] != ' ' && line[i] != '\t') {
                i++;
            }
            parsed.emplace_back(line.substr(start, i - start));
        }
    }

    if (parsed.empty()) {
        return false;
    }

    args = std::move(parsed);
    return true;
}

}
