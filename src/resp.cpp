#include "resp.h"
#include <charconv>

using std::string;
using std::string_view;
using std::vector;

namespace redis {

string RespWriter::status(string_view s) {
    string out;
    out.reserve(s.size() + 3);
    out.push_back('+');
    out.append(s);
    out.append("\r\n");
    return out;
}

string RespWriter::error(string_view msg) {
    string out;
    out.reserve(msg.size() + 7);
    out.append("-ERR ");
    out.append(msg);
    out.append("\r\n");
    return out;
}

string RespWriter::error_with_code(string_view code, string_view msg) {
    string out;
    out.reserve(code.size() + msg.size() + 4);
    out.push_back('-');
    out.append(code);
    out.push_back(' ');
    out.append(msg);
    out.append("\r\n");
    return out;
}

string RespWriter::integer(int64_t val) {
    string out;
    out.push_back(':');
    out.append(std::to_string(val));
    out.append("\r\n");
    return out;
}

string RespWriter::bulk(string_view s) {
    string out;
    string len_str = std::to_string(s.size());
    out.reserve(1 + len_str.size() + 2 + s.size() + 2);
    out.push_back('$');
    out.append(len_str);
    out.append("\r\n");
    out.append(s);
    out.append("\r\n");
    return out;
}

string RespWriter::null_bulk() {
    return "$-1\r\n";
}

string RespWriter::null_array() {
    return "*-1\r\n";
}

string RespWriter::array(const vector<string>& elements) {
    string out;
    string count_str = std::to_string(elements.size());
    out.push_back('*');
    out.append(count_str);
    out.append("\r\n");

    for (const auto& elem : elements) {
        out.append(bulk(elem));
    }
    return out;
}

string RespWriter::empty_array() {
    return "*0\r\n";
}

string RespWriter::ok() {
    return "+OK\r\n";
}

string RespWriter::pong() {
    return "+PONG\r\n";
}

void RespParser::feed(const char* data, size_t len) {
    buffer_.append(data, len);
}

void RespParser::feed(string_view s) {
    buffer_.append(s);
}

bool RespParser::next_command(vector<string>& args) {
    args.clear();
    if (buffer_.empty()) return false;

    size_t consumed = 0;
    bool ok = false;

    if (buffer_[0] == '*') {
        ok = parse_array(args, consumed);
    } else {
        ok = parse_inline(args, consumed);
    }

    if (ok && consumed > 0) {
        buffer_.erase(0, consumed);
        return true;
    }

    return false;
}

bool RespParser::parse_array(vector<string>& args, size_t& consumed) {
    consumed = 0;
    size_t crlf = buffer_.find("\r\n");
    if (crlf == string::npos) {
        return false;
    }

    int64_t count = 0;
    auto [p, ec] = std::from_chars(buffer_.data() + 1, buffer_.data() + crlf, count);
    if (ec != std::errc() || count < 0) {
        return false;
    }

    size_t cur = crlf + 2;
    vector<string> parsed;
    parsed.reserve(count);

    for (int64_t i = 0; i < count; ++i) {
        if (cur >= buffer_.size() || buffer_[cur] != '$') {
            return false;
        }

        size_t next_crlf = buffer_.find("\r\n", cur);
        if (next_crlf == string::npos) {
            return false;
        }

        int64_t bulk_len = 0;
        auto [bp, bec] = std::from_chars(buffer_.data() + cur + 1, buffer_.data() + next_crlf, bulk_len);
        if (bec != std::errc() || bulk_len < 0) {
            return false;
        }

        size_t data_start = next_crlf + 2;
        size_t data_end = data_start + bulk_len;

        if (data_end + 2 > buffer_.size() || buffer_[data_end] != '\r' || buffer_[data_end + 1] != '\n') {
            return false;
        }

        parsed.emplace_back(buffer_.substr(data_start, bulk_len));
        cur = data_end + 2;
    }

    consumed = cur;
    args = std::move(parsed);
    return true;
}

bool RespParser::parse_inline(vector<string>& args, size_t& consumed) {
    consumed = 0;
    size_t newline = buffer_.find('\n');
    if (newline == string::npos) {
        return false;
    }

    size_t line_len = newline;
    if (line_len > 0 && buffer_[line_len - 1] == '\r') {
        line_len--;
    }

    string_view line(buffer_.data(), line_len);
    consumed = newline + 1;

    vector<string> parsed;
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
