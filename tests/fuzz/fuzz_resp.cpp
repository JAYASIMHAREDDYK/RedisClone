#include "resp.h"
#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size == 0) return 0;

    redis::RespParser parser;
    parser.feed(reinterpret_cast<const char*>(data), size);

    std::vector<std::string> args;
    // Limit iterations to prevent infinite loops from hanging the fuzzer
    int iterations = 0;
    while (parser.next_command(args) && ++iterations < 1000) {
        // Successfully parsed a command frame
    }

    return 0;
}
