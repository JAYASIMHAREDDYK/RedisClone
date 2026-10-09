#include "server.h"
#include "util.h"
#include <sstream>
#include <iostream>
#include <iomanip>

namespace redis {

Server::Server(ServerConfig config)
    : config_(std::move(config)),
      eviction_(config_.eviction_policy),
      aof_(config_.aof_filename, config_.aof_fsync),
      reactor_(config_.bind_ip, config_.port) {
    stats_.start_time_sec = getUnixTimeSec();
    reactor_.setIdleTimeout(config_.client_timeout);
}

Server::~Server() {
    stop();
}

size_t Server::estimateEntrySize(const std::string& key, const DictValue& value) {
    size_t sz = sizeof(DictEntry) + key.capacity();
    if (std::holds_alternative<std::string>(value)) {
        sz += std::get<std::string>(value).capacity();
    } else if (std::holds_alternative<std::shared_ptr<SortedSet>>(value)) {
        auto zset = std::get<std::shared_ptr<SortedSet>>(value);
        if (zset) {
            sz += sizeof(SortedSet) + (zset->size() * (sizeof(SkipListNode) + 64));
        }
    }
    return sz;
}

bool Server::deleteKeyInternal(const std::string& key) {
    DictEntry* entry = dict_.find(key);
    if (!entry) return false;

    size_t sz = estimateEntrySize(entry->key, entry->value);
    eviction_.onKeyRemoved(entry);
    expire_.clearExpire(entry);

    bool ok = dict_.erase(key);
    if (ok && stats_.used_memory_bytes >= sz) {
        stats_.used_memory_bytes -= sz;
    }
    return ok;
}

void Server::checkEviction() {
    if (config_.maxmemory == 0) return;

    while (stats_.used_memory_bytes > config_.maxmemory) {
        std::string candidate = eviction_.selectEvictionCandidate(dict_);
        if (candidate.empty()) {
            break;
        }
        deleteKeyInternal(candidate);
    }
}

void Server::handlePeriodicTasks() {
    expire_.activeExpireCycle(dict_, [this](const std::string& key) {
        deleteKeyInternal(key);
    }, 5);

    if (dict_.isRehashing()) {
        dict_.rehashMilliseconds(1);
    }

    if (config_.aof_enabled) {
        aof_.checkBackgroundRewriteStatus();
    }
}

void Server::registerHandlers() {
    reactor_.setCommandHandler([this](ClientConnection& client, const std::vector<std::string>& args) {
        executeCommand(client, args);
    });

    reactor_.setPeriodicHandler([this]() {
        handlePeriodicTasks();
    });
}

bool Server::start() {
    if (!reactor_.init()) {
        return false;
    }

    registerHandlers();

    if (config_.aof_enabled) {
        aof_.loadAof([this](const std::vector<std::string>& args) {
            if (args.empty()) return;
            ClientConnection dummy(INVALID_SOCK, "127.0.0.1", 0);
            executeCommand(dummy, args);
        });

        if (!aof_.open()) {
            return false;
        }
    }

    running_ = true;
    reactor_.run();
    return true;
}

void Server::stop() {
    if (!running_) return;
    running_ = false;
    reactor_.stop();
    if (config_.aof_enabled) {
        aof_.close();
    }
}

void Server::executeCommand(ClientConnection& client, const std::vector<std::string>& args) {
    if (args.empty()) return;

    stats_.total_commands_processed++;

    std::string cmd = toUpper(args[0]);

    if (cmd == "PING") {
        cmdPing(client, args);
    } else if (cmd == "ECHO") {
        cmdEcho(client, args);
    } else if (cmd == "SET") {
        cmdSet(client, args);
    } else if (cmd == "GET") {
        cmdGet(client, args);
    } else if (cmd == "DEL") {
        cmdDel(client, args);
    } else if (cmd == "EXISTS") {
        cmdExists(client, args);
    } else if (cmd == "EXPIRE") {
        cmdExpire(client, args);
    } else if (cmd == "TTL") {
        cmdTtl(client, args);
    } else if (cmd == "ZADD") {
        cmdZAdd(client, args);
    } else if (cmd == "ZRANGE") {
        cmdZRange(client, args);
    } else if (cmd == "ZRANGEBYSCORE") {
        cmdZRangeByScore(client, args);
    } else if (cmd == "ZSCORE") {
        cmdZScore(client, args);
    } else if (cmd == "ZCARD") {
        cmdZCard(client, args);
    } else if (cmd == "BGREWRITEAOF") {
        cmdBgRewriteAof(client, args);
    } else if (cmd == "INFO") {
        cmdInfo(client, args);
    } else if (cmd == "COMMAND") {
        cmdCommand(client, args);
    } else {
        client.appendWrite(RespEncoder::error("unknown command '" + args[0] + "'"));
    }
}

void Server::cmdPing(ClientConnection& client, const std::vector<std::string>& args) {
    if (args.size() == 1) {
        client.appendWrite(RespEncoder::pong());
    } else if (args.size() == 2) {
        client.appendWrite(RespEncoder::bulkString(args[1]));
    } else {
        client.appendWrite(RespEncoder::error("wrong number of arguments for 'ping' command"));
    }
}

void Server::cmdEcho(ClientConnection& client, const std::vector<std::string>& args) {
    if (args.size() != 2) {
        client.appendWrite(RespEncoder::error("wrong number of arguments for 'echo' command"));
        return;
    }
    client.appendWrite(RespEncoder::bulkString(args[1]));
}

void Server::cmdSet(ClientConnection& client, const std::vector<std::string>& args) {
    if (args.size() < 3) {
        client.appendWrite(RespEncoder::error("wrong number of arguments for 'set' command"));
        return;
    }

    const std::string& key = args[1];
    const std::string& val = args[2];
    std::optional<int64_t> ex_seconds;

    if (args.size() > 3) {
        for (size_t i = 3; i < args.size(); ++i) {
            std::string opt = toUpper(args[i]);
            if (opt == "EX" && i + 1 < args.size()) {
                auto parsed = parseInteger(args[++i]);
                if (!parsed || *parsed <= 0) {
                    client.appendWrite(RespEncoder::error("value is not an integer or out of range"));
                    return;
                }
                ex_seconds = *parsed;
            } else {
                client.appendWrite(RespEncoder::error("syntax error"));
                return;
            }
        }
    }

    if (config_.maxmemory > 0 && config_.eviction_policy == EvictionPolicy::NoEviction) {
        if (stats_.used_memory_bytes > config_.maxmemory) {
            client.appendWrite(RespEncoder::customError("OOM", "command not allowed when used memory > 'maxmemory'"));
            return;
        }
    }

    DictEntry* old_entry = dict_.find(key);
    if (old_entry) {
        deleteKeyInternal(key);
    }

    dict_.set(key, val);
    DictEntry* new_entry = dict_.find(key);
    if (new_entry) {
        stats_.used_memory_bytes += estimateEntrySize(key, val);
        eviction_.onKeyInserted(new_entry);

        if (ex_seconds.has_value()) {
            uint64_t expire_at = getUnixTimeMs() + (*ex_seconds * 1000);
            expire_.setExpire(new_entry, expire_at);
        } else {
            expire_.clearExpire(new_entry);
        }
    }

    checkEviction();

    if (config_.aof_enabled && client.fd != INVALID_SOCK) {
        aof_.appendCommand(args);
    }

    client.appendWrite(RespEncoder::ok());
}

void Server::cmdGet(ClientConnection& client, const std::vector<std::string>& args) {
    if (args.size() != 2) {
        client.appendWrite(RespEncoder::error("wrong number of arguments for 'get' command"));
        return;
    }

    const std::string& key = args[1];
    DictEntry* entry = dict_.find(key);

    if (!entry) {
        client.appendWrite(RespEncoder::nullBulkString());
        return;
    }

    if (expire_.isExpired(entry)) {
        deleteKeyInternal(key);
        client.appendWrite(RespEncoder::nullBulkString());
        return;
    }

    if (!entry->isString()) {
        client.appendWrite(RespEncoder::customError("WRONGTYPE", "Operation against a key holding the wrong kind of value"));
        return;
    }

    eviction_.onKeyAccessed(entry);
    client.appendWrite(RespEncoder::bulkString(entry->getString()));
}

void Server::cmdDel(ClientConnection& client, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        client.appendWrite(RespEncoder::error("wrong number of arguments for 'del' command"));
        return;
    }

    int64_t count = 0;
    for (size_t i = 1; i < args.size(); ++i) {
        if (deleteKeyInternal(args[i])) {
            count++;
        }
    }

    if (count > 0 && config_.aof_enabled && client.fd != INVALID_SOCK) {
        aof_.appendCommand(args);
    }

    client.appendWrite(RespEncoder::integer(count));
}

void Server::cmdExists(ClientConnection& client, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        client.appendWrite(RespEncoder::error("wrong number of arguments for 'exists' command"));
        return;
    }

    int64_t count = 0;
    for (size_t i = 1; i < args.size(); ++i) {
        DictEntry* entry = dict_.find(args[i]);
        if (entry) {
            if (expire_.isExpired(entry)) {
                deleteKeyInternal(args[i]);
            } else {
                count++;
            }
        }
    }

    client.appendWrite(RespEncoder::integer(count));
}

void Server::cmdExpire(ClientConnection& client, const std::vector<std::string>& args) {
    if (args.size() != 3) {
        client.appendWrite(RespEncoder::error("wrong number of arguments for 'expire' command"));
        return;
    }

    const std::string& key = args[1];
    auto sec = parseInteger(args[2]);
    if (!sec) {
        client.appendWrite(RespEncoder::error("value is not an integer or out of range"));
        return;
    }

    DictEntry* entry = dict_.find(key);
    if (!entry || expire_.isExpired(entry)) {
        if (entry) deleteKeyInternal(key);
        client.appendWrite(RespEncoder::integer(0));
        return;
    }

    if (*sec <= 0) {
        deleteKeyInternal(key);
        if (config_.aof_enabled && client.fd != INVALID_SOCK) {
            aof_.appendCommand({"DEL", key});
        }
        client.appendWrite(RespEncoder::integer(1));
        return;
    }

    uint64_t expire_at = getUnixTimeMs() + (*sec * 1000);
    expire_.setExpire(entry, expire_at);

    if (config_.aof_enabled && client.fd != INVALID_SOCK) {
        aof_.appendCommand(args);
    }

    client.appendWrite(RespEncoder::integer(1));
}

void Server::cmdTtl(ClientConnection& client, const std::vector<std::string>& args) {
    if (args.size() != 2) {
        client.appendWrite(RespEncoder::error("wrong number of arguments for 'ttl' command"));
        return;
    }

    const std::string& key = args[1];
    DictEntry* entry = dict_.find(key);

    if (!entry) {
        client.appendWrite(RespEncoder::integer(-2));
        return;
    }

    if (expire_.isExpired(entry)) {
        deleteKeyInternal(key);
        client.appendWrite(RespEncoder::integer(-2));
        return;
    }

    int64_t ttl = expire_.getTtlSeconds(entry);
    client.appendWrite(RespEncoder::integer(ttl));
}

void Server::cmdZAdd(ClientConnection& client, const std::vector<std::string>& args) {
    if (args.size() != 4) {
        client.appendWrite(RespEncoder::error("wrong number of arguments for 'zadd' command"));
        return;
    }

    const std::string& key = args[1];
    auto score = parseDouble(args[2]);
    if (!score) {
        client.appendWrite(RespEncoder::error("value is not a valid float"));
        return;
    }
    const std::string& member = args[3];

    DictEntry* entry = dict_.find(key);
    if (entry && expire_.isExpired(entry)) {
        deleteKeyInternal(key);
        entry = nullptr;
    }

    std::shared_ptr<SortedSet> zset;
    if (!entry) {
        zset = std::make_shared<SortedSet>();
        dict_.set(key, zset);
        entry = dict_.find(key);
        if (entry) {
            stats_.used_memory_bytes += estimateEntrySize(key, zset);
            eviction_.onKeyInserted(entry);
        }
    } else {
        if (!entry->isZSet()) {
            client.appendWrite(RespEncoder::customError("WRONGTYPE", "Operation against a key holding the wrong kind of value"));
            return;
        }
        zset = entry->getZSet();
        eviction_.onKeyAccessed(entry);
    }

    bool added = zset->add(*score, member);

    checkEviction();

    if (config_.aof_enabled && client.fd != INVALID_SOCK) {
        aof_.appendCommand(args);
    }

    client.appendWrite(RespEncoder::integer(added ? 1 : 0));
}

void Server::cmdZRange(ClientConnection& client, const std::vector<std::string>& args) {
    if (args.size() < 4) {
        client.appendWrite(RespEncoder::error("wrong number of arguments for 'zrange' command"));
        return;
    }

    const std::string& key = args[1];
    auto start = parseInteger(args[2]);
    auto stop = parseInteger(args[3]);

    if (!start || !stop) {
        client.appendWrite(RespEncoder::error("value is not an integer or out of range"));
        return;
    }

    bool with_scores = false;
    if (args.size() == 5 && toUpper(args[4]) == "WITHSCORES") {
        with_scores = true;
    }

    DictEntry* entry = dict_.find(key);
    if (!entry || expire_.isExpired(entry)) {
        if (entry) deleteKeyInternal(key);
        client.appendWrite(RespEncoder::emptyArray());
        return;
    }

    if (!entry->isZSet()) {
        client.appendWrite(RespEncoder::customError("WRONGTYPE", "Operation against a key holding the wrong kind of value"));
        return;
    }

    eviction_.onKeyAccessed(entry);
    auto zset = entry->getZSet();
    auto items = zset->range(*start, *stop, with_scores);

    std::vector<std::string> output;
    output.reserve(items.size() * (with_scores ? 2 : 1));
    for (const auto& [member, score] : items) {
        output.push_back(member);
        if (with_scores) {
            std::ostringstream ss;
            ss << std::setprecision(15) << score;
            output.push_back(ss.str());
        }
    }

    client.appendWrite(RespEncoder::array(output));
}

void Server::cmdZRangeByScore(ClientConnection& client, const std::vector<std::string>& args) {
    if (args.size() < 4) {
        client.appendWrite(RespEncoder::error("wrong number of arguments for 'zrangebyscore' command"));
        return;
    }

    const std::string& key = args[1];
    auto min_score = parseDouble(args[2]);
    auto max_score = parseDouble(args[3]);

    if (!min_score || !max_score) {
        client.appendWrite(RespEncoder::error("min or max is not a float"));
        return;
    }

    bool with_scores = false;
    for (size_t i = 4; i < args.size(); ++i) {
        if (toUpper(args[i]) == "WITHSCORES") {
            with_scores = true;
        }
    }

    DictEntry* entry = dict_.find(key);
    if (!entry || expire_.isExpired(entry)) {
        if (entry) deleteKeyInternal(key);
        client.appendWrite(RespEncoder::emptyArray());
        return;
    }

    if (!entry->isZSet()) {
        client.appendWrite(RespEncoder::customError("WRONGTYPE", "Operation against a key holding the wrong kind of value"));
        return;
    }

    eviction_.onKeyAccessed(entry);
    auto zset = entry->getZSet();
    auto items = zset->rangeByScore(*min_score, *max_score, true, true, 0, -1, with_scores);

    std::vector<std::string> output;
    output.reserve(items.size() * (with_scores ? 2 : 1));
    for (const auto& [member, score] : items) {
        output.push_back(member);
        if (with_scores) {
            std::ostringstream ss;
            ss << std::setprecision(15) << score;
            output.push_back(ss.str());
        }
    }

    client.appendWrite(RespEncoder::array(output));
}

void Server::cmdZScore(ClientConnection& client, const std::vector<std::string>& args) {
    if (args.size() != 3) {
        client.appendWrite(RespEncoder::error("wrong number of arguments for 'zscore' command"));
        return;
    }

    const std::string& key = args[1];
    const std::string& member = args[2];

    DictEntry* entry = dict_.find(key);
    if (!entry || expire_.isExpired(entry)) {
        if (entry) deleteKeyInternal(key);
        client.appendWrite(RespEncoder::nullBulkString());
        return;
    }

    if (!entry->isZSet()) {
        client.appendWrite(RespEncoder::customError("WRONGTYPE", "Operation against a key holding the wrong kind of value"));
        return;
    }

    eviction_.onKeyAccessed(entry);
    auto zset = entry->getZSet();
    auto score = zset->getScore(member);

    if (!score) {
        client.appendWrite(RespEncoder::nullBulkString());
        return;
    }

    std::ostringstream ss;
    ss << std::setprecision(15) << *score;
    client.appendWrite(RespEncoder::bulkString(ss.str()));
}

void Server::cmdZCard(ClientConnection& client, const std::vector<std::string>& args) {
    if (args.size() != 2) {
        client.appendWrite(RespEncoder::error("wrong number of arguments for 'zcard' command"));
        return;
    }

    const std::string& key = args[1];
    DictEntry* entry = dict_.find(key);

    if (!entry || expire_.isExpired(entry)) {
        if (entry) deleteKeyInternal(key);
        client.appendWrite(RespEncoder::integer(0));
        return;
    }

    if (!entry->isZSet()) {
        client.appendWrite(RespEncoder::customError("WRONGTYPE", "Operation against a key holding the wrong kind of value"));
        return;
    }

    eviction_.onKeyAccessed(entry);
    auto zset = entry->getZSet();
    client.appendWrite(RespEncoder::integer(static_cast<int64_t>(zset->size())));
}

void Server::cmdBgRewriteAof(ClientConnection& client, const std::vector<std::string>& args) {
    (void)args;
    if (!config_.aof_enabled) {
        client.appendWrite(RespEncoder::error("AOF is disabled"));
        return;
    }

    if (aof_.isRewriteInProgress()) {
        client.appendWrite(RespEncoder::error("Background append only file rewriting already in progress"));
        return;
    }

    bool started = aof_.startBackgroundRewrite(dict_);
    if (started) {
        client.appendWrite(RespEncoder::simpleString("Background append only file rewriting started"));
    } else {
        client.appendWrite(RespEncoder::error("Unable to start background rewrite"));
    }
}

void Server::cmdInfo(ClientConnection& client, const std::vector<std::string>& args) {
    (void)args;
    uint32_t uptime = getUnixTimeSec() - static_cast<uint32_t>(stats_.start_time_sec);

    std::ostringstream ss;
    ss << "# Server\r\n";
    ss << "redis_version:7.0.0-clone\r\n";
    ss << "uptime_in_seconds:" << uptime << "\r\n";
    ss << "# Clients\r\n";
    ss << "connected_clients:" << reactor_.activeClientsCount() << "\r\n";
    ss << "# Memory\r\n";
    ss << "used_memory:" << stats_.used_memory_bytes.load() << "\r\n";
    ss << "maxmemory:" << config_.maxmemory << "\r\n";
    ss << "# Stats\r\n";
    ss << "total_commands_processed:" << stats_.total_commands_processed.load() << "\r\n";
    ss << "# Persistence\r\n";
    ss << "aof_enabled:" << (config_.aof_enabled ? 1 : 0) << "\r\n";
    ss << "aof_rewrite_in_progress:" << (aof_.isRewriteInProgress() ? 1 : 0) << "\r\n";
    ss << "aof_current_size:" << aof_.getFileSize() << "\r\n";
    ss << "# Keyspace\r\n";
    ss << "db0:keys=" << dict_.size() << ",expires=" << expire_.expiringKeysCount() << "\r\n";

    client.appendWrite(RespEncoder::bulkString(ss.str()));
}

void Server::cmdCommand(ClientConnection& client, const std::vector<std::string>& args) {
    (void)args;
    client.appendWrite(RespEncoder::emptyArray());
}

}
