#include "server.h"
#include "common.h"
#include <sstream>
#include <iomanip>

using std::string;
using std::vector;
using std::shared_ptr;
using std::make_shared;
using std::optional;

namespace redis {

Server::Server(Config config)
    : config_(std::move(config)),
      eviction_(config_.eviction_policy),
      aof_(config_.aof_filename, config_.aof_fsync),
      loop_(config_.bind_ip, config_.port) {
    stats_.start_time_sec = unix_time_sec();
    loop_.set_idle_timeout(config_.client_timeout);
}

Server::~Server() {
    stop();
}

size_t Server::estimate_size(const string& key, const Value& value) {
    size_t sz = sizeof(Entry) + key.capacity();
    if (std::holds_alternative<string>(value)) {
        sz += std::get<string>(value).capacity();
    } else if (std::holds_alternative<shared_ptr<ZSet>>(value)) {
        auto zset = std::get<shared_ptr<ZSet>>(value);
        if (zset) {
            sz += sizeof(ZSet) + (zset->size() * (sizeof(SkipNode) + 64));
        }
    }
    return sz;
}

bool Server::del_key(const string& key) {
    Entry* entry = dict_.find(key);
    if (!entry) return false;

    size_t sz = estimate_size(entry->key, entry->value);
    eviction_.on_remove(entry);
    expire_.clear_expire(entry);

    bool ok = dict_.erase(key);
    if (ok && stats_.used_memory_bytes >= sz) {
        stats_.used_memory_bytes -= sz;
    }
    return ok;
}

void Server::evict_if_needed() {
    if (config_.maxmemory == 0) return;

    while (stats_.used_memory_bytes > config_.maxmemory) {
        string victim = eviction_.pick_victim(dict_);
        if (victim.empty()) break;
        del_key(victim);
    }
}

void Server::cron() {
    expire_.sample_expired(dict_, [this](const string& key) {
        del_key(key);
    }, 5);

    if (dict_.is_rehashing()) {
        dict_.rehash_ms(1);
    }

    if (config_.aof_enabled) {
        aof_.check_rewrite();
    }
}

void Server::setup_handlers() {
    loop_.on_command([this](Client& client, const vector<string>& args) {
        execute(client, args);
    });

    loop_.on_tick([this]() {
        cron();
    });
}

bool Server::start() {
    if (!loop_.init()) return false;

    setup_handlers();

    if (config_.aof_enabled) {
        aof_.load([this](const vector<string>& args) {
            if (args.empty()) return;
            Client dummy(INVALID_SOCK, "127.0.0.1", 0);
            execute(dummy, args);
        });

        if (!aof_.open()) return false;
    }

    running_ = true;
    loop_.run();
    return true;
}

void Server::stop() {
    if (!running_) return;
    running_ = false;
    loop_.stop();
    if (config_.aof_enabled) {
        aof_.close();
    }
}

void Server::execute(Client& client, const vector<string>& args) {
    if (args.empty()) return;

    stats_.total_commands_processed++;
    string cmd = to_upper(args[0]);

    if (cmd == "PING") cmd_ping(client, args);
    else if (cmd == "ECHO") cmd_echo(client, args);
    else if (cmd == "SET") cmd_set(client, args);
    else if (cmd == "GET") cmd_get(client, args);
    else if (cmd == "DEL") cmd_del(client, args);
    else if (cmd == "EXISTS") cmd_exists(client, args);
    else if (cmd == "EXPIRE") cmd_expire(client, args);
    else if (cmd == "TTL") cmd_ttl(client, args);
    else if (cmd == "ZADD") cmd_zadd(client, args);
    else if (cmd == "ZRANGE") cmd_zrange(client, args);
    else if (cmd == "ZRANGEBYSCORE") cmd_zrangebyscore(client, args);
    else if (cmd == "ZSCORE") cmd_zscore(client, args);
    else if (cmd == "ZCARD") cmd_zcard(client, args);
    else if (cmd == "BGREWRITEAOF") cmd_bgrewriteaof(client, args);
    else if (cmd == "INFO") cmd_info(client, args);
    else if (cmd == "COMMAND") cmd_command(client, args);
    else client.write(RespWriter::error("unknown command '" + args[0] + "'"));
}

void Server::cmd_ping(Client& client, const vector<string>& args) {
    if (args.size() == 1) {
        client.write(RespWriter::pong());
    } else if (args.size() == 2) {
        client.write(RespWriter::bulk(args[1]));
    } else {
        client.write(RespWriter::error("wrong number of arguments for 'ping' command"));
    }
}

void Server::cmd_echo(Client& client, const vector<string>& args) {
    if (args.size() != 2) {
        client.write(RespWriter::error("wrong number of arguments for 'echo' command"));
        return;
    }
    client.write(RespWriter::bulk(args[1]));
}

void Server::cmd_set(Client& client, const vector<string>& args) {
    if (args.size() < 3) {
        client.write(RespWriter::error("wrong number of arguments for 'set' command"));
        return;
    }

    const string& key = args[1];
    const string& val = args[2];
    optional<int64_t> ex_seconds;

    if (args.size() > 3) {
        for (size_t i = 3; i < args.size(); ++i) {
            string opt = to_upper(args[i]);
            if (opt == "EX" && i + 1 < args.size()) {
                auto parsed = parse_int(args[++i]);
                if (!parsed || *parsed <= 0) {
                    client.write(RespWriter::error("value is not an integer or out of range"));
                    return;
                }
                ex_seconds = *parsed;
            } else {
                client.write(RespWriter::error("syntax error"));
                return;
            }
        }
    }

    if (config_.maxmemory > 0 && config_.eviction_policy == EvictPolicy::NoEviction) {
        if (stats_.used_memory_bytes > config_.maxmemory) {
            client.write(RespWriter::error_with_code("OOM", "command not allowed when used memory > 'maxmemory'"));
            return;
        }
    }

    Entry* old_entry = dict_.find(key);
    if (old_entry) {
        del_key(key);
    }

    dict_.set(key, val);
    Entry* new_entry = dict_.find(key);
    if (new_entry) {
        stats_.used_memory_bytes += estimate_size(key, val);
        eviction_.on_insert(new_entry);

        if (ex_seconds.has_value()) {
            uint64_t expire_at = unix_time_ms() + (*ex_seconds * 1000);
            expire_.set_expire(new_entry, expire_at);
        } else {
            expire_.clear_expire(new_entry);
        }
    }

    evict_if_needed();

    if (config_.aof_enabled && client.fd != INVALID_SOCK) {
        aof_.append(args);
    }

    client.write(RespWriter::ok());
}

void Server::cmd_get(Client& client, const vector<string>& args) {
    if (args.size() != 2) {
        client.write(RespWriter::error("wrong number of arguments for 'get' command"));
        return;
    }

    const string& key = args[1];
    Entry* entry = dict_.find(key);

    if (!entry) {
        client.write(RespWriter::null_bulk());
        return;
    }

    // Passive lazy expiry
    if (expire_.is_expired(entry)) {
        del_key(key);
        client.write(RespWriter::null_bulk());
        return;
    }

    if (!entry->is_str()) {
        client.write(RespWriter::error_with_code("WRONGTYPE", "Operation against a key holding the wrong kind of value"));
        return;
    }

    eviction_.on_touch(entry);
    client.write(RespWriter::bulk(entry->str()));
}

void Server::cmd_del(Client& client, const vector<string>& args) {
    if (args.size() < 2) {
        client.write(RespWriter::error("wrong number of arguments for 'del' command"));
        return;
    }

    int64_t count = 0;
    for (size_t i = 1; i < args.size(); ++i) {
        if (del_key(args[i])) count++;
    }

    if (count > 0 && config_.aof_enabled && client.fd != INVALID_SOCK) {
        aof_.append(args);
    }

    client.write(RespWriter::integer(count));
}

void Server::cmd_exists(Client& client, const vector<string>& args) {
    if (args.size() < 2) {
        client.write(RespWriter::error("wrong number of arguments for 'exists' command"));
        return;
    }

    int64_t count = 0;
    for (size_t i = 1; i < args.size(); ++i) {
        Entry* entry = dict_.find(args[i]);
        if (entry) {
            if (expire_.is_expired(entry)) {
                del_key(args[i]);
            } else {
                count++;
            }
        }
    }

    client.write(RespWriter::integer(count));
}

void Server::cmd_expire(Client& client, const vector<string>& args) {
    if (args.size() != 3) {
        client.write(RespWriter::error("wrong number of arguments for 'expire' command"));
        return;
    }

    const string& key = args[1];
    auto sec = parse_int(args[2]);
    if (!sec) {
        client.write(RespWriter::error("value is not an integer or out of range"));
        return;
    }

    Entry* entry = dict_.find(key);
    if (!entry || expire_.is_expired(entry)) {
        if (entry) del_key(key);
        client.write(RespWriter::integer(0));
        return;
    }

    if (*sec <= 0) {
        del_key(key);
        if (config_.aof_enabled && client.fd != INVALID_SOCK) {
            aof_.append({"DEL", key});
        }
        client.write(RespWriter::integer(1));
        return;
    }

    uint64_t expire_at = unix_time_ms() + (*sec * 1000);
    expire_.set_expire(entry, expire_at);

    if (config_.aof_enabled && client.fd != INVALID_SOCK) {
        aof_.append(args);
    }

    client.write(RespWriter::integer(1));
}

void Server::cmd_ttl(Client& client, const vector<string>& args) {
    if (args.size() != 2) {
        client.write(RespWriter::error("wrong number of arguments for 'ttl' command"));
        return;
    }

    const string& key = args[1];
    Entry* entry = dict_.find(key);

    if (!entry) {
        client.write(RespWriter::integer(-2));
        return;
    }

    if (expire_.is_expired(entry)) {
        del_key(key);
        client.write(RespWriter::integer(-2));
        return;
    }

    client.write(RespWriter::integer(expire_.ttl_sec(entry)));
}

void Server::cmd_zadd(Client& client, const vector<string>& args) {
    if (args.size() != 4) {
        client.write(RespWriter::error("wrong number of arguments for 'zadd' command"));
        return;
    }

    const string& key = args[1];
    auto score = parse_double(args[2]);
    if (!score) {
        client.write(RespWriter::error("value is not a valid float"));
        return;
    }
    const string& member = args[3];

    Entry* entry = dict_.find(key);
    if (entry && expire_.is_expired(entry)) {
        del_key(key);
        entry = nullptr;
    }

    shared_ptr<ZSet> zset;
    if (!entry) {
        zset = make_shared<ZSet>();
        dict_.set(key, zset);
        entry = dict_.find(key);
        if (entry) {
            stats_.used_memory_bytes += estimate_size(key, zset);
            eviction_.on_insert(entry);
        }
    } else {
        if (!entry->is_zset()) {
            client.write(RespWriter::error_with_code("WRONGTYPE", "Operation against a key holding the wrong kind of value"));
            return;
        }
        zset = entry->zset();
        eviction_.on_touch(entry);
    }

    bool added = zset->add(*score, member);

    evict_if_needed();

    if (config_.aof_enabled && client.fd != INVALID_SOCK) {
        aof_.append(args);
    }

    client.write(RespWriter::integer(added ? 1 : 0));
}

void Server::cmd_zrange(Client& client, const vector<string>& args) {
    if (args.size() < 4) {
        client.write(RespWriter::error("wrong number of arguments for 'zrange' command"));
        return;
    }

    const string& key = args[1];
    auto start = parse_int(args[2]);
    auto stop = parse_int(args[3]);

    if (!start || !stop) {
        client.write(RespWriter::error("value is not an integer or out of range"));
        return;
    }

    bool with_scores = (args.size() == 5 && to_upper(args[4]) == "WITHSCORES");

    Entry* entry = dict_.find(key);
    if (!entry || expire_.is_expired(entry)) {
        if (entry) del_key(key);
        client.write(RespWriter::empty_array());
        return;
    }

    if (!entry->is_zset()) {
        client.write(RespWriter::error_with_code("WRONGTYPE", "Operation against a key holding the wrong kind of value"));
        return;
    }

    eviction_.on_touch(entry);
    auto zset = entry->zset();
    auto items = zset->range(*start, *stop, with_scores);

    vector<string> output;
    output.reserve(items.size() * (with_scores ? 2 : 1));
    for (const auto& [member, score] : items) {
        output.push_back(member);
        if (with_scores) {
            std::ostringstream ss;
            ss << std::setprecision(15) << score;
            output.push_back(ss.str());
        }
    }

    client.write(RespWriter::array(output));
}

void Server::cmd_zrangebyscore(Client& client, const vector<string>& args) {
    if (args.size() < 4) {
        client.write(RespWriter::error("wrong number of arguments for 'zrangebyscore' command"));
        return;
    }

    const string& key = args[1];
    auto min_score = parse_double(args[2]);
    auto max_score = parse_double(args[3]);

    if (!min_score || !max_score) {
        client.write(RespWriter::error("min or max is not a float"));
        return;
    }

    bool with_scores = false;
    for (size_t i = 4; i < args.size(); ++i) {
        if (to_upper(args[i]) == "WITHSCORES") with_scores = true;
    }

    Entry* entry = dict_.find(key);
    if (!entry || expire_.is_expired(entry)) {
        if (entry) del_key(key);
        client.write(RespWriter::empty_array());
        return;
    }

    if (!entry->is_zset()) {
        client.write(RespWriter::error_with_code("WRONGTYPE", "Operation against a key holding the wrong kind of value"));
        return;
    }

    eviction_.on_touch(entry);
    auto zset = entry->zset();
    auto items = zset->range_by_score(*min_score, *max_score, true, true, 0, -1, with_scores);

    vector<string> output;
    output.reserve(items.size() * (with_scores ? 2 : 1));
    for (const auto& [member, score] : items) {
        output.push_back(member);
        if (with_scores) {
            std::ostringstream ss;
            ss << std::setprecision(15) << score;
            output.push_back(ss.str());
        }
    }

    client.write(RespWriter::array(output));
}

void Server::cmd_zscore(Client& client, const vector<string>& args) {
    if (args.size() != 3) {
        client.write(RespWriter::error("wrong number of arguments for 'zscore' command"));
        return;
    }

    const string& key = args[1];
    const string& member = args[2];

    Entry* entry = dict_.find(key);
    if (!entry || expire_.is_expired(entry)) {
        if (entry) del_key(key);
        client.write(RespWriter::null_bulk());
        return;
    }

    if (!entry->is_zset()) {
        client.write(RespWriter::error_with_code("WRONGTYPE", "Operation against a key holding the wrong kind of value"));
        return;
    }

    eviction_.on_touch(entry);
    auto zset = entry->zset();
    auto score = zset->score_of(member);

    if (!score) {
        client.write(RespWriter::null_bulk());
        return;
    }

    std::ostringstream ss;
    ss << std::setprecision(15) << *score;
    client.write(RespWriter::bulk(ss.str()));
}

void Server::cmd_zcard(Client& client, const vector<string>& args) {
    if (args.size() != 2) {
        client.write(RespWriter::error("wrong number of arguments for 'zcard' command"));
        return;
    }

    const string& key = args[1];
    Entry* entry = dict_.find(key);

    if (!entry || expire_.is_expired(entry)) {
        if (entry) del_key(key);
        client.write(RespWriter::integer(0));
        return;
    }

    if (!entry->is_zset()) {
        client.write(RespWriter::error_with_code("WRONGTYPE", "Operation against a key holding the wrong kind of value"));
        return;
    }

    eviction_.on_touch(entry);
    auto zset = entry->zset();
    client.write(RespWriter::integer(static_cast<int64_t>(zset->size())));
}

void Server::cmd_bgrewriteaof(Client& client, const vector<string>& args) {
    (void)args;
    if (!config_.aof_enabled) {
        client.write(RespWriter::error("AOF is disabled"));
        return;
    }

    if (aof_.is_rewriting()) {
        client.write(RespWriter::error("Background append only file rewriting already in progress"));
        return;
    }

    if (aof_.rewrite_bg(dict_)) {
        client.write(RespWriter::status("Background append only file rewriting started"));
    } else {
        client.write(RespWriter::error("Unable to start background rewrite"));
    }
}

void Server::cmd_info(Client& client, const vector<string>& args) {
    (void)args;
    uint32_t uptime = unix_time_sec() - static_cast<uint32_t>(stats_.start_time_sec);

    std::ostringstream ss;
    ss << "# Server\r\n";
    ss << "redis_version:7.0.0-clone\r\n";
    ss << "uptime_in_seconds:" << uptime << "\r\n";
    ss << "# Clients\r\n";
    ss << "connected_clients:" << loop_.client_count() << "\r\n";
    ss << "# Memory\r\n";
    ss << "used_memory:" << stats_.used_memory_bytes.load() << "\r\n";
    ss << "maxmemory:" << config_.maxmemory << "\r\n";
    ss << "# Stats\r\n";
    ss << "total_commands_processed:" << stats_.total_commands_processed.load() << "\r\n";
    ss << "# Persistence\r\n";
    ss << "aof_enabled:" << (config_.aof_enabled ? 1 : 0) << "\r\n";
    ss << "aof_rewrite_in_progress:" << (aof_.is_rewriting() ? 1 : 0) << "\r\n";
    ss << "aof_current_size:" << aof_.size() << "\r\n";
    ss << "# Keyspace\r\n";
    ss << "db0:keys=" << dict_.size() << ",expires=" << expire_.size() << "\r\n";

    client.write(RespWriter::bulk(ss.str()));
}

void Server::cmd_command(Client& client, const vector<string>& args) {
    (void)args;
    client.write(RespWriter::empty_array());
}

}
