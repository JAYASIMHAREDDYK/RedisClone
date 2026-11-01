#include "server.h"
#include "common.h"
#include <sstream>
#include <iomanip>
#include <cstring>
#include <charconv>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#endif

using std::string;
using std::string_view;
using std::vector;
using std::shared_ptr;
using std::make_shared;
using std::optional;

namespace redis {

static void close_sock(socket_t s) {
    if (s != INVALID_SOCK_REPL) {
#ifdef _WIN32
        closesocket(s);
#else
        ::close(s);
#endif
    }
}

Server::Server(Config config)
    : config_(std::move(config)),
      eviction_(config_.eviction_policy),
      aof_(config_.aof_filename, config_.aof_fsync),
      loop_(config_.bind_ip, config_.port),
      repl_id_(generate_replication_id()),
      backlog_(1024 * 1024) {
    stats_.start_time_sec = unix_time_sec();
    loop_.set_idle_timeout(config_.client_timeout);

    if (!config_.replicaof_host.empty() && config_.replicaof_port > 0) {
        start_replication(config_.replicaof_host, config_.replicaof_port);
    }
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
    std::lock_guard<std::mutex> lock(server_mutex_);

    expire_.sample_expired(dict_, [this](const string& key) {
        del_key(key);
    }, 5);

    if (dict_.is_rehashing()) {
        dict_.rehash_ms(1);
    }

    if (config_.aof_enabled) {
        aof_.check_rewrite();
    }

    // Ping replicas periodically if Master
    if (role_ == NodeRole::Master && !replicas_.empty()) {
        string ping = RespWriter::array({"PING"});
        for (auto it = replicas_.begin(); it != replicas_.end();) {
            if (it->second.is_online) {
#ifdef _WIN32
                int n = send(it->second.fd, ping.data(), static_cast<int>(ping.size()), 0);
#else
                ssize_t n = ::send(it->second.fd, ping.data(), ping.size(), MSG_NOSIGNAL);
#endif
                if (n < 0) {
                    it = replicas_.erase(it);
                    continue;
                }
            }
            ++it;
        }
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
    stop_replication();
    loop_.stop();
    if (config_.aof_enabled) {
        aof_.close();
    }
}

void Server::propagate_to_replicas(const vector<string>& args) {
    string payload = RespWriter::array(args);
    backlog_.feed(payload);

    for (auto it = replicas_.begin(); it != replicas_.end();) {
        if (it->second.is_online) {
#ifdef _WIN32
            int n = send(it->second.fd, payload.data(), static_cast<int>(payload.size()), 0);
#else
            ssize_t n = ::send(it->second.fd, payload.data(), payload.size(), MSG_NOSIGNAL);
#endif
            if (n < 0) {
                it = replicas_.erase(it);
                continue;
            }
        }
        ++it;
    }
}

string Server::dump_snapshot() {
    string out;
    dict_.scan([&out](Entry* entry) {
        if (!entry) return;

        if (entry->is_str()) {
            vector<string> cmd{"SET", entry->key, entry->str()};
            if (entry->expire_at_ms > 0) {
                uint64_t now = unix_time_ms();
                if (entry->expire_at_ms > now) {
                    uint64_t ttl = (entry->expire_at_ms - now + 999) / 1000;
                    cmd.push_back("EX");
                    cmd.push_back(std::to_string(ttl));
                } else {
                    return;
                }
            }
            out.append(RespWriter::array(cmd));
        } else if (entry->is_zset()) {
            auto zset = entry->zset();
            if (zset && zset->size() > 0) {
                for (const auto& [member, score] : zset->dict) {
                    vector<string> cmd{"ZADD", entry->key, std::to_string(score), member};
                    out.append(RespWriter::array(cmd));
                }
            }
        }
    });
    return out;
}

void Server::execute(Client& client, const vector<string>& args) {
    std::lock_guard<std::mutex> lock(server_mutex_);
    execute_internal(client, args, false);
}

void Server::execute_internal(Client& client, const vector<string>& args, bool from_master) {
    if (args.empty()) return;

    stats_.total_commands_processed++;
    string cmd = to_upper(args[0]);

    // Read-only replica guard
    if (role_ == NodeRole::Replica && !from_master) {
        if (cmd == "SET" || cmd == "DEL" || cmd == "EXPIRE" || cmd == "ZADD") {
            client.write(RespWriter::error_with_code("READONLY", "You can't write against a read only replica."));
            return;
        }
    }

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
    else if (cmd == "REPLICAOF" || cmd == "SLAVEOF") cmd_replicaof(client, args);
    else if (cmd == "WAIT") cmd_wait(client, args);
    else if (cmd == "PSYNC" || cmd == "SYNC") cmd_psync(client, args);
    else if (cmd == "REPLCONF") cmd_replconf(client, args);
    else client.write(RespWriter::error("unknown command '" + args[0] + "'"));

    // Propagate mutation to replicas if Master and not internal
    if (role_ == NodeRole::Master && client.fd != INVALID_SOCK && !from_master) {
        if (cmd == "SET" || cmd == "DEL" || cmd == "EXPIRE" || cmd == "ZADD") {
            propagate_to_replicas(args);
        }
    }
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

    bool is_new = dict_.set(key, val);
    Entry* entry = dict_.find(key);
    if (entry) {
        if (is_new) {
            stats_.used_memory_bytes += estimate_size(key, val);
            eviction_.on_insert(entry);
        } else {
            eviction_.on_touch(entry);
        }

        if (ex_seconds.has_value()) {
            uint64_t expire_at = unix_time_ms() + (*ex_seconds * 1000);
            expire_.set_expire(entry, expire_at);
        } else {
            expire_.clear_expire(entry);
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

    if (entry && !entry->is_zset()) {
        client.write(RespWriter::error_with_code("WRONGTYPE", "Operation against a key holding the wrong kind of value"));
        return;
    }

    if (config_.maxmemory > 0 && config_.eviction_policy == EvictPolicy::NoEviction) {
        if (stats_.used_memory_bytes > config_.maxmemory) {
            client.write(RespWriter::error_with_code("OOM", "command not allowed when used memory > 'maxmemory'"));
            return;
        }
    }

    shared_ptr<ZSet> zset;
    bool is_new = false;

    if (!entry) {
        zset = make_shared<ZSet>();
        dict_.set(key, zset);
        entry = dict_.find(key);
        eviction_.on_insert(entry);
        is_new = true;
    } else {
        zset = entry->zset();
        eviction_.on_touch(entry);
    }

    int added = zset->add(*score, member);

    if (is_new) {
        stats_.used_memory_bytes += estimate_size(key, zset);
    } else {
        stats_.used_memory_bytes += sizeof(SkipNode) + member.capacity() + 64;
    }

    evict_if_needed();

    if (config_.aof_enabled && client.fd != INVALID_SOCK) {
        aof_.append(args);
    }

    client.write(RespWriter::integer(added));
}

void Server::cmd_zrange(Client& client, const vector<string>& args) {
    if (args.size() < 4) {
        client.write(RespWriter::error("wrong number of arguments for 'zrange' command"));
        return;
    }

    const string& key = args[1];
    auto start_opt = parse_int(args[2]);
    auto stop_opt = parse_int(args[3]);

    if (!start_opt || !stop_opt) {
        client.write(RespWriter::error("value is not an integer or out of range"));
        return;
    }

    bool with_scores = false;
    if (args.size() > 4) {
        if (to_upper(args[4]) == "WITHSCORES") {
            with_scores = true;
        } else {
            client.write(RespWriter::error("syntax error"));
            return;
        }
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
    auto elements = zset->range(*start_opt, *stop_opt);

    vector<string> resp;
    for (const auto& elem : elements) {
        resp.push_back(elem.first);
        if (with_scores) {
            resp.push_back(std::to_string(elem.second));
        }
    }

    client.write(RespWriter::array(resp));
}

void Server::cmd_zrangebyscore(Client& client, const vector<string>& args) {
    if (args.size() < 4) {
        client.write(RespWriter::error("wrong number of arguments for 'zrangebyscore' command"));
        return;
    }

    const string& key = args[1];
    auto min_opt = parse_double(args[2]);
    auto max_opt = parse_double(args[3]);

    if (!min_opt || !max_opt) {
        client.write(RespWriter::error("min or max is not a float"));
        return;
    }

    bool with_scores = false;
    if (args.size() > 4) {
        if (to_upper(string_view(args[4])) == "WITHSCORES") {
            with_scores = true;
        } else {
            client.write(RespWriter::error("syntax error"));
            return;
        }
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
    auto elements = zset->range_by_score(*min_opt, *max_opt);

    vector<string> resp;
    for (const auto& elem : elements) {
        resp.push_back(elem.first);
        if (with_scores) {
            resp.push_back(std::to_string(elem.second));
        }
    }

    client.write(RespWriter::array(resp));
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

    client.write(RespWriter::bulk(std::to_string(*score)));
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
    ss << "# Replication\r\n";
    ss << "role:" << (role_ == NodeRole::Master ? "master" : "slave") << "\r\n";
    ss << "connected_slaves:" << replicas_.size() << "\r\n";
    ss << "master_replid:" << repl_id_ << "\r\n";
    ss << "master_repl_offset:" << backlog_.master_offset() << "\r\n";
    ss << "second_repl_offset:-1\r\n";
    ss << "repl_backlog_active:1\r\n";
    ss << "repl_backlog_size:" << backlog_.size() << "\r\n";
    ss << "repl_backlog_first_byte_offset:" << backlog_.start_offset() << "\r\n";
    ss << "repl_backlog_histlen:" << backlog_.size() << "\r\n";
    ss << "# Keyspace\r\n";
    ss << "db0:keys=" << dict_.size() << ",expires=" << expire_.size() << "\r\n";

    client.write(RespWriter::bulk(ss.str()));
}

void Server::cmd_command(Client& client, const vector<string>& args) {
    (void)args;
    client.write(RespWriter::empty_array());
}

void Server::cmd_replicaof(Client& client, const vector<string>& args) {
    if (args.size() != 3) {
        client.write(RespWriter::error("wrong number of arguments for 'replicaof' command"));
        return;
    }

    if (to_upper(string_view(args[1])) == "NO" && to_upper(string_view(args[2])) == "ONE") {
        server_mutex_.unlock();
        stop_replication();
        server_mutex_.lock();
        role_ = NodeRole::Master;
        client.write(RespWriter::ok());
        return;
    }

    auto port_opt = parse_int(args[2]);
    if (!port_opt || *port_opt <= 0 || *port_opt > 65535) {
        client.write(RespWriter::error("invalid master port"));
        return;
    }

    server_mutex_.unlock();
    start_replication(args[1], static_cast<int>(*port_opt));
    server_mutex_.lock();
    client.write(RespWriter::ok());
}

void Server::cmd_wait(Client& client, const vector<string>& args) {
    if (args.size() != 3) {
        client.write(RespWriter::error("wrong number of arguments for 'wait' command"));
        return;
    }
    auto num_opt = parse_int(args[1]);
    auto timeout_opt = parse_int(args[2]);
    if (!num_opt || !timeout_opt) {
        client.write(RespWriter::error("value is not an integer or out of range"));
        return;
    }
    int64_t target_replicas = *num_opt;
    int64_t timeout_ms = *timeout_opt;

    if (target_replicas <= 0 || replicas_.empty()) {
        client.write(RespWriter::integer(0));
        return;
    }

    int64_t target_offset = backlog_.master_offset();

    // Query replicas for their current ACK
    string getack = RespWriter::array({"REPLCONF", "GETACK", "*"});
    for (const auto& [fd, peer] : replicas_) {
        if (peer.is_online) {
#ifdef _WIN32
            send(fd, getack.data(), static_cast<int>(getack.size()), 0);
#else
            ::send(fd, getack.data(), getack.size(), MSG_NOSIGNAL);
#endif
        }
    }

    uint64_t start = unix_time_ms();
    int64_t acked = 0;

    while (true) {
        acked = 0;
        for (const auto& [fd, peer] : replicas_) {
            if (peer.is_online && peer.ack_offset >= target_offset) {
                acked++;
            }
        }

        if (acked >= target_replicas) break;

        uint64_t elapsed = unix_time_ms() - start;
        if (timeout_ms >= 0 && elapsed >= static_cast<uint64_t>(timeout_ms)) break;

        server_mutex_.unlock();
        loop_.poll_events(5);
        server_mutex_.lock();
    }

    client.write(RespWriter::integer(acked));
}

void Server::cmd_psync(Client& client, const vector<string>& args) {
    string req_id = (args.size() > 1) ? args[1] : "?";
    int64_t req_offset = -1;
    if (args.size() > 2) {
        auto parsed = parse_int(args[2]);
        if (parsed) req_offset = *parsed;
    }

    if (backlog_.can_partial_resync(req_id, req_offset, repl_id_)) {
        // Partial Resync!
        client.write("+CONTINUE\r\n");
        string slice = backlog_.get_data(req_offset);
        if (!slice.empty()) {
            client.write(slice);
        }
        ReplicaPeer peer;
        peer.fd = client.fd;
        peer.ack_offset = req_offset;
        peer.is_online = true;
        peer.last_ack_time = unix_time_ms();
        replicas_[client.fd] = peer;
    } else {
        // Full Resync!
        string full_header = "+FULLRESYNC " + repl_id_ + " " + std::to_string(backlog_.master_offset()) + "\r\n";
        client.write(full_header);
        string snapshot = dump_snapshot();
        client.write(RespWriter::bulk(snapshot));
        ReplicaPeer peer;
        peer.fd = client.fd;
        peer.ack_offset = backlog_.master_offset();
        peer.is_online = true;
        peer.last_ack_time = unix_time_ms();
        replicas_[client.fd] = peer;
    }
}

void Server::cmd_replconf(Client& client, const vector<string>& args) {
    if (args.size() >= 3 && to_upper(args[1]) == "ACK") {
        auto ack = parse_int(args[2]);
        if (ack && replicas_.count(client.fd)) {
            replicas_[client.fd].ack_offset = *ack;
            replicas_[client.fd].last_ack_time = unix_time_ms();
        }
        return;
    }
    if (args.size() >= 2 && to_upper(args[1]) == "GETACK") {
        client.write(RespWriter::array({"REPLCONF", "ACK", std::to_string(replica_applied_offset_)}));
        return;
    }
    client.write(RespWriter::ok());
}

void Server::start_replication(const string& host, int port) {
    stop_replication();

    role_ = NodeRole::Replica;
    master_host_ = host;
    master_port_ = port;
    stop_replica_thread_ = false;

    replica_thread_ = std::thread(&Server::replica_sync_worker, this);
}

void Server::stop_replication() {
    stop_replica_thread_ = true;
    socket_t s = replica_sock_.exchange(INVALID_SOCK_REPL);
    if (s != INVALID_SOCK_REPL) {
        close_sock(s);
    }
    if (replica_thread_.joinable()) {
        replica_thread_.join();
    }
}

void Server::replica_sync_worker() {
    while (!stop_replica_thread_) {
        socket_t sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock == INVALID_SOCK_REPL) {
            for (int i = 0; i < 50 && !stop_replica_thread_; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            continue;
        }

        replica_sock_ = sock;

        struct sockaddr_in sin{};
        sin.sin_family = AF_INET;
        sin.sin_port = htons(static_cast<uint16_t>(master_port_));
        inet_pton(AF_INET, master_host_.c_str(), &sin.sin_addr);

        if (connect(sock, reinterpret_cast<struct sockaddr*>(&sin), sizeof(sin)) != 0) {
            replica_sock_ = INVALID_SOCK_REPL;
            close_sock(sock);
            for (int i = 0; i < 50 && !stop_replica_thread_; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            continue;
        }

#ifdef _WIN32
        DWORD tv = 1000;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
#else
        struct timeval tv{};
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
#endif

        auto send_line = [sock](const string& str) -> bool {
#ifdef _WIN32
            return send(sock, str.data(), static_cast<int>(str.size()), 0) > 0;
#else
            return ::send(sock, str.data(), str.size(), MSG_NOSIGNAL) > 0;
#endif
        };

        // 1. PING
        send_line("PING\r\n");
        char buf[4096];
        int n = recv(sock, buf, sizeof(buf) - 1, 0);
        if (n <= 0) {
            close_sock(sock);
            continue;
        }

        // 2. REPLCONF listening-port
        send_line(RespWriter::array({"REPLCONF", "listening-port", std::to_string(config_.port)}));
        n = recv(sock, buf, sizeof(buf) - 1, 0);
        if (n <= 0) {
            close_sock(sock);
            continue;
        }

        // 3. PSYNC <repl_id> <offset>
        string psync_id = cached_master_repl_id_.empty() ? "?" : cached_master_repl_id_;
        string psync_cmd = RespWriter::array({"PSYNC", psync_id, std::to_string(replica_applied_offset_)});
        send_line(psync_cmd);

        string line;
        while (line.find("\r\n") == string::npos && !stop_replica_thread_) {
            char c;
            int r = recv(sock, &c, 1, 0);
            if (r <= 0) break;
            line.push_back(c);
        }

        if (line.rfind("+FULLRESYNC", 0) == 0) {
            std::stringstream ss(line.substr(12));
            string new_id;
            int64_t new_offset = 0;
            ss >> new_id >> new_offset;
            cached_master_repl_id_ = new_id;
            replica_applied_offset_ = new_offset;

            string len_line;
            while (len_line.find("\r\n") == string::npos && !stop_replica_thread_) {
                char c;
                int r = recv(sock, &c, 1, 0);
                if (r <= 0) break;
                len_line.push_back(c);
            }
            if (len_line.empty() || len_line[0] != '$') {
                close_sock(sock);
                continue;
            }
            int64_t snap_len = 0;
            std::from_chars(len_line.data() + 1, len_line.data() + len_line.size() - 2, snap_len);

            string snapshot_data;
            if (snap_len > 0) {
                snapshot_data.resize(snap_len);
                size_t total_read = 0;
                while (total_read < static_cast<size_t>(snap_len) && !stop_replica_thread_) {
                    int r = recv(sock, &snapshot_data[total_read], static_cast<int>(snap_len - total_read), 0);
                    if (r <= 0) break;
                    total_read += r;
                }
                char trailing[2];
                recv(sock, trailing, 2, 0);

                std::lock_guard<std::mutex> lock(server_mutex_);
                dict_.clear();
                RespParser snap_parser;
                snap_parser.feed(snapshot_data);
                vector<string> snap_cmd;
                while (snap_parser.next_command(snap_cmd)) {
                    Client dummy(INVALID_SOCK_REPL, "127.0.0.1", 0);
                    execute_internal(dummy, snap_cmd, true);
                }
            }
        } else if (line.rfind("+CONTINUE", 0) == 0) {
            // Partial resync succeeded!
        } else {
            close_sock(sock);
            continue;
        }

        RespParser stream_parser;
        uint64_t last_ack_sent = unix_time_ms();

        while (!stop_replica_thread_) {
            char chunk[8192];
            int r = recv(sock, chunk, sizeof(chunk), 0);
            if (r > 0) {
                if (config_.repl_delay_ms > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(config_.repl_delay_ms));
                }
                last_master_io_time_ = unix_time_ms();
                stream_parser.feed(chunk, r);
                vector<string> cmd_args;
                while (stream_parser.next_command(cmd_args)) {
                    if (cmd_args.empty()) continue;
                    string c = to_upper(string_view(cmd_args[0]));
                    if (c == "REPLCONF" && cmd_args.size() > 1 && to_upper(string_view(cmd_args[1])) == "GETACK") {
                        send_line(RespWriter::array({"REPLCONF", "ACK", std::to_string(replica_applied_offset_)}));
                    } else if (c == "PING") {
                        // Keepalive
                    } else {
                        {
                            std::lock_guard<std::mutex> lock(server_mutex_);
                            Client dummy(INVALID_SOCK_REPL, "127.0.0.1", 0);
                            execute_internal(dummy, cmd_args, true);
                        }
                        replica_applied_offset_ += RespWriter::array(cmd_args).size();
                    }
                }
            } else if (r == 0) {
                break;
            } else {
#ifdef _WIN32
                int err = WSAGetLastError();
                if (err != WSAETIMEDOUT && err != WSAEWOULDBLOCK) break;
#else
                if (errno != EAGAIN && errno != EWOULDBLOCK) break;
#endif
            }

            uint64_t now = unix_time_ms();
            if (now - last_ack_sent >= 1000) {
                send_line(RespWriter::array({"REPLCONF", "ACK", std::to_string(replica_applied_offset_)}));
                last_ack_sent = now;
            }
        }

        replica_sock_ = INVALID_SOCK_REPL;
        close_sock(sock);
        for (int i = 0; i < 20 && !stop_replica_thread_; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

}
