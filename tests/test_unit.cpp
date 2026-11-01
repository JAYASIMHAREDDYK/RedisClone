#include "common.h"
#include "skiplist.h"
#include "dict.h"
#include "resp.h"
#include "evict.h"
#include "expire.h"
#include "replication.h"
#include "server.h"
#include <iostream>
#include <cassert>
#include <string>
#include <vector>
#include <random>

using std::string;
using std::vector;
using std::cout;
using std::endl;

void test_common() {
    string s = "hello world";
    redis::to_upper(s);
    assert(s == "HELLO WORLD");

    string s2 = "HeLLo";
    redis::to_lower(s2);
    assert(s2 == "hello");

    auto num = redis::parse_int("123456");
    assert(num.has_value() && *num == 123456);

    auto num_neg = redis::parse_int("-9876");
    assert(num_neg.has_value() && *num_neg == -9876);

    auto d = redis::parse_double("3.14159");
    assert(d.has_value() && std::abs(*d - 3.14159) < 1e-5);

    uint64_t h1 = redis::hash_key("test_key");
    uint64_t h2 = redis::hash_key("test_key");
    assert(h1 == h2);
    assert(h1 != 0);
}

void test_dict() {
    redis::Dict dict;

    assert(dict.set("key1", "val1") == true);
    assert(dict.set("key2", "val2") == true);
    assert(dict.set("key3", "val3") == true);
    assert(dict.set("key4", "val4") == true);
    assert(dict.size() == 4);

    auto* e1 = dict.find("key1");
    assert(e1 != nullptr && e1->str() == "val1");

    auto* e2 = dict.find("key2");
    assert(e2 != nullptr && e2->str() == "val2");

    assert(dict.set("key1", "updated_val1") == false);
    e1 = dict.find("key1");
    assert(e1 != nullptr && e1->str() == "updated_val1");

    for (int i = 5; i <= 200; ++i) {
        dict.set("k" + std::to_string(i), "v" + std::to_string(i));
    }
    assert(dict.size() == 200);

    while (dict.is_rehashing()) {
        dict.step_rehash(1);
    }
    assert(!dict.is_rehashing());

    for (int i = 5; i <= 200; ++i) {
        auto* e = dict.find("k" + std::to_string(i));
        assert(e != nullptr && e->str() == ("v" + std::to_string(i)));
    }

    assert(dict.erase("k10") == true);
    assert(dict.find("k10") == nullptr);
    assert(dict.size() == 199);
}

void test_skiplist() {
    redis::ZSet zset;

    assert(zset.add(10.0, "alice") == true);
    assert(zset.add(20.0, "bob") == true);
    assert(zset.add(15.0, "charlie") == true);
    assert(zset.add(5.0, "david") == true);
    assert(zset.size() == 4);

    auto score = zset.score_of("charlie");
    assert(score.has_value() && *score == 15.0);

    auto rank_david = zset.rank_of("david");
    assert(rank_david.has_value() && *rank_david == 0);

    auto rank_alice = zset.rank_of("alice");
    assert(rank_alice.has_value() && *rank_alice == 1);

    auto rank_charlie = zset.rank_of("charlie");
    assert(rank_charlie.has_value() && *rank_charlie == 2);

    auto rank_bob = zset.rank_of("bob");
    assert(rank_bob.has_value() && *rank_bob == 3);

    auto r = zset.range(0, -1);
    assert(r.size() == 4);
    assert(r[0].first == "david");
    assert(r[1].first == "alice");
    assert(r[2].first == "charlie");
    assert(r[3].first == "bob");

    auto by_score = zset.range_by_score(10.0, 20.0);
    assert(by_score.size() == 3);
    assert(by_score[0].first == "alice");
    assert(by_score[1].first == "charlie");
    assert(by_score[2].first == "bob");

    assert(zset.add(25.0, "alice") == false);
    assert(zset.size() == 4);
    assert(*zset.rank_of("alice") == 3);

    assert(zset.remove("david") == true);
    assert(zset.size() == 3);
    assert(zset.rank_of("david").has_value() == false);
}

void test_resp_parser_framing() {
    redis::RespParser parser;

    // Normal complete command
    parser.feed("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n");
    vector<string> cmd;
    assert(parser.next_command(cmd) == true);
    assert(cmd.size() == 3 && cmd[0] == "SET" && cmd[1] == "a" && cmd[2] == "b");
    assert(parser.next_command(cmd) == false);

    // Partial reads across chunk boundaries
    parser.feed("*2\r\n$3\r\nGE");
    assert(parser.next_command(cmd) == false);
    parser.feed("T\r\n$1\r\nx");
    assert(parser.next_command(cmd) == false);
    parser.feed("\r\n");
    assert(parser.next_command(cmd) == true);
    assert(cmd.size() == 2 && cmd[0] == "GET" && cmd[1] == "x");

    // Split CRLF across chunks
    parser.feed("*1\r\n$4\r\nPING\r");
    assert(parser.next_command(cmd) == false);
    parser.feed("\n");
    assert(parser.next_command(cmd) == true);
    assert(cmd.size() == 1 && cmd[0] == "PING");

    // Multiple pipelined commands in one buffer
    parser.feed("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n");
    for (int i = 0; i < 3; ++i) {
        assert(parser.next_command(cmd) == true);
        assert(cmd[0] == "PING");
    }
    assert(parser.next_command(cmd) == false);

    // Writer checks
    assert(redis::RespWriter::status("OK") == "+OK\r\n");
    assert(redis::RespWriter::integer(42) == ":42\r\n");
    assert(redis::RespWriter::bulk("hi") == "$2\r\nhi\r\n");
    assert(redis::RespWriter::null_bulk() == "$-1\r\n");
}

void test_evict() {
    redis::Evictor evictor(redis::EvictPolicy::AllKeysLFU);

    redis::Entry e1("k1", "v1");
    redis::Entry e2("k2", "v2");
    redis::Entry e3("k3", "v3");

    evictor.on_insert(&e1);
    evictor.on_insert(&e2);
    evictor.on_insert(&e3);

    evictor.on_touch(&e1);
    evictor.on_touch(&e1);
    evictor.on_touch(&e2);

    redis::Dict dict;
    dict.set("k1", "v1");
    dict.set("k2", "v2");
    dict.set("k3", "v3");

    string victim = evictor.pick_victim(dict);
    assert(victim == "k3");

    evictor.on_remove(&e3);
    victim = evictor.pick_victim(dict);
    assert(victim == "k2");
}

void test_expire() {
    redis::Expirer expirer;
    redis::Entry e("temp", "val");

    assert(expirer.is_expired(&e) == false);
    assert(expirer.ttl_sec(&e) == -1);

    uint64_t now = redis::unix_time_ms();
    expirer.set_expire(&e, now + 10000);
    assert(expirer.is_expired(&e) == false);
    int64_t ttl = expirer.ttl_sec(&e);
    assert(ttl >= 9 && ttl <= 10);

    expirer.set_expire(&e, now - 1000);
    assert(expirer.is_expired(&e) == true);
    assert(expirer.ttl_sec(&e) == -2);
}

void test_replication_backlog() {
    string my_id = "test_repl_id";

    // 1. Basic feed and read
    redis::ReplicationBacklog backlog(100);
    backlog.feed("hello");
    assert(backlog.master_offset() == 5);
    assert(backlog.start_offset() == 0);
    assert(backlog.size() == 5);
    assert(backlog.get_data(0) == "hello");

    // 2. Wraparound
    string data1(80, 'A');
    backlog.feed(data1); // master_offset = 85, size = 85
    assert(backlog.master_offset() == 85);
    assert(backlog.start_offset() == 0);

    string data2(30, 'B');
    backlog.feed(data2); // total fed = 115 > max_size 100
    assert(backlog.master_offset() == 115);
    assert(backlog.size() == 100);
    assert(backlog.start_offset() == 15); // 115 - 100

    string read_all = backlog.get_data(15);
    assert(read_all.size() == 100);
    assert(read_all == string(70, 'A') + string(30, 'B'));

    // 3. Offset exactly at the window edge (start_offset)
    assert(backlog.can_partial_resync(my_id, 15, my_id) == true);
    assert(backlog.get_data(15).size() == 100);

    // Offset exactly at master_offset
    assert(backlog.can_partial_resync(my_id, 115, my_id) == true);
    assert(backlog.get_data(115) == "");

    // 4. Offset one byte outside the window edge (must reject / force full resync)
    assert(backlog.can_partial_resync(my_id, 14, my_id) == false);
    assert(backlog.get_data(14) == "");
    assert(backlog.can_partial_resync(my_id, -1, my_id) == false);

    // 5. Payload larger than the entire buffer with unaligned offset and distinct byte assertion
    redis::ReplicationBacklog backlog_large(50);
    // Write 17 bytes first so initial write_idx is unaligned (17 % 50 != 0)
    string prefix(17, 'P');
    backlog_large.feed(prefix);
    assert(backlog_large.master_offset() == 17);
    assert(backlog_large.start_offset() == 0);

    // Feed 137 bytes (not a multiple of 50) of distinct sequential characters
    string distinct_oversized;
    distinct_oversized.reserve(137);
    for (int i = 0; i < 137; ++i) {
        distinct_oversized.push_back(static_cast<char>('A' + (i % 26)));
    }
    backlog_large.feed(distinct_oversized);

    // Master offset is 17 + 137 = 154
    // Start offset is 154 - 50 = 104
    assert(backlog_large.master_offset() == 154);
    assert(backlog_large.size() == 50);
    assert(backlog_large.start_offset() == 104);

    // The retained 50 bytes must match the trailing 50 bytes of distinct_oversized (from index 137 - 50 = 87)
    string expected_retained = distinct_oversized.substr(137 - 50, 50);
    string read_full = backlog_large.get_data(104);
    assert(read_full.size() == 50);
    assert(read_full == expected_retained);

    // Read sub-window
    string read_sub = backlog_large.get_data(124);
    assert(read_sub == distinct_oversized.substr(137 - 30, 30));

    // 6. Subsequent write with wraparound after an oversize payload
    string post_wrap;
    post_wrap.reserve(33);
    for (int i = 0; i < 33; ++i) {
        post_wrap.push_back(static_cast<char>('0' + (i % 10)));
    }
    backlog_large.feed(post_wrap);

    // Master offset is 154 + 33 = 187
    // Start offset is 187 - 50 = 137
    assert(backlog_large.master_offset() == 187);
    assert(backlog_large.size() == 50);
    assert(backlog_large.start_offset() == 137);

    // Expected 50 bytes: last 17 bytes from distinct_oversized + all 33 bytes from post_wrap
    string expected_after_wrap = distinct_oversized.substr(137 - 17, 17) + post_wrap;
    string read_after_wrap = backlog_large.get_data(137);
    assert(read_after_wrap.size() == 50);
    assert(read_after_wrap == expected_after_wrap);
}

void test_replication_backlog_model_check() {
    // Randomized model check: Compare ReplicationBacklog against an ideal std::string model
    const size_t cap = 64; // ring buffer size
    redis::ReplicationBacklog backlog(cap);
    string model;
    string repl_id = "test_backlog_model_id_1234567890";

    std::mt19937 rng(42); // deterministic seed for reproducibility
    std::uniform_int_distribution<int> mode_dist(0, 7);

    size_t byte_counter = 0;
    const int NUM_ITERATIONS = 5000;

    for (int iter = 0; iter < NUM_ITERATIONS; ++iter) {
        size_t len = 0;
        int mode = mode_dist(rng);
        if (mode == 0) {
            len = 1; // 1 byte
        } else if (mode == 1) {
            len = cap - 1; // boundary: max_size - 1
        } else if (mode == 2) {
            len = cap; // boundary: exactly max_size
        } else if (mode == 3) {
            len = cap + 1; // boundary: max_size + 1
        } else if (mode == 4) {
            len = cap * 2 - 1;
        } else if (mode == 5) {
            len = cap * 3 + 17; // oversize, non-multiple
        } else if (mode == 6) {
            len = std::uniform_int_distribution<size_t>(2, cap - 2)(rng);
        } else {
            len = std::uniform_int_distribution<size_t>(cap + 2, cap * 5)(rng);
        }

        string chunk;
        chunk.reserve(len);
        for (size_t i = 0; i < len; ++i) {
            chunk.push_back(static_cast<char>('!' + (byte_counter++ % 90)));
        }

        backlog.feed(chunk);
        model.append(chunk);

        int64_t expected_master_offset = static_cast<int64_t>(model.size());
        size_t expected_stored = std::min(model.size(), cap);
        int64_t expected_start_offset = expected_master_offset - static_cast<int64_t>(expected_stored);

        assert(backlog.master_offset() == expected_master_offset);
        assert(backlog.size() == expected_stored);
        assert(backlog.start_offset() == expected_start_offset);

        // Model Check Query 1: Exactly at start_offset
        if (expected_stored > 0) {
            assert(backlog.can_partial_resync(repl_id, expected_start_offset, repl_id) == true);
            string full_window = backlog.get_data(expected_start_offset);
            assert(full_window.size() == expected_stored);
            assert(full_window == model.substr(expected_start_offset, expected_stored));
        }

        // Model Check Query 2: Exactly at master_offset (up-to-date replica, 0 bytes needed)
        assert(backlog.can_partial_resync(repl_id, expected_master_offset, repl_id) == true);
        assert(backlog.get_data(expected_master_offset) == "");

        // Model Check Query 3: Outside window bounds (must reject)
        if (expected_start_offset > 0) {
            assert(backlog.can_partial_resync(repl_id, expected_start_offset - 1, repl_id) == false);
            assert(backlog.get_data(expected_start_offset - 1) == "");
        }
        assert(backlog.can_partial_resync(repl_id, expected_master_offset + 1, repl_id) == false);
        assert(backlog.get_data(expected_master_offset + 1) == "");

        // Model Check Query 4: Random offsets within valid window
        if (expected_stored > 0) {
            std::uniform_int_distribution<int64_t> off_dist(expected_start_offset, expected_master_offset);
            for (int q = 0; q < 3; ++q) {
                int64_t query_off = off_dist(rng);
                assert(backlog.can_partial_resync(repl_id, query_off, repl_id) == true);
                string slice = backlog.get_data(query_off);
                size_t expected_len = static_cast<size_t>(expected_master_offset - query_off);
                assert(slice.size() == expected_len);
                assert(slice == model.substr(query_off, expected_len));
            }
        }
    }
}

void test_transactions() {
    redis::Config cfg;
    cfg.aof_enabled = false;
    redis::Server server(cfg);

    redis::Client c1(INVALID_SOCK, "127.0.0.1", 10001);
    redis::Client c2(INVALID_SOCK, "127.0.0.1", 10002);

    // 1. Errors outside MULTI
    c1.write_buf.clear();
    server.execute(c1, {"EXEC"});
    assert(c1.write_buf.find("-ERR EXEC without MULTI") != string::npos);

    c1.write_buf.clear();
    server.execute(c1, {"DISCARD"});
    assert(c1.write_buf.find("-ERR DISCARD without MULTI") != string::npos);

    // 2. Empty MULTI/EXEC
    c1.write_buf.clear();
    server.execute(c1, {"MULTI"});
    assert(c1.write_buf == "+OK\r\n");
    assert(c1.in_multi == true);

    c1.write_buf.clear();
    server.execute(c1, {"EXEC"});
    assert(c1.write_buf == "*0\r\n");
    assert(c1.in_multi == false);

    // 3. Normal MULTI / QUEUED / EXEC with multiple operations
    c1.write_buf.clear();
    server.execute(c1, {"MULTI"});
    assert(c1.write_buf == "+OK\r\n");

    c1.write_buf.clear();
    server.execute(c1, {"SET", "txn_k1", "val1"});
    assert(c1.write_buf == "+QUEUED\r\n");

    c1.write_buf.clear();
    server.execute(c1, {"SET", "txn_k2", "val2"});
    assert(c1.write_buf == "+QUEUED\r\n");

    c1.write_buf.clear();
    server.execute(c1, {"GET", "txn_k1"});
    assert(c1.write_buf == "+QUEUED\r\n");

    c1.write_buf.clear();
    server.execute(c1, {"PING"});
    assert(c1.write_buf == "+QUEUED\r\n");

    // While c1 is in MULTI, keys should not be modified yet
    assert(server.dict().find("txn_k1") == nullptr);

    c1.write_buf.clear();
    server.execute(c1, {"EXEC"});
    string expected_exec = "*4\r\n+OK\r\n+OK\r\n$4\r\nval1\r\n+PONG\r\n";
    assert(c1.write_buf == expected_exec);
    assert(server.dict().find("txn_k1") != nullptr);
    assert(server.dict().find("txn_k1")->str() == "val1");

    // 4. DISCARD test
    c1.write_buf.clear();
    server.execute(c1, {"MULTI"});
    assert(c1.write_buf == "+OK\r\n");

    c1.write_buf.clear();
    server.execute(c1, {"SET", "txn_k1", "should_not_set"});
    assert(c1.write_buf == "+QUEUED\r\n");

    c1.write_buf.clear();
    server.execute(c1, {"DISCARD"});
    assert(c1.write_buf == "+OK\r\n");
    assert(c1.in_multi == false);
    assert(server.dict().find("txn_k1")->str() == "val1");

    // 5. Nested MULTI causes EXECABORT
    c1.write_buf.clear();
    server.execute(c1, {"MULTI"});
    assert(c1.write_buf == "+OK\r\n");

    c1.write_buf.clear();
    server.execute(c1, {"MULTI"});
    assert(c1.write_buf.find("-ERR MULTI calls can not be nested") != string::npos);
    assert(c1.dirty_exec == true);

    c1.write_buf.clear();
    server.execute(c1, {"EXEC"});
    assert(c1.write_buf.find("-EXECABORT") != string::npos);
    assert(c1.in_multi == false);

    // 6. Queue-time error (wrong arity) causes EXECABORT
    c1.write_buf.clear();
    server.execute(c1, {"MULTI"});
    assert(c1.write_buf == "+OK\r\n");

    c1.write_buf.clear();
    server.execute(c1, {"SET"});
    assert(c1.write_buf.find("-ERR wrong number of arguments") != string::npos);
    assert(c1.dirty_exec == true);

    c1.write_buf.clear();
    server.execute(c1, {"SET", "aborted_key", "aborted_val"});
    assert(c1.write_buf == "+QUEUED\r\n");

    c1.write_buf.clear();
    server.execute(c1, {"EXEC"});
    assert(c1.write_buf.find("-EXECABORT") != string::npos);
    assert(server.dict().find("aborted_key") == nullptr);

    // 7. Runtime error does NOT abort subsequent commands
    server.execute(c1, {"SET", "str_key", "my_string"});
    c1.write_buf.clear();
    server.execute(c1, {"MULTI"});
    server.execute(c1, {"SET", "ok_key1", "val1"});
    server.execute(c1, {"ZADD", "str_key", "1.0", "member1"}); // runtime WRONGTYPE
    server.execute(c1, {"SET", "ok_key2", "val2"});

    c1.write_buf.clear();
    server.execute(c1, {"EXEC"});
    assert(c1.write_buf.find("+OK\r\n-WRONGTYPE") != string::npos);
    assert(server.dict().find("ok_key1") != nullptr);
    assert(server.dict().find("ok_key2") != nullptr);

    // 8. WATCH / UNWATCH optimistic locking with mock socket fds
    c1.fd = 101;
    c2.fd = 102;
    server.execute(c1, {"SET", "watched_key", "initial"});

    c1.write_buf.clear();
    server.execute(c1, {"WATCH", "watched_key"});
    assert(c1.write_buf == "+OK\r\n");

    c2.write_buf.clear();
    server.execute(c2, {"SET", "watched_key", "modified_by_c2"});
    assert(c2.write_buf == "+OK\r\n");

    c1.write_buf.clear();
    server.execute(c1, {"MULTI"});
    server.execute(c1, {"SET", "watched_key", "c1_txn_val"});
    c1.write_buf.clear();
    server.execute(c1, {"EXEC"});
    assert(c1.write_buf == "$-1\r\n");
    assert(server.dict().find("watched_key")->str() == "modified_by_c2");

    // 9. UNWATCH before modification allows EXEC to succeed
    server.execute(c1, {"WATCH", "watched_key"});
    server.execute(c1, {"UNWATCH"});

    server.execute(c2, {"SET", "watched_key", "modified_again"});

    c1.write_buf.clear();
    server.execute(c1, {"MULTI"});
    server.execute(c1, {"SET", "watched_key", "c1_success"});
    c1.write_buf.clear();
    server.execute(c1, {"EXEC"});
    assert(c1.write_buf == "*1\r\n+OK\r\n");
    assert(server.dict().find("watched_key")->str() == "c1_success");

    // 10. WATCH inside MULTI is rejected and aborts
    c1.write_buf.clear();
    server.execute(c1, {"MULTI"});
    server.execute(c1, {"WATCH", "watched_key"});
    assert(c1.write_buf.find("-ERR WATCH inside MULTI is not allowed") != string::npos);
    c1.write_buf.clear();
    server.execute(c1, {"EXEC"});
    assert(c1.write_buf.find("-EXECABORT") != string::npos);

    // Reset fds to INVALID_SOCK so Client destructor doesn't call closesocket on fake fds
    c1.fd = INVALID_SOCK;
    c2.fd = INVALID_SOCK;
}

int main() {
    test_common();
    cout << "[PASS] Common" << endl;

    test_dict();
    cout << "[PASS] Dict" << endl;

    test_skiplist();
    cout << "[PASS] SkipList & ZSet" << endl;

    test_resp_parser_framing();
    cout << "[PASS] RESP2 Parser & Framing (partial reads, split CRLF, pipeline)" << endl;

    test_evict();
    cout << "[PASS] Evictor (LFU & LRU)" << endl;

    test_expire();
    cout << "[PASS] Expirer" << endl;

    test_replication_backlog();
    cout << "[PASS] Replication Backlog Ring Buffer (wraparound, edge, outside, oversized)" << endl;

    test_replication_backlog_model_check();
    cout << "[PASS] Replication Backlog Randomized Model Check (5000 iterations vs std::string)" << endl;

    test_transactions();
    cout << "[PASS] Transactions (MULTI, EXEC, DISCARD, WATCH, UNWATCH, EXECABORT)" << endl;

    cout << "\nALL UNIT TESTS PASSED SUCCESSFULLY!" << endl;
    return 0;
}
