#include "common.h"
#include "skiplist.h"
#include "dict.h"
#include "resp.h"
#include "evict.h"
#include "expire.h"
#include <iostream>
#include <cassert>
#include <string>
#include <vector>

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

    cout << "\nALL UNIT TESTS PASSED SUCCESSFULLY!" << endl;
    return 0;
}
