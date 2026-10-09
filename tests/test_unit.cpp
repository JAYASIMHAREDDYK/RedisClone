#include "util.h"
#include "skiplist.h"
#include "dict.h"
#include "resp.h"
#include "eviction.h"
#include "expire.h"
#include <iostream>
#include <cassert>
#include <string>
#include <vector>

void testUtils() {
    std::string s = "hello world";
    redis::toUpper(s);
    assert(s == "HELLO WORLD");

    std::string s2 = "HeLLo";
    redis::toLower(s2);
    assert(s2 == "hello");

    auto num = redis::parseInteger("123456");
    assert(num.has_value() && *num == 123456);

    auto num_neg = redis::parseInteger("-9876");
    assert(num_neg.has_value() && *num_neg == -9876);

    auto d = redis::parseDouble("3.14159");
    assert(d.has_value() && std::abs(*d - 3.14159) < 1e-5);

    uint64_t h1 = redis::hashKey("test_key");
    uint64_t h2 = redis::hashKey("test_key");
    assert(h1 == h2);
    assert(h1 != 0);
}

void testDict() {
    redis::ProgressiveDict dict;

    assert(dict.set("key1", "val1") == true);
    assert(dict.set("key2", "val2") == true);
    assert(dict.set("key3", "val3") == true);
    assert(dict.set("key4", "val4") == true);

    assert(dict.size() == 4);

    auto* e1 = dict.find("key1");
    assert(e1 != nullptr && e1->getString() == "val1");

    auto* e2 = dict.find("key2");
    assert(e2 != nullptr && e2->getString() == "val2");

    assert(dict.set("key1", "updated_val1") == false);
    e1 = dict.find("key1");
    assert(e1 != nullptr && e1->getString() == "updated_val1");

    for (int i = 5; i <= 200; ++i) {
        dict.set("k" + std::to_string(i), "v" + std::to_string(i));
    }

    assert(dict.size() == 200);

    while (dict.isRehashing()) {
        dict.stepRehash(1);
    }
    assert(!dict.isRehashing());

    for (int i = 5; i <= 200; ++i) {
        auto* e = dict.find("k" + std::to_string(i));
        assert(e != nullptr && e->getString() == ("v" + std::to_string(i)));
    }

    assert(dict.erase("k10") == true);
    assert(dict.find("k10") == nullptr);
    assert(dict.size() == 199);
}

void testSkipList() {
    redis::SortedSet zset;

    assert(zset.add(10.0, "alice") == true);
    assert(zset.add(20.0, "bob") == true);
    assert(zset.add(15.0, "charlie") == true);
    assert(zset.add(5.0, "david") == true);

    assert(zset.size() == 4);

    auto score = zset.getScore("charlie");
    assert(score.has_value() && *score == 15.0);

    auto rank_david = zset.getRank("david");
    assert(rank_david.has_value() && *rank_david == 0);

    auto rank_alice = zset.getRank("alice");
    assert(rank_alice.has_value() && *rank_alice == 1);

    auto rank_charlie = zset.getRank("charlie");
    assert(rank_charlie.has_value() && *rank_charlie == 2);

    auto rank_bob = zset.getRank("bob");
    assert(rank_bob.has_value() && *rank_bob == 3);

    auto range = zset.range(0, -1);
    assert(range.size() == 4);
    assert(range[0].first == "david");
    assert(range[1].first == "alice");
    assert(range[2].first == "charlie");
    assert(range[3].first == "bob");

    auto by_score = zset.rangeByScore(10.0, 20.0);
    assert(by_score.size() == 3);
    assert(by_score[0].first == "alice");
    assert(by_score[1].first == "charlie");
    assert(by_score[2].first == "bob");

    assert(zset.add(25.0, "alice") == false);
    assert(zset.size() == 4);
    auto new_rank_alice = zset.getRank("alice");
    assert(new_rank_alice.has_value() && *new_rank_alice == 3);

    assert(zset.remove("david") == true);
    assert(zset.size() == 3);
    assert(zset.getRank("david").has_value() == false);
}

void testResp() {
    redis::RespParser parser;

    std::string stream = "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$4\r\njohn\r\n*1\r\n$4\r\nPING\r\n";
    parser.feed(stream);

    std::vector<std::string> cmd1;
    assert(parser.nextCommand(cmd1) == true);
    assert(cmd1.size() == 3);
    assert(cmd1[0] == "SET");
    assert(cmd1[1] == "name");
    assert(cmd1[2] == "john");

    std::vector<std::string> cmd2;
    assert(parser.nextCommand(cmd2) == true);
    assert(cmd2.size() == 1);
    assert(cmd2[0] == "PING");

    std::vector<std::string> cmd3;
    assert(parser.nextCommand(cmd3) == false);

    parser.feed("*2\r\n$3\r\nGE");
    assert(parser.nextCommand(cmd3) == false);
    parser.feed("T\r\n$4\r\nuser\r\n");
    assert(parser.nextCommand(cmd3) == true);
    assert(cmd3.size() == 2);
    assert(cmd3[0] == "GET");
    assert(cmd3[1] == "user");

    assert(redis::RespEncoder::simpleString("OK") == "+OK\r\n");
    assert(redis::RespEncoder::integer(42) == ":42\r\n");
    assert(redis::RespEncoder::bulkString("hi") == "$2\r\nhi\r\n");
    assert(redis::RespEncoder::nullBulkString() == "$-1\r\n");
}

void testEviction() {
    redis::EvictionEngine engine(redis::EvictionPolicy::AllKeysLFU);

    redis::DictEntry e1("k1", "v1");
    redis::DictEntry e2("k2", "v2");
    redis::DictEntry e3("k3", "v3");

    engine.onKeyInserted(&e1);
    engine.onKeyInserted(&e2);
    engine.onKeyInserted(&e3);

    engine.onKeyAccessed(&e1);
    engine.onKeyAccessed(&e1);
    engine.onKeyAccessed(&e2);

    redis::ProgressiveDict dict;
    dict.set("k1", "v1");
    dict.set("k2", "v2");
    dict.set("k3", "v3");

    std::string candidate = engine.selectEvictionCandidate(dict);
    assert(candidate == "k3");

    engine.onKeyRemoved(&e3);
    candidate = engine.selectEvictionCandidate(dict);
    assert(candidate == "k2");
}

void testExpiration() {
    redis::ExpirationManager expire;
    redis::DictEntry e("temp", "val");

    assert(expire.isExpired(&e) == false);
    assert(expire.getTtlSeconds(&e) == -1);

    uint64_t now = redis::getUnixTimeMs();
    expire.setExpire(&e, now + 10000);
    assert(expire.isExpired(&e) == false);
    int64_t ttl = expire.getTtlSeconds(&e);
    assert(ttl >= 9 && ttl <= 10);

    expire.setExpire(&e, now - 1000);
    assert(expire.isExpired(&e) == true);
    assert(expire.getTtlSeconds(&e) == -2);
}

int main() {
    testUtils();
    std::cout << "[PASS] Utils" << std::endl;

    testDict();
    std::cout << "[PASS] Progressive Dict" << std::endl;

    testSkipList();
    std::cout << "[PASS] SkipList & SortedSet" << std::endl;

    testResp();
    std::cout << "[PASS] RESP2 Parser & Serializer" << std::endl;

    testEviction();
    std::cout << "[PASS] Eviction Engine" << std::endl;

    testExpiration();
    std::cout << "[PASS] Expiration Manager" << std::endl;

    std::cout << "\nALL UNIT TESTS PASSED SUCCESSFULLY!" << std::endl;
    return 0;
}
