#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "hyper/storage/database.hpp"
#include "hyper/storage/object.hpp"
#include "hyper/storage/redis_manager.hpp"
#include "hyper/storage/snapshot.hpp"

using namespace hyper;

namespace {
    [[nodiscard]] ExpireTimePoint makeTime(UnixMilliseconds ms) {
        return ExpireTimePoint{Milliseconds{ms}};
    }

    [[nodiscard]] std::map<std::string, std::string> hashToMap(const RedisObjectPtr& obj) {
        std::map<std::string, std::string> values;
        obj->hashForEach([&values](std::string_view field, const RedisObjectPtr& value) {
            values.emplace(std::string(field), value->asString());
        });
        return values;
    }

    [[nodiscard]] std::set<std::string> setToSet(const RedisObjectPtr& obj) {
        std::set<std::string> values;
        obj->setForEach([&values](std::string_view member) {
            values.emplace(member);
        });
        return values;
    }

    void expectString(RedisDb& db, std::string_view key, std::string_view expected, ExpireTimePoint now) {
        auto obj = db.get(key, now);
        ASSERT_NE(obj, nullptr);
        ASSERT_EQ(obj->getType(), ObjectType::String);
        EXPECT_EQ(obj->asString(), expected);
    }

    void expectList(RedisDb& db, std::string_view key, const std::vector<std::string>& expected,
                    ExpireTimePoint now) {
        auto obj = db.get(key, now);
        ASSERT_NE(obj, nullptr);
        ASSERT_EQ(obj->getType(), ObjectType::List);
        EXPECT_EQ(obj->listRangeAsStrings(0, -1), expected);
    }

    void expectHash(RedisDb& db, std::string_view key, const std::map<std::string, std::string>& expected,
                    ExpireTimePoint now) {
        auto obj = db.get(key, now);
        ASSERT_NE(obj, nullptr);
        ASSERT_EQ(obj->getType(), ObjectType::Hash);
        EXPECT_EQ(hashToMap(obj), expected);
    }

    void expectSet(RedisDb& db, std::string_view key, const std::set<std::string>& expected,
                   ExpireTimePoint now) {
        auto obj = db.get(key, now);
        ASSERT_NE(obj, nullptr);
        ASSERT_EQ(obj->getType(), ObjectType::Set);
        EXPECT_EQ(setToSet(obj), expected);
    }

    void expectZSet(RedisDb& db, std::string_view key,
                    const std::vector<std::pair<std::string, double>>& expected,
                    ExpireTimePoint now) {
        auto obj = db.get(key, now);
        ASSERT_NE(obj, nullptr);
        ASSERT_EQ(obj->getType(), ObjectType::ZSet);

        auto actual = obj->zSetRange(0, -1);
        ASSERT_EQ(actual.size(), expected.size());
        for (std::size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(actual[i].first, expected[i].first);
            EXPECT_DOUBLE_EQ(actual[i].second, expected[i].second);
        }
    }
}

TEST(SnapshotTest, EmptyManagerRoundTrips) {
    RedisManager source(3);
    RedisManager target(3);
    const auto now = makeTime(1'000);
    Snapshot snapshot;

    auto bytes = snapshot.save(source, now);

    ASSERT_FALSE(bytes.empty());
    EXPECT_TRUE(snapshot.load(bytes, target, now));
    ASSERT_EQ(target.dbCount(), 3U);
    for (std::size_t i = 0; i < target.dbCount(); ++i) {
        ASSERT_NE(target.db(i), nullptr);
        EXPECT_EQ(target.db(i)->size(), 0U);
    }
}

TEST(SnapshotTest, AllSupportedObjectTypesRoundTripAcrossDatabases) {
    RedisManager source(3);
    const auto now = makeTime(1'000);
    ASSERT_NE(source.db(0), nullptr);
    ASSERT_NE(source.db(1), nullptr);
    ASSERT_NE(source.db(2), nullptr);

    source.db(0)->set("string", RedisObject::createSharedStringObject("value"));
    source.db(0)->set("integer-string", RedisObject::createSharedStringObject("123"));

    auto list = RedisObject::createSharedListObject();
    list->listRightPush(RedisObject::createSharedStringObject("left"));
    list->listRightPush(RedisObject::createSharedStringObject("middle"));
    list->listRightPush(RedisObject::createSharedStringObject("right"));
    source.db(0)->set("list", list);

    auto hash = RedisObject::createSharedHashObject();
    hash->hashSet("field-2", RedisObject::createSharedStringObject("two"));
    hash->hashSet("field-1", RedisObject::createSharedStringObject("one"));
    source.db(1)->set("hash", hash);

    auto set = RedisObject::createSharedSetObject();
    set->setAdd("gamma");
    set->setAdd("alpha");
    set->setAdd("beta");
    source.db(1)->set("set", set);

    auto zset = RedisObject::createSharedZSetObject();
    zset->zSetAdd("two", 2.0);
    zset->zSetAdd("one", 1.0);
    zset->zSetAdd("also-two", 2.0);
    source.db(2)->set("zset", zset);

    RedisManager target(3);
    target.db(0)->set("stale", RedisObject::createSharedStringObject("old"));
    target.db(1)->set("stale", RedisObject::createSharedStringObject("old"));

    Snapshot snapshot;
    auto bytes = snapshot.save(source, now);
    ASSERT_TRUE(snapshot.load(bytes, target, now));

    EXPECT_EQ(target.db(0)->get("stale", now), nullptr);
    EXPECT_EQ(target.db(1)->get("stale", now), nullptr);

    expectString(*target.db(0), "string", "value", now);
    expectString(*target.db(0), "integer-string", "123", now);
    expectList(*target.db(0), "list", {"left", "middle", "right"}, now);
    expectHash(*target.db(1), "hash", {{"field-1", "one"}, {"field-2", "two"}}, now);
    expectSet(*target.db(1), "set", {"alpha", "beta", "gamma"}, now);
    expectZSet(*target.db(2), "zset", {{"one", 1.0}, {"also-two", 2.0}, {"two", 2.0}}, now);
}

TEST(SnapshotTest, ExpirationMetadataRoundTripsAndExpiredKeysAreSkipped) {
    RedisManager source(2);
    const auto now = makeTime(1'000);
    const auto save_time = now + Milliseconds{20};
    ASSERT_NE(source.db(0), nullptr);

    source.db(0)->set("plain", RedisObject::createSharedStringObject("plain"));
    source.db(0)->set("volatile", RedisObject::createSharedStringObject("volatile"));
    source.db(0)->set("expired", RedisObject::createSharedStringObject("expired"));

    ASSERT_TRUE(source.db(0)->expireAfter("volatile", Milliseconds{5'000}, now));
    ASSERT_TRUE(source.db(0)->expireAfter("expired", Milliseconds{10}, now));

    Snapshot snapshot;
    auto bytes = snapshot.save(source, save_time);

    RedisManager target(2);
    ASSERT_TRUE(snapshot.load(bytes, target, save_time));
    ASSERT_NE(target.db(0), nullptr);

    expectString(*target.db(0), "plain", "plain", save_time);
    EXPECT_EQ(target.db(0)->pttl("plain", save_time), -1);

    expectString(*target.db(0), "volatile", "volatile", save_time);
    EXPECT_EQ(target.db(0)->pttl("volatile", save_time), 4'980);
    EXPECT_NE(target.db(0)->get("volatile", now + Milliseconds{4'999}), nullptr);
    EXPECT_EQ(target.db(0)->get("volatile", now + Milliseconds{5'000}), nullptr);

    EXPECT_EQ(target.db(0)->get("expired", save_time), nullptr);
    EXPECT_EQ(target.db(0)->pttl("expired", save_time), -2);
}

TEST(SnapshotTest, LoadRejectsBadDataWithoutChangingExistingManager) {
    RedisManager target(2);
    const auto now = makeTime(1'000);
    ASSERT_NE(target.db(0), nullptr);
    ASSERT_NE(target.db(1), nullptr);
    target.db(0)->set("keep", RedisObject::createSharedStringObject("db0"));
    target.db(1)->set("keep", RedisObject::createSharedStringObject("db1"));

    Snapshot snapshot;
    const std::vector<std::uint8_t> bad_magic{'B', 'A', 'D'};

    EXPECT_FALSE(snapshot.load(bad_magic, target, now));
    expectString(*target.db(0), "keep", "db0", now);
    expectString(*target.db(1), "keep", "db1", now);

    RedisManager source(2);
    ASSERT_NE(source.db(0), nullptr);
    source.db(0)->set("new", RedisObject::createSharedStringObject("value"));
    auto bytes = snapshot.save(source, now);
    ASSERT_GT(bytes.size(), 1U);
    bytes.pop_back();

    EXPECT_FALSE(snapshot.load(bytes, target, now));
    expectString(*target.db(0), "keep", "db0", now);
    expectString(*target.db(1), "keep", "db1", now);
    EXPECT_EQ(target.db(0)->get("new", now), nullptr);
}

TEST(SnapshotTest, SnapshotBytesAreStableAcrossInsertionOrder) {
    RedisManager first(1);
    RedisManager second(1);
    const auto now = makeTime(1'000);
    ASSERT_NE(first.db(0), nullptr);
    ASSERT_NE(second.db(0), nullptr);

    first.db(0)->set("b", RedisObject::createSharedStringObject("2"));
    first.db(0)->set("a", RedisObject::createSharedStringObject("1"));
    auto first_hash = RedisObject::createSharedHashObject();
    first_hash->hashSet("y", RedisObject::createSharedStringObject("2"));
    first_hash->hashSet("x", RedisObject::createSharedStringObject("1"));
    first.db(0)->set("hash", first_hash);
    auto first_set = RedisObject::createSharedSetObject();
    first_set->setAdd("c");
    first_set->setAdd("a");
    first_set->setAdd("b");
    first.db(0)->set("set", first_set);

    auto second_hash = RedisObject::createSharedHashObject();
    second_hash->hashSet("x", RedisObject::createSharedStringObject("1"));
    second_hash->hashSet("y", RedisObject::createSharedStringObject("2"));
    second.db(0)->set("hash", second_hash);
    second.db(0)->set("a", RedisObject::createSharedStringObject("1"));
    auto second_set = RedisObject::createSharedSetObject();
    second_set->setAdd("b");
    second_set->setAdd("c");
    second_set->setAdd("a");
    second.db(0)->set("set", second_set);
    second.db(0)->set("b", RedisObject::createSharedStringObject("2"));

    Snapshot snapshot;
    EXPECT_EQ(snapshot.save(first, now), snapshot.save(second, now));
}
