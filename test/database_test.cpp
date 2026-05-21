#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "hyper/storage/database.hpp"

using namespace hyper;

namespace {
    [[nodiscard]] ExpireTimePoint makeTime(UnixMilliseconds ms) {
        return ExpireTimePoint{Milliseconds{ms}};
    }
}

TEST(DatabaseTest, SetGetDelAndExists) {
    RedisDb db;
    const auto now = makeTime(1'000);

    EXPECT_EQ(db.size(), 0);
    EXPECT_FALSE(db.exists("key", now));
    EXPECT_EQ(db.get("key", now), nullptr);

    db.set("key", RedisObject::createSharedStringObject("value"));

    EXPECT_EQ(db.size(), 1);
    EXPECT_TRUE(db.exists("key", now));
    const auto value = db.get("key", now);
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(value->asString(), "value");

    EXPECT_TRUE(db.del("key"));
    EXPECT_FALSE(db.del("key"));
    EXPECT_EQ(db.size(), 0);
    EXPECT_FALSE(db.exists("key", now));
}

TEST(DatabaseTest, SetOverwritesExistingValue) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("key", RedisObject::createSharedStringObject("old"));
    db.set("key", RedisObject::createSharedStringObject("new"));

    EXPECT_EQ(db.size(), 1);
    const auto value = db.get("key", now);
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(value->asString(), "new");
}

TEST(DatabaseTest, ExpireAtRemovesKeyAtDeadline) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("key", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAt("key", now, now + Milliseconds{10}));

    EXPECT_TRUE(db.exists("key", now + Milliseconds{9}));
    EXPECT_NE(db.get("key", now + Milliseconds{9}), nullptr);

    EXPECT_FALSE(db.exists("key", now + Milliseconds{10}));
    EXPECT_EQ(db.get("key", now + Milliseconds{10}), nullptr);
    EXPECT_EQ(db.size(), 0);
}

TEST(DatabaseTest, ExpireAfterUsesRelativeDuration) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("key", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("key", Milliseconds{25}, now));

    EXPECT_NE(db.get("key", now + Milliseconds{24}), nullptr);
    EXPECT_EQ(db.get("key", now + Milliseconds{25}), nullptr);
}

TEST(DatabaseTest, ExpireWithNonPositiveTtlDeletesKeyImmediately) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("zero", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("zero", Milliseconds{0}, now));
    EXPECT_EQ(db.size(), 0);
    EXPECT_FALSE(db.exists("zero", now));
    EXPECT_EQ(db.pttl("zero", now), -2);

    db.set("negative", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("negative", Milliseconds{-1}, now));
    EXPECT_EQ(db.size(), 0);
    EXPECT_FALSE(db.exists("negative", now));
    EXPECT_EQ(db.pttl("negative", now), -2);
}

TEST(DatabaseTest, ExpireAtPastDeadlineDeletesKeyImmediately) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("key", RedisObject::createSharedStringObject("value"));

    EXPECT_TRUE(db.expireAt("key", now, now - Milliseconds{1}));
    EXPECT_EQ(db.size(), 0);
    EXPECT_EQ(db.get("key", now), nullptr);
    EXPECT_EQ(db.pttl("key", now), -2);
}

TEST(DatabaseTest, ExpireConditionsAreCheckedBeforeImmediateDelete) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("nx", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("nx", Milliseconds{0}, now, RedisDb::ExpireCondition::NX));
    EXPECT_FALSE(db.exists("nx", now));

    db.set("xx", RedisObject::createSharedStringObject("value"));
    EXPECT_FALSE(db.expireAfter("xx", Milliseconds{0}, now, RedisDb::ExpireCondition::XX));
    EXPECT_TRUE(db.exists("xx", now));
    EXPECT_EQ(db.pttl("xx", now), -1);

    db.set("gt", RedisObject::createSharedStringObject("value"));
    EXPECT_FALSE(db.expireAfter("gt", Milliseconds{0}, now, RedisDb::ExpireCondition::GT));
    EXPECT_TRUE(db.exists("gt", now));
    EXPECT_EQ(db.pttl("gt", now), -1);

    db.set("lt", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("lt", Milliseconds{0}, now, RedisDb::ExpireCondition::LT));
    EXPECT_FALSE(db.exists("lt", now));
}

TEST(DatabaseTest, ExpireReturnsFalseForMissingOrAlreadyExpiredKey) {
    RedisDb db;
    const auto now = makeTime(1'000);

    EXPECT_FALSE(db.expireAt("missing", now, now + Milliseconds{10}));
    EXPECT_FALSE(db.expireAfter("missing", Milliseconds{10}, now));

    db.set("key", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAt("key", now, now + Milliseconds{10}));

    EXPECT_FALSE(db.expireAt("key", now + Milliseconds{10}, now + Milliseconds{30}));
    EXPECT_EQ(db.get("key", now + Milliseconds{10}), nullptr);
}

TEST(DatabaseTest, SetClearsPreviousExpiration) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("key", RedisObject::createSharedStringObject("old"));
    EXPECT_TRUE(db.expireAfter("key", Milliseconds{10}, now));

    db.set("key", RedisObject::createSharedStringObject("new"));

    const auto value = db.get("key", now + Milliseconds{10});
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(value->asString(), "new");
}

TEST(DatabaseTest, PttlAndTtlMatchRedisExpirationSemantics) {
    RedisDb db;
    const auto now = makeTime(1'000);

    EXPECT_EQ(db.pttl("missing", now), -2);
    EXPECT_EQ(db.ttl("missing", now), -2);

    db.set("key", RedisObject::createSharedStringObject("value"));
    EXPECT_EQ(db.pttl("key", now), -1);
    EXPECT_EQ(db.ttl("key", now), -1);

    EXPECT_TRUE(db.expireAfter("key", Milliseconds{2'500}, now));
    EXPECT_EQ(db.pttl("key", now), 2'500);
    EXPECT_EQ(db.ttl("key", now), 3);

    EXPECT_EQ(db.pttl("key", now + Milliseconds{1'000}), 1'500);
    EXPECT_EQ(db.ttl("key", now + Milliseconds{1'000}), 2);

    EXPECT_EQ(db.pttl("key", now + Milliseconds{1'001}), 1'499);
    EXPECT_EQ(db.ttl("key", now + Milliseconds{1'001}), 1);

    EXPECT_EQ(db.pttl("key", now + Milliseconds{2'500}), -2);
    EXPECT_EQ(db.ttl("key", now + Milliseconds{2'500}), -2);
    EXPECT_EQ(db.size(), 0);
}

TEST(DatabaseTest, ExpireTimeReturnsAbsoluteUnixMillisecondsForLiveVolatileKeys) {
    RedisDb db;
    const auto now = makeTime(1'000);

    EXPECT_FALSE(db.expireTime("missing", now).has_value());

    db.set("plain", RedisObject::createSharedStringObject("value"));
    EXPECT_FALSE(db.expireTime("plain", now).has_value());

    db.set("volatile", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("volatile", Milliseconds{2'500}, now));
    ASSERT_TRUE(db.expireTime("volatile", now).has_value());
    EXPECT_EQ(db.expireTime("volatile", now).value(), 3'500);
    EXPECT_EQ(db.expireTime("volatile", now + Milliseconds{1'000}).value(), 3'500);
    EXPECT_FALSE(db.expireTime("volatile", now + Milliseconds{2'500}).has_value());
}

TEST(DatabaseTest, PersistClearsExpirationForLiveKeysOnly) {
    RedisDb db;
    const auto now = makeTime(1'000);

    EXPECT_FALSE(db.persist("missing", now));

    db.set("plain", RedisObject::createSharedStringObject("value"));
    EXPECT_FALSE(db.persist("plain", now));
    EXPECT_EQ(db.pttl("plain", now), -1);

    db.set("key", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("key", Milliseconds{10}, now));
    EXPECT_TRUE(db.persist("key", now + Milliseconds{5}));
    EXPECT_EQ(db.pttl("key", now + Milliseconds{5}), -1);
    EXPECT_NE(db.get("key", now + Milliseconds{10}), nullptr);
    EXPECT_FALSE(db.persist("key", now + Milliseconds{10}));

    db.set("expired", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("expired", Milliseconds{10}, now));
    EXPECT_FALSE(db.persist("expired", now + Milliseconds{10}));
    EXPECT_EQ(db.get("expired", now + Milliseconds{10}), nullptr);
}

TEST(DatabaseTest, ExpireConditionNxOnlySetsWhenKeyHasNoExpiration) {
    RedisDb db;
    const auto now = makeTime(1'000);

    EXPECT_FALSE(db.expireAfter("missing", Milliseconds{100}, now, RedisDb::ExpireCondition::NX));

    db.set("key", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("key", Milliseconds{100}, now, RedisDb::ExpireCondition::NX));
    EXPECT_EQ(db.pttl("key", now), 100);

    EXPECT_FALSE(db.expireAfter("key", Milliseconds{200}, now, RedisDb::ExpireCondition::NX));
    EXPECT_EQ(db.pttl("key", now), 100);
}

TEST(DatabaseTest, ExpireConditionXxOnlySetsWhenKeyHasExpiration) {
    RedisDb db;
    const auto now = makeTime(1'000);

    EXPECT_FALSE(db.expireAfter("missing", Milliseconds{100}, now, RedisDb::ExpireCondition::XX));

    db.set("key", RedisObject::createSharedStringObject("value"));
    EXPECT_FALSE(db.expireAfter("key", Milliseconds{100}, now, RedisDb::ExpireCondition::XX));
    EXPECT_EQ(db.pttl("key", now), -1);

    EXPECT_TRUE(db.expireAfter("key", Milliseconds{100}, now));
    EXPECT_TRUE(db.expireAfter("key", Milliseconds{200}, now, RedisDb::ExpireCondition::XX));
    EXPECT_EQ(db.pttl("key", now), 200);
}

TEST(DatabaseTest, ExpireConditionGtOnlyExtendsExistingExpiration) {
    RedisDb db;
    const auto now = makeTime(1'000);

    EXPECT_FALSE(db.expireAfter("missing", Milliseconds{100}, now, RedisDb::ExpireCondition::GT));

    db.set("key", RedisObject::createSharedStringObject("value"));
    EXPECT_FALSE(db.expireAfter("key", Milliseconds{100}, now, RedisDb::ExpireCondition::GT));
    EXPECT_EQ(db.pttl("key", now), -1);

    EXPECT_TRUE(db.expireAfter("key", Milliseconds{100}, now));
    EXPECT_FALSE(db.expireAfter("key", Milliseconds{50}, now, RedisDb::ExpireCondition::GT));
    EXPECT_EQ(db.pttl("key", now), 100);

    EXPECT_FALSE(db.expireAfter("key", Milliseconds{100}, now, RedisDb::ExpireCondition::GT));
    EXPECT_EQ(db.pttl("key", now), 100);

    EXPECT_TRUE(db.expireAfter("key", Milliseconds{200}, now, RedisDb::ExpireCondition::GT));
    EXPECT_EQ(db.pttl("key", now), 200);
}

TEST(DatabaseTest, ExpireConditionLtOnlyShortensExistingExpiration) {
    RedisDb db;
    const auto now = makeTime(1'000);

    EXPECT_FALSE(db.expireAfter("missing", Milliseconds{100}, now, RedisDb::ExpireCondition::LT));

    db.set("key", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("key", Milliseconds{300}, now, RedisDb::ExpireCondition::LT));
    EXPECT_EQ(db.pttl("key", now), 300);

    EXPECT_FALSE(db.expireAfter("key", Milliseconds{500}, now, RedisDb::ExpireCondition::LT));
    EXPECT_EQ(db.pttl("key", now), 300);

    EXPECT_FALSE(db.expireAfter("key", Milliseconds{300}, now, RedisDb::ExpireCondition::LT));
    EXPECT_EQ(db.pttl("key", now), 300);

    EXPECT_TRUE(db.expireAfter("key", Milliseconds{100}, now, RedisDb::ExpireCondition::LT));
    EXPECT_EQ(db.pttl("key", now), 100);
}

TEST(DatabaseTest, ActiveExpireCycleDoesNothingWhenMaxChecksIsZero) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("key", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("key", Milliseconds{10}, now));

    EXPECT_EQ(db.activeExpireCycle(now + Milliseconds{10}, 0), 0);
    EXPECT_EQ(db.size(), 1);
}

TEST(DatabaseTest, ActiveExpireCycleIgnoresLiveExpiringKeys) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("key", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("key", Milliseconds{100}, now));

    EXPECT_EQ(db.activeExpireCycle(now + Milliseconds{50}, 10), 0);
    EXPECT_EQ(db.size(), 1);
}

TEST(DatabaseTest, ActiveExpireCycleRemovesExpiredKeysUpToMaxChecks) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("a", RedisObject::createSharedStringObject("1"));
    db.set("b", RedisObject::createSharedStringObject("2"));
    db.set("c", RedisObject::createSharedStringObject("3"));

    EXPECT_TRUE(db.expireAfter("a", Milliseconds{10}, now));
    EXPECT_TRUE(db.expireAfter("b", Milliseconds{10}, now));
    EXPECT_TRUE(db.expireAfter("c", Milliseconds{10}, now));

    EXPECT_EQ(db.activeExpireCycle(now + Milliseconds{10}, 2), 2);
    EXPECT_EQ(db.size(), 1);

    EXPECT_EQ(db.activeExpireCycle(now + Milliseconds{10}, 2), 1);
    EXPECT_EQ(db.size(), 0);
}

TEST(DatabaseTest, TypeReturnsObjectTypeAndTreatsMissingOrExpiredKeysAsEmpty) {
    RedisDb db;
    const auto now = makeTime(1'000);

    EXPECT_FALSE(db.type("missing", now).has_value());

    db.set("string", RedisObject::createSharedStringObject("value"));
    db.set("hash", RedisObject::createSharedHashObject());
    db.set("list", RedisObject::createSharedListObject());
    db.set("set", RedisObject::createSharedSetObject());
    db.set("zset", RedisObject::createSharedZSetObject());

    EXPECT_EQ(db.type("string", now), ObjectType::String);
    EXPECT_EQ(db.type("hash", now), ObjectType::Hash);
    EXPECT_EQ(db.type("list", now), ObjectType::List);
    EXPECT_EQ(db.type("set", now), ObjectType::Set);
    EXPECT_EQ(db.type("zset", now), ObjectType::ZSet);

    db.set("expired", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("expired", Milliseconds{10}, now));
    EXPECT_FALSE(db.type("expired", now + Milliseconds{10}).has_value());
    EXPECT_FALSE(db.exists("expired", now + Milliseconds{10}));
}

TEST(DatabaseTest, RenameMovesValueAndRemovesOldKey) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("old", RedisObject::createSharedStringObject("value"));

    EXPECT_TRUE(db.rename("old", "new", now));

    EXPECT_FALSE(db.exists("old", now));
    EXPECT_EQ(db.get("old", now), nullptr);
    EXPECT_EQ(db.size(), 1);

    const auto value = db.get("new", now);
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(value->asString(), "value");
}

TEST(DatabaseTest, RenameMovesExpirationToNewKey) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("old", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("old", Milliseconds{100}, now));

    EXPECT_TRUE(db.rename("old", "new", now));

    EXPECT_EQ(db.pttl("old", now), -2);
    EXPECT_EQ(db.pttl("new", now), 100);
    EXPECT_EQ(db.get("new", now + Milliseconds{99})->asString(), "value");
    EXPECT_EQ(db.get("new", now + Milliseconds{100}), nullptr);
    EXPECT_EQ(db.size(), 0);
}

TEST(DatabaseTest, RenameSameKeyIsNoOpAndKeepsExpiration) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("key", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("key", Milliseconds{100}, now));

    EXPECT_TRUE(db.rename("key", "key", now + Milliseconds{10}));

    EXPECT_EQ(db.size(), 1);
    EXPECT_TRUE(db.exists("key", now + Milliseconds{10}));
    EXPECT_EQ(db.pttl("key", now + Milliseconds{10}), 90);
    EXPECT_EQ(db.get("key", now + Milliseconds{10})->asString(), "value");
}

TEST(DatabaseTest, RenameOverwrittenDestinationInheritsSourceExpiration) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("old", RedisObject::createSharedStringObject("old-value"));
    db.set("new", RedisObject::createSharedStringObject("new-value"));
    EXPECT_TRUE(db.expireAfter("old", Milliseconds{100}, now));
    EXPECT_TRUE(db.expireAfter("new", Milliseconds{10}, now));

    EXPECT_TRUE(db.rename("old", "new", now + Milliseconds{5}));

    EXPECT_FALSE(db.exists("old", now + Milliseconds{5}));
    EXPECT_EQ(db.pttl("new", now + Milliseconds{5}), 95);
    EXPECT_EQ(db.get("new", now + Milliseconds{10})->asString(), "old-value");
    EXPECT_EQ(db.get("new", now + Milliseconds{100}), nullptr);
    EXPECT_EQ(db.size(), 0);
}

TEST(DatabaseTest, RenameOverwritesDestinationAndReplacesItsExpiration) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("old", RedisObject::createSharedStringObject("old-value"));
    db.set("new", RedisObject::createSharedStringObject("new-value"));
    EXPECT_TRUE(db.expireAfter("new", Milliseconds{10}, now));

    EXPECT_TRUE(db.rename("old", "new", now));

    EXPECT_FALSE(db.exists("old", now));
    EXPECT_EQ(db.pttl("new", now), -1);

    const auto value = db.get("new", now + Milliseconds{10});
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(value->asString(), "old-value");
    EXPECT_EQ(db.size(), 1);
}

TEST(DatabaseTest, RenameReturnsFalseForMissingOrExpiredSource) {
    RedisDb db;
    const auto now = makeTime(1'000);

    EXPECT_FALSE(db.rename("missing", "new", now));

    db.set("old", RedisObject::createSharedStringObject("value"));
    EXPECT_TRUE(db.expireAfter("old", Milliseconds{10}, now));

    EXPECT_FALSE(db.rename("old", "new", now + Milliseconds{10}));
    EXPECT_FALSE(db.exists("old", now + Milliseconds{10}));
    EXPECT_FALSE(db.exists("new", now + Milliseconds{10}));
}

TEST(DatabaseTest, ClearRemovesValuesAndExpirations) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("plain", RedisObject::createSharedStringObject("value"));
    db.set("volatile", RedisObject::createSharedStringObject("expires"));
    EXPECT_TRUE(db.expireAfter("volatile", Milliseconds{100}, now));

    db.clear();

    EXPECT_EQ(db.size(), 0);
    EXPECT_EQ(db.get("plain", now), nullptr);
    EXPECT_EQ(db.get("volatile", now), nullptr);
    EXPECT_EQ(db.pttl("volatile", now), -2);
    EXPECT_EQ(db.randomKey(now), std::nullopt);
}

TEST(DatabaseTest, RandomKeyReturnsOnlyLiveKeys) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("expired", RedisObject::createSharedStringObject("expired"));
    db.set("live", RedisObject::createSharedStringObject("live"));
    EXPECT_TRUE(db.expireAfter("expired", Milliseconds{10}, now));

    const auto key = db.randomKey(now + Milliseconds{10});

    ASSERT_TRUE(key.has_value());
    EXPECT_EQ(*key, "live");
}

TEST(DatabaseTest, RandomKeyReturnsNulloptForEmptyOrOnlyExpiredDb) {
    RedisDb db;
    const auto now = makeTime(1'000);

    EXPECT_EQ(db.randomKey(now), std::nullopt);

    db.set("a", RedisObject::createSharedStringObject("1"));
    db.set("b", RedisObject::createSharedStringObject("2"));
    EXPECT_TRUE(db.expireAfter("a", Milliseconds{10}, now));
    EXPECT_TRUE(db.expireAfter("b", Milliseconds{10}, now));

    EXPECT_EQ(db.randomKey(now + Milliseconds{10}), std::nullopt);
    EXPECT_EQ(db.size(), 0);
}

TEST(DatabaseTest, ForEachVisitsOnlyLiveKeys) {
    RedisDb db;
    const auto now = makeTime(1'000);

    db.set("a", RedisObject::createSharedStringObject("1"));
    db.set("b", RedisObject::createSharedStringObject("2"));
    db.set("expired", RedisObject::createSharedStringObject("expired"));
    EXPECT_TRUE(db.expireAfter("expired", Milliseconds{10}, now));

    std::vector<std::pair<std::string, std::string>> visited;
    db.forEach(now + Milliseconds{10},
               [&visited](std::string_view key, const RedisObjectPtr& value) {
                   visited.emplace_back(std::string(key), value->asString());
               });

    std::sort(visited.begin(), visited.end());

    ASSERT_EQ(visited.size(), 2);
    EXPECT_EQ(visited[0], std::make_pair(std::string("a"), std::string("1")));
    EXPECT_EQ(visited[1], std::make_pair(std::string("b"), std::string("2")));
    EXPECT_EQ(db.size(), 2);
    EXPECT_FALSE(db.exists("expired", now + Milliseconds{10}));
}
