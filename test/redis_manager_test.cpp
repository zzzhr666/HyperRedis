#include <gtest/gtest.h>

#include "hyper/storage/database.hpp"
#include "hyper/storage/redis_manager.hpp"

using namespace hyper;

namespace {
    [[nodiscard]] ExpireTimePoint makeTime(UnixMilliseconds ms) {
        return ExpireTimePoint{Milliseconds{ms}};
    }
}

TEST(RedisManagerTest, StartsWithDefaultDbCount) {
    RedisManager manager;

    EXPECT_EQ(manager.dbCount(), 16);
    EXPECT_NE(manager.db(0), nullptr);
    EXPECT_EQ(manager.db(16), nullptr);
}

TEST(RedisManagerTest, SupportsCustomDbCount) {
    RedisManager manager(3);

    EXPECT_EQ(manager.dbCount(), 3);
    EXPECT_NE(manager.db(0), nullptr);
    EXPECT_NE(manager.db(2), nullptr);
    EXPECT_EQ(manager.db(3), nullptr);
}

TEST(RedisManagerTest, DirectDbAccessKeepsDatabasesIndependent) {
    RedisManager manager(2);
    const auto now = makeTime(1'000);

    ASSERT_NE(manager.db(0), nullptr);
    ASSERT_NE(manager.db(1), nullptr);

    manager.db(0)->set("key", RedisObject::createSharedStringObject("zero"));
    manager.db(1)->set("key", RedisObject::createSharedStringObject("one"));

    EXPECT_EQ(manager.db(0)->get("key", now)->asString(), "zero");
    EXPECT_EQ(manager.db(1)->get("key", now)->asString(), "one");
}

TEST(RedisManagerTest, ExpirationIsIsolatedPerDbForSameKey) {
    RedisManager manager(2);
    const auto now = makeTime(1'000);

    ASSERT_NE(manager.db(0), nullptr);
    ASSERT_NE(manager.db(1), nullptr);

    manager.db(0)->set("key", RedisObject::createSharedStringObject("db0"));
    EXPECT_TRUE(manager.db(0)->expireAfter("key", Milliseconds{10}, now));
    manager.db(1)->set("key", RedisObject::createSharedStringObject("db1"));

    EXPECT_EQ(manager.db(1)->get("key", now + Milliseconds{10})->asString(), "db1");
    EXPECT_EQ(manager.db(0)->get("key", now + Milliseconds{10}), nullptr);
}

TEST(RedisManagerTest, ClearDbClearsOnlySelectedIndex) {
    RedisManager manager(2);
    const auto now = makeTime(1'000);

    ASSERT_NE(manager.db(0), nullptr);
    ASSERT_NE(manager.db(1), nullptr);

    manager.db(0)->set("key", RedisObject::createSharedStringObject("db0"));
    manager.db(1)->set("key", RedisObject::createSharedStringObject("db1"));

    EXPECT_TRUE(manager.clearDb(1));
    EXPECT_FALSE(manager.clearDb(2));

    EXPECT_EQ(manager.db(1)->size(), 0);
    EXPECT_EQ(manager.db(0)->size(), 1);
    EXPECT_EQ(manager.db(0)->get("key", now)->asString(), "db0");
}

TEST(RedisManagerTest, ClearAllClearsEveryDatabase) {
    RedisManager manager(3);
    const auto now = makeTime(1'000);

    ASSERT_NE(manager.db(0), nullptr);
    ASSERT_NE(manager.db(1), nullptr);
    ASSERT_NE(manager.db(2), nullptr);

    manager.db(0)->set("db0", RedisObject::createSharedStringObject("0"));
    manager.db(1)->set("db1", RedisObject::createSharedStringObject("1"));
    manager.db(2)->set("db2", RedisObject::createSharedStringObject("2"));

    manager.clearAll();

    EXPECT_EQ(manager.db(0)->get("db0", now), nullptr);
    EXPECT_EQ(manager.db(1)->get("db1", now), nullptr);
    EXPECT_EQ(manager.db(2)->get("db2", now), nullptr);
}
