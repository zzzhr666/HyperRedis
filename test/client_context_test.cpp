#include <gtest/gtest.h>

#include "hyper/server/client_context.hpp"
#include "hyper/storage/database.hpp"
#include "hyper/storage/redis_manager.hpp"

using namespace hyper;

namespace {
    [[nodiscard]] ExpireTimePoint makeTime(UnixMilliseconds ms) {
        return ExpireTimePoint{Milliseconds{ms}};
    }
}

TEST(ClientContextTest, StartsAtDbZero) {
    RedisManager manager(3);
    RedisClientContext client;

    EXPECT_EQ(client.dbIndex(), 0);
    EXPECT_EQ(client.currentDb(manager), manager.db(0));
}

TEST(ClientContextTest, SelectDbUpdatesCurrentIndexForValidDatabase) {
    RedisManager manager(3);
    RedisClientContext client;

    EXPECT_TRUE(client.selectDb(manager, 2));

    EXPECT_EQ(client.dbIndex(), 2);
    EXPECT_EQ(client.currentDb(manager), manager.db(2));
}

TEST(ClientContextTest, SelectDbRejectsOutOfRangeIndexAndKeepsPreviousDb) {
    RedisManager manager(2);
    RedisClientContext client;

    ASSERT_TRUE(client.selectDb(manager, 1));

    EXPECT_FALSE(client.selectDb(manager, 2));
    EXPECT_EQ(client.dbIndex(), 1);
    EXPECT_EQ(client.currentDb(manager), manager.db(1));
}

TEST(ClientContextTest, CurrentDbSupportsConstManagerAccess) {
    RedisManager manager(2);
    RedisClientContext client;
    ASSERT_TRUE(client.selectDb(manager, 1));

    const RedisManager& const_manager = manager;

    EXPECT_EQ(client.currentDb(const_manager), const_manager.db(1));
}

TEST(ClientContextTest, MultipleClientsKeepIndependentDbSelections) {
    RedisManager manager(2);
    RedisClientContext first;
    RedisClientContext second;
    const auto now = makeTime(1'000);

    ASSERT_NE(first.currentDb(manager), nullptr);
    ASSERT_TRUE(second.selectDb(manager, 1));
    ASSERT_NE(second.currentDb(manager), nullptr);

    first.currentDb(manager)->set("key", RedisObject::createSharedStringObject("db0"));
    second.currentDb(manager)->set("key", RedisObject::createSharedStringObject("db1"));

    EXPECT_EQ(first.dbIndex(), 0);
    EXPECT_EQ(second.dbIndex(), 1);
    EXPECT_EQ(first.currentDb(manager)->get("key", now)->asString(), "db0");
    EXPECT_EQ(second.currentDb(manager)->get("key", now)->asString(), "db1");
}
