#include <gtest/gtest.h>

#include <array>
#include <string_view>

#include "hyper/server/client_context.hpp"
#include "hyper/server/redis_server.hpp"
#include "hyper/server/resp_value.hpp"
#include "hyper/time.hpp"

using namespace hyper;

namespace {
    [[nodiscard]] ExpireTimePoint makeTime(UnixMilliseconds ms) {
        return ExpireTimePoint{Milliseconds{ms}};
    }

    void expectSimpleString(const RespValue& value, std::string_view expected) {
        const auto* simple = std::get_if<RespSimpleString>(&value);
        ASSERT_NE(simple, nullptr);
        EXPECT_EQ(simple->value, expected);
    }

    void expectBulkString(const RespValue& value, std::string_view expected) {
        const auto* bulk = std::get_if<RespBulkString>(&value);
        ASSERT_NE(bulk, nullptr);
        ASSERT_TRUE(bulk->value.has_value());
        EXPECT_EQ(*bulk->value, expected);
    }

    void expectInteger(const RespValue& value, std::int64_t expected) {
        const auto* integer = std::get_if<RespInteger>(&value);
        ASSERT_NE(integer, nullptr);
        EXPECT_EQ(integer->value, expected);
    }

    void expectError(const RespValue& value, std::string_view expected) {
        const auto* error = std::get_if<RespError>(&value);
        ASSERT_NE(error, nullptr);
        EXPECT_EQ(error->message, expected);
    }
}

TEST(RedisServerTest, SupportsCustomDbCount) {
    RedisServer server(3);

    EXPECT_EQ(server.manager().dbCount(), 3);
    EXPECT_EQ(server.dirtyCount(), 0);
}

TEST(RedisServerTest, ExecuteCanSetAndGetThroughClientContext) {
    RedisServer server;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    const std::array<std::string_view, 3> set_args{"SET", "name", "hyper"};
    const std::array<std::string_view, 2> get_args{"GET", "name"};

    expectSimpleString(server.execute(client, set_args, now), "OK");
    expectBulkString(server.execute(client, get_args, now), "hyper");
}

TEST(RedisServerTest, ReadCommandDoesNotIncreaseDirtyCount) {
    RedisServer server;
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> set_args{"SET", "name", "hyper"};
    const std::array<std::string_view, 2> get_args{"GET", "name"};

    expectSimpleString(server.execute(client, set_args, now), "OK");
    ASSERT_EQ(server.dirtyCount(), 1);

    expectBulkString(server.execute(client, get_args, now), "hyper");

    EXPECT_EQ(server.dirtyCount(), 1);
}

TEST(RedisServerTest, SuccessfulWriteCommandIncreasesDirtyCount) {
    RedisServer server;
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> first_set{"SET", "first", "1"};
    const std::array<std::string_view, 3> second_set{"SET", "second", "2"};

    expectSimpleString(server.execute(client, first_set, now), "OK");
    EXPECT_EQ(server.dirtyCount(), 1);

    expectSimpleString(server.execute(client, second_set, now), "OK");
    EXPECT_EQ(server.dirtyCount(), 2);
}

TEST(RedisServerTest, FailedWriteCommandDoesNotIncreaseDirtyCount) {
    RedisServer server;
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> set_args{"SET", "key", "not-an-int"};
    const std::array<std::string_view, 2> incr_args{"INCR", "key"};

    expectSimpleString(server.execute(client, set_args, now), "OK");
    ASSERT_EQ(server.dirtyCount(), 1);

    expectError(server.execute(client, incr_args, now),
                "ERR value is not an integer or out of range");

    EXPECT_EQ(server.dirtyCount(), 1);
}

TEST(RedisServerTest, LowercaseWriteCommandStillIncreasesDirtyCount) {
    RedisServer server;
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> set_args{"set", "key", "value"};
    const std::array<std::string_view, 2> get_args{"get", "key"};

    expectSimpleString(server.execute(client, set_args, now), "OK");
    EXPECT_EQ(server.dirtyCount(), 1);

    expectBulkString(server.execute(client, get_args, now), "value");
    EXPECT_EQ(server.dirtyCount(), 1);
}

TEST(RedisServerTest, EmptyCommandReturnsErrorWithoutIncreasingDirtyCount) {
    RedisServer server;
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 0> args{};

    expectError(server.execute(client, args, now), "ERR empty command");

    EXPECT_EQ(server.dirtyCount(), 0);
}

TEST(RedisServerTest, ActiveExpireCycleRemovesExpiredKeysAcrossDatabases) {
    RedisServer server(2);
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> set_db0{"SET", "db0-key", "zero"};
    const std::array<std::string_view, 3> expire_db0{"PEXPIRE", "db0-key", "10"};
    const std::array<std::string_view, 2> select_db1{"SELECT", "1"};
    const std::array<std::string_view, 3> set_db1{"SET", "db1-key", "one"};
    const std::array<std::string_view, 3> expire_db1{"PEXPIRE", "db1-key", "10"};

    expectSimpleString(server.execute(client, set_db0, now), "OK");
    expectInteger(server.execute(client, expire_db0, now), 1);
    expectSimpleString(server.execute(client, select_db1, now), "OK");
    expectSimpleString(server.execute(client, set_db1, now), "OK");
    expectInteger(server.execute(client, expire_db1, now), 1);

    EXPECT_EQ(server.activeExpireCycle(now + Milliseconds{10}, 10), 2);

    ASSERT_NE(server.manager().db(0), nullptr);
    ASSERT_NE(server.manager().db(1), nullptr);
    EXPECT_EQ(server.manager().db(0)->get("db0-key", now + Milliseconds{10}), nullptr);
    EXPECT_EQ(server.manager().db(1)->get("db1-key", now + Milliseconds{10}), nullptr);
}

TEST(RedisServerTest, ActiveExpireCycleIncreasesDirtyCountByDeletedKeys) {
    RedisServer server(2);
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> set_db0{"SET", "db0-key", "zero"};
    const std::array<std::string_view, 3> expire_db0{"PEXPIRE", "db0-key", "10"};
    const std::array<std::string_view, 2> select_db1{"SELECT", "1"};
    const std::array<std::string_view, 3> set_db1{"SET", "db1-key", "one"};
    const std::array<std::string_view, 3> expire_db1{"PEXPIRE", "db1-key", "10"};

    expectSimpleString(server.execute(client, set_db0, now), "OK");
    expectInteger(server.execute(client, expire_db0, now), 1);
    expectSimpleString(server.execute(client, select_db1, now), "OK");
    expectSimpleString(server.execute(client, set_db1, now), "OK");
    expectInteger(server.execute(client, expire_db1, now), 1);
    ASSERT_EQ(server.dirtyCount(), 4);

    EXPECT_EQ(server.activeExpireCycle(now + Milliseconds{10}, 10), 2);

    EXPECT_EQ(server.dirtyCount(), 6);
}
