#include <gtest/gtest.h>

#include "hyper/server/client_context.hpp"
#include "hyper/server/command_executor.hpp"
#include "hyper/server/resp_value.hpp"
#include "hyper/storage/database.hpp"
#include "hyper/storage/redis_manager.hpp"

#include <array>
#include <span>
#include <string>
#include <string_view>

using namespace hyper;

namespace {
    [[nodiscard]] ExpireTimePoint makeTime(UnixMilliseconds ms) {
        return ExpireTimePoint{Milliseconds{ms}};
    }

    template<std::size_t N>
    [[nodiscard]] RespValue execute(CommandExecutor& executor,
                                    RedisManager& manager,
                                    RedisClientContext& client,
                                    const std::array<std::string_view, N>& args,
                                    ExpireTimePoint now = makeTime(1'000)) {
        return executor.execute(manager, client, std::span<const std::string_view>{args.data(), args.size()}, now);
    }

    void expectSimpleString(const RespValue& value, std::string_view expected) {
        const auto* simple_string = std::get_if<RespSimpleString>(&value);
        ASSERT_NE(simple_string, nullptr);
        EXPECT_EQ(simple_string->value, expected);
    }

    void expectBulkString(const RespValue& value, std::string_view expected) {
        const auto* bulk_string = std::get_if<RespBulkString>(&value);
        ASSERT_NE(bulk_string, nullptr);
        ASSERT_TRUE(bulk_string->value.has_value());
        EXPECT_EQ(*bulk_string->value, expected);
    }

    void expectNullBulkString(const RespValue& value) {
        const auto* bulk_string = std::get_if<RespBulkString>(&value);
        ASSERT_NE(bulk_string, nullptr);
        EXPECT_FALSE(bulk_string->value.has_value());
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

TEST(CommandExecutorTest, EmptyCommandReturnsError) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const std::array<std::string_view, 0> args{};

    expectError(execute(executor, manager, client, args), "ERR empty command");
}

TEST(CommandExecutorTest, UnknownCommandReturnsError) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const std::array<std::string_view, 1> args{"NO_SUCH_COMMAND"};

    expectError(execute(executor, manager, client, args), "ERR unknown command");
}

TEST(CommandExecutorTest, WrongArityReturnsError) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;

    expectError(execute(executor, manager, client, std::array<std::string_view, 1>{"GET"}),
                "ERR wrong number of arguments");
    expectError(execute(executor, manager, client, std::array<std::string_view, 2>{"SET", "key"}),
                "ERR wrong number of arguments");
    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"PING", "one", "two"}),
                "ERR wrong number of arguments");
}

TEST(CommandExecutorTest, PingReturnsPongOrEchoesMessage) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 1>{"PING"}), "PONG");
    expectBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"PING", "hello"}), "hello");
}

TEST(CommandExecutorTest, SetAndGetStringValues) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "key", "value"}), "OK");
    expectBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "key"}), "value");
    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "missing"}));
}

TEST(CommandExecutorTest, CommandsAreCaseInsensitive) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"set", "key", "value"}), "OK");
    expectBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"gEt", "key"}), "value");
}

TEST(CommandExecutorTest, DelReturnsNumberOfDeletedKeys) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;

    ASSERT_NE(client.currentDb(manager), nullptr);
    client.currentDb(manager)->set("one", RedisObject::createSharedStringObject("1"));
    client.currentDb(manager)->set("two", RedisObject::createSharedStringObject("2"));

    expectInteger(execute(executor, manager, client, std::array<std::string_view, 4>{"DEL", "one", "missing", "two"}), 2);
    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "one"}));
    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "two"}));
}

TEST(CommandExecutorTest, DelDoesNotCountExpiredKeys) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    auto* db = client.currentDb(manager);
    ASSERT_NE(db, nullptr);

    const auto now = makeTime(1'000);
    db->set("expired", RedisObject::createSharedStringObject("expired"));
    db->set("live", RedisObject::createSharedStringObject("live"));
    ASSERT_TRUE(db->expireAfter("expired", Milliseconds{10}, now));

    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"DEL", "expired", "live"},
                          now + Milliseconds{10}), 1);
    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "expired"},
                                 now + Milliseconds{10}));
    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "live"},
                                 now + Milliseconds{10}));
}

TEST(CommandExecutorTest, ExistsReturnsNumberOfExistingKeys) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;

    client.currentDb(manager)->set("one", RedisObject::createSharedStringObject("1"));
    client.currentDb(manager)->set("two", RedisObject::createSharedStringObject("2"));

    expectInteger(execute(executor, manager, client, std::array<std::string_view, 4>{"EXISTS", "one", "missing", "two"}), 2);
}

TEST(CommandExecutorTest, SelectChangesOnlyTheClientCurrentDb) {
    CommandExecutor executor;
    RedisManager manager(2);
    RedisClientContext client;

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"SELECT", "1"}), "OK");
    EXPECT_EQ(client.dbIndex(), 1);

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "key", "db1"}), "OK");
    expectBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "key"}), "db1");

    EXPECT_EQ(manager.db(0)->get("key", makeTime(1'000)), nullptr);
    ASSERT_NE(manager.db(1)->get("key", makeTime(1'000)), nullptr);
    EXPECT_EQ(manager.db(1)->get("key", makeTime(1'000))->asString(), "db1");
}

TEST(CommandExecutorTest, SelectRejectsInvalidDbIndexWithoutChangingClient) {
    CommandExecutor executor;
    RedisManager manager(2);
    RedisClientContext client;

    ASSERT_TRUE(client.selectDb(manager, 1));

    expectError(execute(executor, manager, client, std::array<std::string_view, 2>{"SELECT", "2"}),
                "ERR invalid DB index");
    EXPECT_EQ(client.dbIndex(), 1);

    expectError(execute(executor, manager, client, std::array<std::string_view, 2>{"SELECT", "not-an-integer"}),
                "ERR invalid DB index");
    EXPECT_EQ(client.dbIndex(), 1);
}

TEST(CommandExecutorTest, GetReturnsWrongTypeErrorForNonStringValue) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;

    ASSERT_NE(client.currentDb(manager), nullptr);
    client.currentDb(manager)->set("hash", RedisObject::createSharedHashObject());

    expectError(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "hash"}),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
}

TEST(CommandExecutorTest, TypeReturnsTypeNameOrNone) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    auto* db = client.currentDb(manager);
    ASSERT_NE(db, nullptr);

    const auto now = makeTime(1'000);
    db->set("string", RedisObject::createSharedStringObject("value"));
    db->set("hash", RedisObject::createSharedHashObject());
    db->set("list", RedisObject::createSharedListObject());
    db->set("set", RedisObject::createSharedSetObject());
    db->set("zset", RedisObject::createSharedZSetObject());

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"TYPE", "string"}, now),
                       "string");
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"TYPE", "hash"}, now),
                       "hash");
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"TYPE", "list"}, now),
                       "list");
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"TYPE", "set"}, now),
                       "set");
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"TYPE", "zset"}, now),
                       "zset");
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"TYPE", "missing"}, now),
                       "none");
}

TEST(CommandExecutorTest, TtlAndPttlReturnRedisStatusIntegers) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    auto* db = client.currentDb(manager);
    ASSERT_NE(db, nullptr);

    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"TTL", "missing"}, now), -2);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "missing"}, now), -2);

    db->set("key", RedisObject::createSharedStringObject("value"));
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"TTL", "key"}, now), -1);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "key"}, now), -1);

    ASSERT_TRUE(db->expireAfter("key", Milliseconds{2'500}, now));
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"TTL", "key"}, now), 3);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "key"}, now), 2'500);
}

TEST(CommandExecutorTest, PersistReturnsOneOnlyWhenExpirationWasCleared) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    auto* db = client.currentDb(manager);
    ASSERT_NE(db, nullptr);

    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PERSIST", "missing"}, now), 0);

    db->set("plain", RedisObject::createSharedStringObject("value"));
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PERSIST", "plain"}, now), 0);

    db->set("volatile", RedisObject::createSharedStringObject("value"));
    ASSERT_TRUE(db->expireAfter("volatile", Milliseconds{100}, now));
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PERSIST", "volatile"}, now), 1);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "volatile"}, now), -1);
}

TEST(CommandExecutorTest, ExpireAndPExpireReturnOneWhenTimeoutWasApplied) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    auto* db = client.currentDb(manager);
    ASSERT_NE(db, nullptr);

    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"EXPIRE", "missing", "10"}, now),
                  0);

    db->set("seconds", RedisObject::createSharedStringObject("value"));
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"EXPIRE", "seconds", "2"}, now),
                  1);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "seconds"}, now), 2'000);

    db->set("milliseconds", RedisObject::createSharedStringObject("value"));
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 3>{"PEXPIRE", "milliseconds", "25"}, now),
                  1);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "milliseconds"}, now),
                  25);
}

TEST(CommandExecutorTest, ExpireAcceptsNonPositiveTimeoutsAsImmediateExpiration) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    auto* db = client.currentDb(manager);
    ASSERT_NE(db, nullptr);

    const auto now = makeTime(1'000);

    db->set("zero", RedisObject::createSharedStringObject("value"));
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"EXPIRE", "zero", "0"}, now), 1);
    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "zero"}, now));

    db->set("negative", RedisObject::createSharedStringObject("value"));
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 3>{"PEXPIRE", "negative", "-1"}, now),
                  1);
    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "negative"}, now));
}

TEST(CommandExecutorTest, ExpireRejectsInvalidTimeouts) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;

    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"EXPIRE", "key", "not-int"}),
                "ERR value is not an integer or out of range");
    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"PEXPIRE", "key", "12.5"}),
                "ERR value is not an integer or out of range");
}
