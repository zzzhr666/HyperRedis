#include <gtest/gtest.h>

#include "hyper/server/client_context.hpp"
#include "hyper/server/command_executor.hpp"
#include "hyper/server/resp_value.hpp"
#include "hyper/storage/database.hpp"
#include "hyper/storage/redis_manager.hpp"

#include <array>
#include <initializer_list>
#include <span>
#include <set>
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

    void expectBulkDouble(const RespValue& value, double expected) {
        const auto* bulk_string = std::get_if<RespBulkString>(&value);
        ASSERT_NE(bulk_string, nullptr);
        ASSERT_TRUE(bulk_string->value.has_value());

        std::size_t consumed{};
        double actual{};
        ASSERT_NO_THROW(actual = std::stod(*bulk_string->value, &consumed));
        EXPECT_EQ(consumed, bulk_string->value->size());
        EXPECT_DOUBLE_EQ(actual, expected);
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

    void expectBulkArray(const RespValue& value, std::initializer_list<std::string_view> expected) {
        const auto* arr = std::get_if<std::shared_ptr<RespArray>>(&value);
        ASSERT_NE(arr, nullptr);
        ASSERT_NE(*arr, nullptr);
        ASSERT_EQ((*arr)->values.size(), expected.size());

        std::size_t index{0};
        for (const auto item : expected) {
            expectBulkString((*arr)->values[index], item);
            ++index;
        }
    }

    void expectZSetRangeArray(const RespValue& value, std::initializer_list<std::string_view> expected) {
        expectBulkArray(value, expected);
    }

    void expectZSetRangeWithScores(const RespValue& value,
                                   std::initializer_list<std::pair<std::string_view, double>> expected) {
        const auto* arr = std::get_if<std::shared_ptr<RespArray>>(&value);
        ASSERT_NE(arr, nullptr);
        ASSERT_NE(*arr, nullptr);
        ASSERT_EQ((*arr)->values.size(), expected.size() * 2);

        std::size_t index{0};
        for (const auto& [member, score] : expected) {
            expectBulkString((*arr)->values[index], member);
            expectBulkDouble((*arr)->values[index + 1], score);
            index += 2;
        }
    }

    void expectBulkArrayUnordered(const RespValue& value, const std::multiset<std::string>& expected) {
        const auto* arr = std::get_if<std::shared_ptr<RespArray>>(&value);
        ASSERT_NE(arr, nullptr);
        ASSERT_NE(*arr, nullptr);
        ASSERT_EQ((*arr)->values.size(), expected.size());

        std::multiset<std::string> actual;
        for (const auto& item : (*arr)->values) {
            const auto* bulk_string = std::get_if<RespBulkString>(&item);
            ASSERT_NE(bulk_string, nullptr);
            ASSERT_TRUE(bulk_string->value.has_value());
            actual.insert(*bulk_string->value);
        }

        EXPECT_EQ(actual, expected);
    }

    [[nodiscard]] std::multiset<std::string> bulkArrayToMultiset(const RespValue& value) {
        const auto* arr = std::get_if<std::shared_ptr<RespArray>>(&value);
        EXPECT_NE(arr, nullptr);
        EXPECT_NE(*arr, nullptr);

        std::multiset<std::string> actual;
        if (arr == nullptr || *arr == nullptr) {
            return actual;
        }

        for (const auto& item : (*arr)->values) {
            const auto* bulk_string = std::get_if<RespBulkString>(&item);
            EXPECT_NE(bulk_string, nullptr);
            if (bulk_string == nullptr || !bulk_string->value.has_value()) {
                continue;
            }
            actual.insert(*bulk_string->value);
        }
        return actual;
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

TEST(CommandExecutorTest, ExpireSupportsConditionOptions) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    auto* db = client.currentDb(manager);
    ASSERT_NE(db, nullptr);

    const auto now = makeTime(1'000);

    db->set("key", RedisObject::createSharedStringObject("value"));
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"EXPIRE", "key", "10", "NX"}, now),
                  1);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "key"}, now),
                  10'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"EXPIRE", "key", "20", "NX"}, now),
                  0);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "key"}, now),
                  10'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"EXPIRE", "key", "20", "XX"}, now),
                  1);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "key"}, now),
                  20'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"PEXPIRE", "key", "20", "XX"}, now),
                  1);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "key"}, now),
                  20);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"PEXPIRE", "key", "25", "GT"}, now),
                  1);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "key"}, now),
                  25);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"PEXPIRE", "key", "20", "gt"}, now),
                  0);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "key"}, now),
                  25);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"PEXPIRE", "key", "20", "lt"}, now),
                  1);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "key"}, now),
                  20);
}

TEST(CommandExecutorTest, ExpireRejectsInvalidConditionOptionWithoutChangingTtl) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    auto* db = client.currentDb(manager);
    ASSERT_NE(db, nullptr);

    const auto now = makeTime(1'000);

    db->set("key", RedisObject::createSharedStringObject("value"));
    ASSERT_TRUE(db->expireAfter("key", Milliseconds{1'000}, now));

    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 4>{"EXPIRE", "key", "10", "BAD"}, now),
                "ERR syntax error");
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "key"}, now),
                  1'000);
}

TEST(CommandExecutorTest, IncrByFloatReportsPreciseFloatErrors) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    auto* db = client.currentDb(manager);
    ASSERT_NE(db, nullptr);

    const auto now = makeTime(1'000);

    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 3>{"INCRBYFLOAT", "missing", "not-a-float"}, now),
                "ERR value is not a valid float");

    db->set("not-float", RedisObject::createSharedStringObject("not-a-float"));
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 3>{"INCRBYFLOAT", "not-float", "1.5"}, now),
                "ERR value is not a valid float");

    db->set("huge", RedisObject::createSharedStringObject("1e308"));
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 3>{"INCRBYFLOAT", "huge", "1e308"}, now),
                "ERR increment would produce NaN or Infinity");
}

TEST(CommandExecutorTest, DbSizeReportsCurrentDbKeyCount) {
    CommandExecutor executor;
    RedisManager manager(2);
    RedisClientContext client;

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "one", "1"}), "OK");
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "two", "2"}), "OK");
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 1>{"DBSIZE"}), 2);

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"SELECT", "1"}), "OK");
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 1>{"DBSIZE"}), 0);

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "other", "value"}), "OK");
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 1>{"DBSIZE"}), 1);

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"SELECT", "0"}), "OK");
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 1>{"DBSIZE"}), 2);
}

TEST(CommandExecutorTest, FlushDbClearsOnlyCurrentDb) {
    CommandExecutor executor;
    RedisManager manager(2);
    RedisClientContext client;

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "key", "db0"}), "OK");
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"SELECT", "1"}), "OK");
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "key", "db1"}), "OK");

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 1>{"FLUSHDB"}), "OK");
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 1>{"DBSIZE"}), 0);
    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "key"}));

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"SELECT", "0"}), "OK");
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 1>{"DBSIZE"}), 1);
    expectBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "key"}), "db0");
}

TEST(CommandExecutorTest, FlushAllClearsEveryDb) {
    CommandExecutor executor;
    RedisManager manager(2);
    RedisClientContext client;

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "key", "db0"}), "OK");
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"SELECT", "1"}), "OK");
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "key", "db1"}), "OK");

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 1>{"FLUSHALL"}), "OK");
    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "key"}));

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"SELECT", "0"}), "OK");
    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "key"}));
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 1>{"DBSIZE"}), 0);
}

TEST(CommandExecutorTest, RandomKeyReturnsLiveKeyOrNullBulk) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    auto* db = client.currentDb(manager);
    ASSERT_NE(db, nullptr);

    const auto now = makeTime(1'000);
    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 1>{"RANDOMKEY"}, now));

    db->set("expired", RedisObject::createSharedStringObject("expired"));
    db->set("live", RedisObject::createSharedStringObject("live"));
    ASSERT_TRUE(db->expireAfter("expired", Milliseconds{10}, now));

    expectBulkString(execute(executor, manager, client, std::array<std::string_view, 1>{"RANDOMKEY"},
                             now + Milliseconds{10}),
                     "live");
}

TEST(CommandExecutorTest, RenameMovesKeyAndReportsMissingSource) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"RENAME", "missing", "new"}, now),
                "ERR no such key");

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "old", "value"}, now),
                       "OK");
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "new", "overwritten"},
                               now),
                       "OK");
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"PEXPIRE", "old", "25"}, now),
                  1);

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"RENAME", "old", "new"}, now),
                       "OK");
    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "old"}, now));
    expectBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "new"}, now), "value");
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"PTTL", "new"}, now), 25);
}

TEST(CommandExecutorTest, ListPushPopAndLen) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    // LPUSH multiple values
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 4>{"LPUSH", "mylist", "v1", "v2"}, now), 2);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"LLEN", "mylist"}, now), 2);

    // RPUSH one value
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"RPUSH", "mylist", "v3"}, now), 3);

    // LRANGE [0, -1] should be v2, v1, v3 (because v2 was pushed last from left)
    auto range_resp = execute(executor, manager, client, std::array<std::string_view, 4>{"LRANGE", "mylist", "0", "-1"}, now);
    const auto* arr = std::get_if<std::shared_ptr<RespArray>>(&range_resp);
    ASSERT_NE(arr, nullptr);
    ASSERT_EQ((*arr)->values.size(), 3);
    expectBulkString((*arr)->values[0], "v2");
    expectBulkString((*arr)->values[1], "v1");
    expectBulkString((*arr)->values[2], "v3");

    // Pop and check key deletion
    expectBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"LPOP", "mylist"}, now), "v2");
    expectBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"RPOP", "mylist"}, now), "v3");
    expectBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"LPOP", "mylist"}, now), "v1");

    // Key should be gone
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"LLEN", "mylist"}, now), 0);
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"TYPE", "mylist"}, now), "none");
}

TEST(CommandExecutorTest, ListWrongType) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "str", "value"}, now), "OK");
    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"LPUSH", "str", "v1"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
}

TEST(CommandExecutorTest, LIndexReturnsElementByPositiveAndNegativeIndex) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 5>{"RPUSH", "list", "a", "b", "c"}, now),
                  3);

    expectBulkString(execute(executor, manager, client,
                             std::array<std::string_view, 3>{"LINDEX", "list", "0"}, now),
                     "a");
    expectBulkString(execute(executor, manager, client,
                             std::array<std::string_view, 3>{"LINDEX", "list", "-1"}, now),
                     "c");
    expectNullBulkString(execute(executor, manager, client,
                                 std::array<std::string_view, 3>{"LINDEX", "list", "3"}, now));
    expectNullBulkString(execute(executor, manager, client,
                                 std::array<std::string_view, 3>{"LINDEX", "missing", "0"}, now));
}

TEST(CommandExecutorTest, LIndexRejectsInvalidIndexAndWrongType) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 3>{"LINDEX", "list", "not-int"}, now),
                "ERR value is not an integer or out of range");

    expectSimpleString(execute(executor, manager, client,
                               std::array<std::string_view, 3>{"SET", "string", "value"}, now),
                       "OK");
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 3>{"LINDEX", "string", "0"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
}

TEST(CommandExecutorTest, LSetUpdatesExistingElementAndReportsMissingOrOutOfRange) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 5>{"RPUSH", "list", "a", "b", "c"}, now),
                  3);

    expectSimpleString(execute(executor, manager, client,
                               std::array<std::string_view, 4>{"LSET", "list", "1", "middle"}, now),
                       "OK");
    expectSimpleString(execute(executor, manager, client,
                               std::array<std::string_view, 4>{"LSET", "list", "-1", "tail"}, now),
                       "OK");
    expectBulkArray(execute(executor, manager, client,
                            std::array<std::string_view, 4>{"LRANGE", "list", "0", "-1"}, now),
                    {"a", "middle", "tail"});

    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 4>{"LSET", "list", "3", "out"}, now),
                "ERR index out of range");
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 4>{"LSET", "missing", "0", "value"}, now),
                "ERR no such key");
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 4>{"LSET", "list", "not-int", "value"}, now),
                "ERR value is not an integer or out of range");
}

TEST(CommandExecutorTest, LInsertInsertsAroundPivotAndHandlesMissingPivotOrKey) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"RPUSH", "list", "a", "c"}, now),
                  2);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 5>{"LINSERT", "list", "BEFORE", "c", "b"}, now),
                  3);
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 5>{"LINSERT", "list", "after", "c", "d"}, now),
                  4);
    expectBulkArray(execute(executor, manager, client,
                            std::array<std::string_view, 4>{"LRANGE", "list", "0", "-1"}, now),
                    {"a", "b", "c", "d"});

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 5>{"LINSERT", "list", "BEFORE", "missing", "x"}, now),
                  -1);
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 5>{"LINSERT", "missing", "BEFORE", "pivot", "value"}, now),
                  0);
}

TEST(CommandExecutorTest, LInsertRejectsInvalidPositionAndWrongType) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 3>{"RPUSH", "list", "a"}, now),
                  1);
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 5>{"LINSERT", "list", "MIDDLE", "a", "b"}, now),
                "ERR syntax error");

    expectSimpleString(execute(executor, manager, client,
                               std::array<std::string_view, 3>{"SET", "string", "value"}, now),
                       "OK");
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 5>{"LINSERT", "string", "BEFORE", "value", "x"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
}

TEST(CommandExecutorTest, LRemRemovesByCountDirectionAndDeletesEmptyKey) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 7>{"RPUSH", "list", "a", "b", "a", "c", "a"}, now),
                  5);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"LREM", "list", "2", "a"}, now),
                  2);
    expectBulkArray(execute(executor, manager, client,
                            std::array<std::string_view, 4>{"LRANGE", "list", "0", "-1"}, now),
                    {"b", "c", "a"});

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"LREM", "list", "-1", "a"}, now),
                  1);
    expectBulkArray(execute(executor, manager, client,
                            std::array<std::string_view, 4>{"LRANGE", "list", "0", "-1"}, now),
                    {"b", "c"});

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"LREM", "list", "0", "b"}, now),
                  1);
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"LREM", "list", "0", "c"}, now),
                  1);
    expectSimpleString(execute(executor, manager, client,
                               std::array<std::string_view, 2>{"TYPE", "list"}, now),
                       "none");
}

TEST(CommandExecutorTest, LRemHandlesMissingKeyInvalidCountAndWrongType) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"LREM", "missing", "0", "x"}, now),
                  0);
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 4>{"LREM", "missing", "bad", "x"}, now),
                "ERR value is not an integer or out of range");

    expectSimpleString(execute(executor, manager, client,
                               std::array<std::string_view, 3>{"SET", "string", "value"}, now),
                       "OK");
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 4>{"LREM", "string", "0", "value"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
}

TEST(CommandExecutorTest, LTrimKeepsInclusiveRangeAndDeletesEmptyResult) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 7>{"RPUSH", "list", "0", "1", "2", "3", "4"}, now),
                  5);

    expectSimpleString(execute(executor, manager, client,
                               std::array<std::string_view, 4>{"LTRIM", "list", "1", "3"}, now),
                       "OK");
    expectBulkArray(execute(executor, manager, client,
                            std::array<std::string_view, 4>{"LRANGE", "list", "0", "-1"}, now),
                    {"1", "2", "3"});

    expectSimpleString(execute(executor, manager, client,
                               std::array<std::string_view, 4>{"LTRIM", "list", "0", "-1"}, now),
                       "OK");
    expectBulkArray(execute(executor, manager, client,
                            std::array<std::string_view, 4>{"LRANGE", "list", "0", "-1"}, now),
                    {"1", "2", "3"});

    expectSimpleString(execute(executor, manager, client,
                               std::array<std::string_view, 4>{"LTRIM", "list", "10", "20"}, now),
                       "OK");
    expectSimpleString(execute(executor, manager, client,
                               std::array<std::string_view, 2>{"TYPE", "list"}, now),
                       "none");
}

TEST(CommandExecutorTest, LTrimHandlesMissingKeyInvalidIndexAndWrongType) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectSimpleString(execute(executor, manager, client,
                               std::array<std::string_view, 4>{"LTRIM", "missing", "0", "1"}, now),
                       "OK");
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 4>{"LTRIM", "missing", "not-int", "1"}, now),
                "ERR value is not an integer or out of range");

    expectSimpleString(execute(executor, manager, client,
                               std::array<std::string_view, 3>{"SET", "string", "value"}, now),
                       "OK");
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 4>{"LTRIM", "string", "0", "1"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
}

TEST(CommandExecutorTest, MSetAndMGet) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 5>{"MSET", "k1", "v1", "k2", "v2"}), "OK");

    auto mget_resp = execute(executor, manager, client, std::array<std::string_view, 4>{"MGET", "k1", "missing", "k2"});
    const auto* arr = std::get_if<std::shared_ptr<RespArray>>(&mget_resp);
    ASSERT_NE(arr, nullptr);
    ASSERT_EQ((*arr)->values.size(), 3);
    expectBulkString((*arr)->values[0], "v1");
    expectNullBulkString((*arr)->values[1]);
    expectBulkString((*arr)->values[2], "v2");
}

TEST(CommandExecutorTest, StringAppendAndLen) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;

    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"APPEND", "ackey", "hello"}), 5);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"APPEND", "ackey", " world"}), 11);
    expectBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"GET", "ackey"}), "hello world");
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"STRLEN", "ackey"}), 11);
}

TEST(CommandExecutorTest, HashBasicOperations) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    // HSET new field
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 4>{"HSET", "myhash", "f1", "v1"}, now), 1);
    // HSET existing field
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 4>{"HSET", "myhash", "f1", "new_v1"}, now), 0);

    // HGET
    expectBulkString(execute(executor, manager, client, std::array<std::string_view, 3>{"HGET", "myhash", "f1"}, now), "new_v1");
    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 3>{"HGET", "myhash", "f2"}, now));

    // HLEN
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"HLEN", "myhash"}, now), 1);

    // HDEL
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"HDEL", "myhash", "f1"}, now), 1);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"HLEN", "myhash"}, now), 0);

    // Key should be deleted after HDEL last field
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"TYPE", "myhash"}, now), "none");
}

TEST(CommandExecutorTest, HGetAllReturnsAllFieldsAndValues) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    execute(executor, manager, client, std::array<std::string_view, 4>{"HSET", "h", "f1", "v1"}, now);
    execute(executor, manager, client, std::array<std::string_view, 4>{"HSET", "h", "f2", "v2"}, now);

    auto resp = execute(executor, manager, client, std::array<std::string_view, 2>{"HGETALL", "h"}, now);
    const auto* arr = std::get_if<std::shared_ptr<RespArray>>(&resp);
    ASSERT_NE(arr, nullptr);
    ASSERT_EQ((*arr)->values.size(), 4);

    // Note: order depends on hash map implementation, but we can check if pairs exist
    // For simplicity, we just check content if implementation is deterministic or small
    std::set<std::string> results;
    for(auto& v : (*arr)->values) {
        results.insert(*std::get<RespBulkString>(v).value);
    }
    std::set<std::string> expected = {"f1", "v1", "f2", "v2"};
    EXPECT_EQ(results, expected);
}

TEST(CommandExecutorTest, HExistsReturnsOneOnlyForExistingField) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"HSET", "hash", "field", "value"}, now),
                  1);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 3>{"HEXISTS", "hash", "field"}, now),
                  1);
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 3>{"HEXISTS", "hash", "missing"}, now),
                  0);
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 3>{"HEXISTS", "missing", "field"}, now),
                  0);
}

TEST(CommandExecutorTest, HKeysAndHValsReturnHashContentWithoutOrderDependency) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"HSET", "hash", "f1", "same"}, now),
                  1);
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"HSET", "hash", "f2", "same"}, now),
                  1);
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"HSET", "hash", "f3", "other"}, now),
                  1);

    expectBulkArrayUnordered(execute(executor, manager, client,
                                     std::array<std::string_view, 2>{"HKEYS", "hash"}, now),
                             {"f1", "f2", "f3"});
    expectBulkArrayUnordered(execute(executor, manager, client,
                                     std::array<std::string_view, 2>{"HVALS", "hash"}, now),
                             {"other", "same", "same"});

    expectBulkArrayUnordered(execute(executor, manager, client,
                                     std::array<std::string_view, 2>{"HKEYS", "missing"}, now),
                             {});
    expectBulkArrayUnordered(execute(executor, manager, client,
                                     std::array<std::string_view, 2>{"HVALS", "missing"}, now),
                             {});
}

TEST(CommandExecutorTest, HashWrongType) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "s", "v"}, now);
    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"HGET", "s", "f1"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"HEXISTS", "s", "f1"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
    expectError(execute(executor, manager, client, std::array<std::string_view, 2>{"HKEYS", "s"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
    expectError(execute(executor, manager, client, std::array<std::string_view, 2>{"HVALS", "s"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
}

TEST(CommandExecutorTest, SetBasicOperations) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    // SADD
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 4>{"SADD", "myset", "m1", "m2"}, now), 2);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"SADD", "myset", "m1"}, now), 0);

    // SCARD
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"SCARD", "myset"}, now), 2);

    // SISMEMBER
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"SISMEMBER", "myset", "m1"}, now), 1);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"SISMEMBER", "myset", "m3"}, now), 0);

    // SREM
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"SREM", "myset", "m1"}, now), 1);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"SCARD", "myset"}, now), 1);

    // SMEMBERS
    auto resp = execute(executor, manager, client, std::array<std::string_view, 2>{"SMEMBERS", "myset"}, now);
    const auto* arr = std::get_if<std::shared_ptr<RespArray>>(&resp);
    ASSERT_NE(arr, nullptr);
    ASSERT_EQ((*arr)->values.size(), 1);
    expectBulkString((*arr)->values[0], "m2");

    // Clear and check deletion
    execute(executor, manager, client, std::array<std::string_view, 3>{"SREM", "myset", "m2"}, now);
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"TYPE", "myset"}, now), "none");
}

TEST(CommandExecutorTest, SetWrongType) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    execute(executor, manager, client, std::array<std::string_view, 4>{"HSET", "h", "f", "v"}, now);
    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"SADD", "h", "m1"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
}

TEST(CommandExecutorTest, SPopWithoutCountRemovesOneRandomMember) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"SADD", "set", "a", "b"}, now),
                  2);

    auto popped = execute(executor, manager, client, std::array<std::string_view, 2>{"SPOP", "set"}, now);
    const auto* popped_bulk = std::get_if<RespBulkString>(&popped);
    ASSERT_NE(popped_bulk, nullptr);
    ASSERT_TRUE(popped_bulk->value.has_value());
    EXPECT_TRUE(*popped_bulk->value == "a" || *popped_bulk->value == "b");
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"SCARD", "set"}, now), 1);
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 3>{"SISMEMBER", "set", *popped_bulk->value}, now),
                  0);

    expectBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"SPOP", "set"}, now),
                     *popped_bulk->value == "a" ? "b" : "a");
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"TYPE", "set"}, now), "none");
    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"SPOP", "missing"}, now));
}

TEST(CommandExecutorTest, SPopWithCountRemovesUpToCountAndDeletesEmptyKey) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 5>{"SADD", "set", "a", "b", "c"}, now),
                  3);

    auto popped = execute(executor, manager, client, std::array<std::string_view, 3>{"SPOP", "set", "5"}, now);
    auto popped_members = bulkArrayToMultiset(popped);
    EXPECT_EQ(popped_members, (std::multiset<std::string>{"a", "b", "c"}));
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"TYPE", "set"}, now), "none");

    expectBulkArrayUnordered(execute(executor, manager, client, std::array<std::string_view, 3>{"SPOP", "missing", "2"},
                                     now),
                             {});
    expectBulkArrayUnordered(execute(executor, manager, client, std::array<std::string_view, 3>{"SPOP", "missing", "0"},
                                     now),
                             {});
}

TEST(CommandExecutorTest, SPopRejectsInvalidOrNegativeCountAndWrongType) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"SADD", "set", "a"}, now), 1);

    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"SPOP", "set", "bad"}, now),
                "ERR value is not an integer or out of range");
    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"SPOP", "set", "-1"}, now),
                "ERR value is out of range, must be positive");

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "string", "value"}, now),
                       "OK");
    expectError(execute(executor, manager, client, std::array<std::string_view, 2>{"SPOP", "string"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"SPOP", "string", "1"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
}

TEST(CommandExecutorTest, SRandMemberWithoutCountDoesNotRemoveMember) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"SADD", "set", "a", "b"}, now),
                  2);

    auto member = execute(executor, manager, client, std::array<std::string_view, 2>{"SRANDMEMBER", "set"}, now);
    const auto* member_bulk = std::get_if<RespBulkString>(&member);
    ASSERT_NE(member_bulk, nullptr);
    ASSERT_TRUE(member_bulk->value.has_value());
    EXPECT_TRUE(*member_bulk->value == "a" || *member_bulk->value == "b");
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"SCARD", "set"}, now), 2);

    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 2>{"SRANDMEMBER", "missing"},
                                 now));
}

TEST(CommandExecutorTest, SRandMemberCountModesDoNotRemoveMembers) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 5>{"SADD", "set", "a", "b", "c"}, now),
                  3);

    auto positive = execute(executor, manager, client, std::array<std::string_view, 3>{"SRANDMEMBER", "set", "2"}, now);
    auto positive_members = bulkArrayToMultiset(positive);
    ASSERT_EQ(positive_members.size(), 2);
    for (const auto& member : positive_members) {
        EXPECT_TRUE(member == "a" || member == "b" || member == "c");
    }

    auto negative = execute(executor, manager, client, std::array<std::string_view, 3>{"SRANDMEMBER", "set", "-5"}, now);
    auto negative_members = bulkArrayToMultiset(negative);
    ASSERT_EQ(negative_members.size(), 5);
    for (const auto& member : negative_members) {
        EXPECT_TRUE(member == "a" || member == "b" || member == "c");
    }

    expectBulkArrayUnordered(execute(executor, manager, client,
                                     std::array<std::string_view, 3>{"SRANDMEMBER", "set", "0"}, now),
                             {});
    expectBulkArrayUnordered(execute(executor, manager, client,
                                     std::array<std::string_view, 3>{"SRANDMEMBER", "missing", "2"}, now),
                             {});
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"SCARD", "set"}, now), 3);
}

TEST(CommandExecutorTest, SRandMemberRejectsInvalidCountAndWrongType) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"SADD", "set", "a"}, now), 1);
    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"SRANDMEMBER", "set", "bad"}, now),
                "ERR value is not an integer or out of range");

    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 3>{"SET", "string", "value"}, now),
                       "OK");
    expectError(execute(executor, manager, client, std::array<std::string_view, 2>{"SRANDMEMBER", "string"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"SRANDMEMBER", "string", "1"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
}

TEST(CommandExecutorTest, ZSetBasicOperations) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    // ZADD multiple
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 6>{"ZADD", "myzset", "10", "m1", "20", "m2"}, now), 2);
    // ZADD update existing
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 4>{"ZADD", "myzset", "15", "m1"}, now), 0);

    // ZSCORE (Note: to_string might have different precision, but for integers it's usually "15.000000")
    auto score_resp = execute(executor, manager, client, std::array<std::string_view, 3>{"ZSCORE", "myzset", "m1"}, now);
    const auto* score_str = std::get_if<RespBulkString>(&score_resp);
    ASSERT_NE(score_str, nullptr);
    ASSERT_TRUE(score_str->value.has_value());
    EXPECT_TRUE(score_str->value->find("15.0") != std::string::npos);

    // ZCARD
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"ZCARD", "myzset"}, now), 2);

    // ZREM
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"ZREM", "myzset", "m1"}, now), 1);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 2>{"ZCARD", "myzset"}, now), 1);
}

TEST(CommandExecutorTest, ZRangeWithAndWithoutScores) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    execute(executor, manager, client, std::array<std::string_view, 6>{"ZADD", "z", "1", "a", "2", "b"}, now);

    // ZRANGE without scores
    auto resp1 = execute(executor, manager, client, std::array<std::string_view, 4>{"ZRANGE", "z", "0", "-1"}, now);
    const auto* arr1 = std::get_if<std::shared_ptr<RespArray>>(&resp1);
    ASSERT_NE(arr1, nullptr);
    ASSERT_EQ((*arr1)->values.size(), 2);
    expectBulkString((*arr1)->values[0], "a");
    expectBulkString((*arr1)->values[1], "b");

    // ZRANGE with scores
    auto resp2 = execute(executor, manager, client, std::array<std::string_view, 5>{"ZRANGE", "z", "0", "-1", "WITHSCORES"}, now);
    const auto* arr2 = std::get_if<std::shared_ptr<RespArray>>(&resp2);
    ASSERT_NE(arr2, nullptr);
    ASSERT_EQ((*arr2)->values.size(), 4);
    expectBulkString((*arr2)->values[0], "a");
    // Value check for score
    EXPECT_TRUE(std::get<RespBulkString>((*arr2)->values[1]).value->find("1.0") != std::string::npos);
}

TEST(CommandExecutorTest, ZAddAtomicValidation) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    // One invalid score should fail the whole command
    expectError(execute(executor, manager, client, std::array<std::string_view, 6>{"ZADD", "z", "10", "m1", "bad", "m2"}, now),
                "ERR value is not a valid float");

    // Key should not exist because the whole command failed
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"TYPE", "z"}, now), "none");
}

TEST(CommandExecutorTest, ZRankAndZRevRankReturnRankOrNull) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 8>{"ZADD", "z", "1", "a", "2", "b", "2", "c"}, now),
                  3);

    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"ZRANK", "z", "a"}, now), 0);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"ZRANK", "z", "c"}, now), 2);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"ZREVRANK", "z", "a"}, now), 2);
    expectInteger(execute(executor, manager, client, std::array<std::string_view, 3>{"ZREVRANK", "z", "c"}, now), 0);

    expectNullBulkString(execute(executor, manager, client, std::array<std::string_view, 3>{"ZRANK", "z", "missing"},
                                 now));
    expectNullBulkString(execute(executor, manager, client,
                                 std::array<std::string_view, 3>{"ZREVRANK", "missing", "a"}, now));
}

TEST(CommandExecutorTest, ZCountCountsInclusiveScoreRange) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 8>{"ZADD", "z", "1", "a", "2", "b", "3", "c"}, now),
                  3);

    expectInteger(execute(executor, manager, client, std::array<std::string_view, 4>{"ZCOUNT", "z", "2", "3"}, now),
                  2);
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"ZCOUNT", "z", "4", "5"}, now),
                  0);
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"ZCOUNT", "missing", "1", "3"}, now),
                  0);
    expectError(execute(executor, manager, client, std::array<std::string_view, 4>{"ZCOUNT", "z", "bad", "3"}, now),
                "ERR value is not a valid float");
}

TEST(CommandExecutorTest, ZRevRangeSupportsWithScoresAndMissingKey) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 8>{"ZADD", "z", "1", "a", "2", "b", "3", "c"}, now),
                  3);

    expectZSetRangeArray(execute(executor, manager, client,
                                 std::array<std::string_view, 4>{"ZREVRANGE", "z", "0", "-1"}, now),
                         {"c", "b", "a"});
    expectZSetRangeArray(execute(executor, manager, client,
                                 std::array<std::string_view, 4>{"ZREVRANGE", "z", "1", "2"}, now),
                         {"b", "a"});
    expectZSetRangeWithScores(execute(executor, manager, client,
                                      std::array<std::string_view, 5>{"ZREVRANGE", "z", "0", "1", "WITHSCORES"},
                                      now),
                              {{"c", 3.0}, {"b", 2.0}});
    expectZSetRangeArray(execute(executor, manager, client,
                                 std::array<std::string_view, 4>{"ZREVRANGE", "missing", "0", "-1"}, now),
                         {});
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 5>{"ZREVRANGE", "z", "0", "1", "BADOPTION"}, now),
                "ERR syntax error");
}

TEST(CommandExecutorTest, ZIncrByCreatesAndUpdatesScores) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectBulkDouble(execute(executor, manager, client,
                             std::array<std::string_view, 4>{"ZINCRBY", "z", "2.5", "a"}, now),
                     2.5);
    expectBulkDouble(execute(executor, manager, client,
                             std::array<std::string_view, 4>{"ZINCRBY", "z", "1.25", "a"}, now),
                     3.75);
    expectBulkDouble(execute(executor, manager, client, std::array<std::string_view, 3>{"ZSCORE", "z", "a"}, now),
                     3.75);

    expectError(execute(executor, manager, client, std::array<std::string_view, 4>{"ZINCRBY", "z", "bad", "a"}, now),
                "ERR value is not a valid float");
}

TEST(CommandExecutorTest, ZIncrByRejectsNonFiniteResult) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"ZADD", "z", "1e308", "a"}, now),
                  1);
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 4>{"ZINCRBY", "z", "1e308", "a"}, now),
                "ERR increment would produce NaN or Infinity");
}

TEST(CommandExecutorTest, ZRemRangeByRankRemovesInclusiveRangeAndDeletesEmptyKey) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 10>{"ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"},
                          now),
                  4);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"ZREMRANGEBYRANK", "z", "1", "2"}, now),
                  2);
    expectZSetRangeArray(execute(executor, manager, client,
                                 std::array<std::string_view, 4>{"ZRANGE", "z", "0", "-1"}, now),
                         {"a", "d"});
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"ZREMRANGEBYRANK", "z", "0", "-1"}, now),
                  2);
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"TYPE", "z"}, now), "none");
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"ZREMRANGEBYRANK", "missing", "0", "-1"}, now),
                  0);
}

TEST(CommandExecutorTest, ZRemRangeByScoreRemovesInclusiveScoreRangeAndDeletesEmptyKey) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 10>{"ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"},
                          now),
                  4);

    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"ZREMRANGEBYSCORE", "z", "2", "3"}, now),
                  2);
    expectZSetRangeArray(execute(executor, manager, client,
                                 std::array<std::string_view, 4>{"ZRANGE", "z", "0", "-1"}, now),
                         {"a", "d"});
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"ZREMRANGEBYSCORE", "z", "1", "4"}, now),
                  2);
    expectSimpleString(execute(executor, manager, client, std::array<std::string_view, 2>{"TYPE", "z"}, now), "none");
    expectInteger(execute(executor, manager, client,
                          std::array<std::string_view, 4>{"ZREMRANGEBYSCORE", "missing", "1", "4"}, now),
                  0);
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 4>{"ZREMRANGEBYSCORE", "missing", "bad", "4"}, now),
                "ERR value is not a valid float");
}

TEST(CommandExecutorTest, ZSetExtendedCommandsRejectWrongType) {
    CommandExecutor executor;
    RedisManager manager;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    expectSimpleString(execute(executor, manager, client,
                               std::array<std::string_view, 3>{"SET", "string", "value"}, now),
                       "OK");

    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"ZRANK", "string", "a"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
    expectError(execute(executor, manager, client, std::array<std::string_view, 3>{"ZREVRANK", "string", "a"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
    expectError(execute(executor, manager, client, std::array<std::string_view, 4>{"ZCOUNT", "string", "1", "2"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 4>{"ZREVRANGE", "string", "0", "-1"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 4>{"ZINCRBY", "string", "1", "a"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 4>{"ZREMRANGEBYRANK", "string", "0", "-1"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
    expectError(execute(executor, manager, client,
                        std::array<std::string_view, 4>{"ZREMRANGEBYSCORE", "string", "1", "2"}, now),
                "WRONGTYPE Operation against a key holding the wrong kind of value");
}
