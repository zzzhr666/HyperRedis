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
    RespValue execute(CommandExecutor& executor,
                      RedisManager& manager,
                      RedisClientContext& client,
                      std::initializer_list<std::string_view> args) {
        std::vector<std::string_view> v_args(args);
        return executor.execute(manager, client, std::span<const std::string_view>{v_args.data(), v_args.size()}, ExpireTimePoint{});
    }

    template<std::size_t N>
    RespValue execute(CommandExecutor& executor,
                      RedisManager& manager,
                      RedisClientContext& client,
                      const std::array<std::string_view, N>& args) {
        return executor.execute(manager, client, std::span<const std::string_view>{args.data(), args.size()}, ExpireTimePoint{});
    }

    void expectBulkString(const RespValue& value, std::string_view expected) {
        const auto* bulk_string = std::get_if<RespBulkString>(&value);
        ASSERT_NE(bulk_string, nullptr);
        ASSERT_TRUE(bulk_string->value.has_value()) << "Expected bulk string \"" << expected << "\", but got Null Bulk String";
        EXPECT_EQ(*bulk_string->value, expected);
    }

    void expectInteger(const RespValue& value, std::int64_t expected) {
        const auto* integer = std::get_if<RespInteger>(&value);
        ASSERT_NE(integer, nullptr);
        EXPECT_EQ(integer->value, expected);
    }

    void expectNullBulkString(const RespValue& value) {
        const auto* bulk_string = std::get_if<RespBulkString>(&value);
        ASSERT_NE(bulk_string, nullptr);
        EXPECT_FALSE(bulk_string->value.has_value());
    }

    void expectError(const RespValue& value, std::string_view expected_prefix) {
        const auto* error = std::get_if<RespError>(&value);
        ASSERT_NE(error, nullptr);
        EXPECT_TRUE(error->message.find(expected_prefix) == 0) << "Expected error starting with \"" << expected_prefix << "\", but got \"" << error->message << "\"";
    }
}

class ObjectCommandTest : public ::testing::Test {
protected:
    RedisManager manager;
    RedisClientContext client;
    CommandExecutor executor;
};

TEST_F(ObjectCommandTest, ObjectEncoding) {
    // String (raw)
    execute(executor, manager, client, {"SET", "k1", "hello"});
    expectBulkString(execute(executor, manager, client, {"OBJECT", "ENCODING", "k1"}), "raw");

    // String (int)
    execute(executor, manager, client, {"SET", "k2", "123"});
    expectBulkString(execute(executor, manager, client, {"OBJECT", "ENCODING", "k2"}), "int");

    // List (ziplist by default for new lists)
    execute(executor, manager, client, {"LPUSH", "mylist", "a"});
    expectBulkString(execute(executor, manager, client, {"OBJECT", "ENCODING", "mylist"}), "ziplist");

    // Missing key
    expectNullBulkString(execute(executor, manager, client, {"OBJECT", "ENCODING", "nonexistent"}));
}

TEST_F(ObjectCommandTest, ObjectRefCount) {
    execute(executor, manager, client, {"SET", "k1", "hello"});
    // refcount should be at least 1
    RespValue res = execute(executor, manager, client, {"OBJECT", "REFCOUNT", "k1"});
    const auto* integer = std::get_if<RespInteger>(&res);
    ASSERT_NE(integer, nullptr);
    EXPECT_GE(integer->value, 1);
}

TEST_F(ObjectCommandTest, InvalidSubcommand) {
    execute(executor, manager, client, {"SET", "k1", "hello"});
    // In Redis, unknown subcommands usually return a syntax error or a specific error.
    expectError(execute(executor, manager, client, {"OBJECT", "INVALID", "k1"}), "ERR syntax error");
}
