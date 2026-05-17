#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>

#include "hyper/server/client_context.hpp"
#include "hyper/server/command_processor.hpp"
#include "hyper/server/resp_codec.hpp"
#include "hyper/server/resp_value.hpp"
#include "hyper/storage/aof_appender.hpp"
#include "hyper/storage/database.hpp"
#include "hyper/storage/object.hpp"
#include "hyper/storage/redis_manager.hpp"

using namespace hyper;

namespace {
    [[nodiscard]] ExpireTimePoint makeTime(UnixMilliseconds ms) {
        return ExpireTimePoint{Milliseconds{ms}};
    }

    [[nodiscard]] std::filesystem::path testPath(std::string_view name) {
        return std::filesystem::temp_directory_path() / std::string(name);
    }

    [[nodiscard]] std::string readFileIfExists(const std::filesystem::path& path) {
        if (!std::filesystem::exists(path)) {
            return {};
        }
        std::ifstream input(path, std::ios::binary);
        return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
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

    void expectError(const RespValue& value, std::string_view expected) {
        const auto* error = std::get_if<RespError>(&value);
        ASSERT_NE(error, nullptr);
        EXPECT_EQ(error->message, expected);
    }
}

TEST(CommandProcessorTest, WriteCommandIsAppendedAfterSuccess) {
    const auto path = testPath("hyperredis-command-processor-write.aof");
    std::filesystem::remove(path);

    RedisManager manager(1);
    RedisClientContext client;
    AofAppender appender(path);
    CommandProcessor processor(&appender);
    const std::array<std::string_view, 3> args{"SET", "key", "value"};

    expectSimpleString(processor.execute(manager, client, args, makeTime(1'000)), "OK");

    EXPECT_EQ(readFileIfExists(path), serializeRespCommand(args));

    std::filesystem::remove(path);
}

TEST(CommandProcessorTest, ReadCommandIsNotAppended) {
    const auto path = testPath("hyperredis-command-processor-read.aof");
    std::filesystem::remove(path);

    RedisManager manager(1);
    RedisClientContext client;
    ASSERT_NE(manager.db(0), nullptr);
    manager.db(0)->set("key", RedisObject::createSharedStringObject("value"));
    AofAppender appender(path);
    CommandProcessor processor(&appender);
    const std::array<std::string_view, 2> args{"GET", "key"};

    expectBulkString(processor.execute(manager, client, args, makeTime(1'000)), "value");

    EXPECT_EQ(readFileIfExists(path), "");

    std::filesystem::remove(path);
}

TEST(CommandProcessorTest, FailedWriteCommandIsNotAppended) {
    const auto path = testPath("hyperredis-command-processor-failed-write.aof");
    std::filesystem::remove(path);

    RedisManager manager(1);
    RedisClientContext client;
    ASSERT_NE(manager.db(0), nullptr);
    manager.db(0)->set("key", RedisObject::createSharedStringObject("not-an-int"));
    AofAppender appender(path);
    CommandProcessor processor(&appender);
    const std::array<std::string_view, 2> args{"INCR", "key"};

    expectError(processor.execute(manager, client, args, makeTime(1'000)),
                "ERR value is not an integer or out of range");

    EXPECT_EQ(readFileIfExists(path), "");

    std::filesystem::remove(path);
}

TEST(CommandProcessorTest, WriteCommandUsesCurrentSelectedDb) {
    const auto path = testPath("hyperredis-command-processor-selected-db.aof");
    std::filesystem::remove(path);

    RedisManager manager(2);
    RedisClientContext client;
    AofAppender appender(path);
    CommandProcessor processor(&appender);
    const std::array<std::string_view, 2> select_args{"SELECT", "1"};
    const std::array<std::string_view, 3> set_args{"SET", "key", "value"};

    expectSimpleString(processor.execute(manager, client, select_args, makeTime(1'000)), "OK");
    expectSimpleString(processor.execute(manager, client, set_args, makeTime(1'000)), "OK");

    EXPECT_EQ(readFileIfExists(path), serializeRespCommand(select_args) + serializeRespCommand(set_args));

    std::filesystem::remove(path);
}

TEST(CommandProcessorTest, UnknownCommandReturnsErrorAndIsNotAppended) {
    const auto path = testPath("hyperredis-command-processor-unknown.aof");
    std::filesystem::remove(path);

    RedisManager manager(1);
    RedisClientContext client;
    AofAppender appender(path);
    CommandProcessor processor(&appender);
    const std::array<std::string_view, 1> args{"NO_SUCH_COMMAND"};

    expectError(processor.execute(manager, client, args, makeTime(1'000)), "ERR unknown command");

    EXPECT_EQ(readFileIfExists(path), "");

    std::filesystem::remove(path);
}

TEST(CommandProcessorTest, AppendFailureReturnsErrorAfterWriteCommandExecutes) {
    const auto path = testPath("hyperredis-command-processor-aof-as-directory.aof");
    std::filesystem::remove_all(path);
    ASSERT_TRUE(std::filesystem::create_directory(path));

    RedisManager manager(1);
    RedisClientContext client;
    AofAppender appender(path);
    CommandProcessor processor(&appender);
    const std::array<std::string_view, 3> args{"SET", "key", "value"};

    expectError(processor.execute(manager, client, args, makeTime(1'000)), "ERR append only file write failed");

    ASSERT_NE(manager.db(0), nullptr);
    auto obj = manager.db(0)->get("key", makeTime(1'000));
    ASSERT_NE(obj, nullptr);
    ASSERT_EQ(obj->getType(), ObjectType::String);
    EXPECT_EQ(obj->asString(), "value");

    std::filesystem::remove_all(path);
}

TEST(CommandProcessorTest, BrokenAppenderRejectsLaterWriteCommandBeforeExecution) {
    const auto path = testPath("hyperredis-command-processor-broken-aof.aof");
    std::filesystem::remove_all(path);
    ASSERT_TRUE(std::filesystem::create_directory(path));

    RedisManager manager(1);
    RedisClientContext client;
    AofAppender appender(path);
    CommandProcessor processor(&appender);
    const std::array<std::string_view, 3> first_set{"SET", "first", "value"};
    const std::array<std::string_view, 3> second_set{"SET", "second", "value"};

    expectError(processor.execute(manager, client, first_set, makeTime(1'000)),
                "ERR append only file write failed");

    std::filesystem::remove_all(path);

    expectError(processor.execute(manager, client, second_set, makeTime(1'000)),
                "ERR append only file write failed");

    ASSERT_NE(manager.db(0), nullptr);
    EXPECT_EQ(manager.db(0)->get("second", makeTime(1'000)), nullptr);
}

TEST(CommandProcessorTest, BrokenAppenderStillAllowsReadCommand) {
    const auto path = testPath("hyperredis-command-processor-broken-aof-read.aof");
    std::filesystem::remove_all(path);
    ASSERT_TRUE(std::filesystem::create_directory(path));

    RedisManager manager(1);
    RedisClientContext client;
    ASSERT_NE(manager.db(0), nullptr);
    manager.db(0)->set("key", RedisObject::createSharedStringObject("value"));
    AofAppender appender(path);
    CommandProcessor processor(&appender);
    const std::array<std::string_view, 3> set_args{"SET", "first", "value"};
    const std::array<std::string_view, 2> get_args{"GET", "key"};

    expectError(processor.execute(manager, client, set_args, makeTime(1'000)),
                "ERR append only file write failed");

    expectBulkString(processor.execute(manager, client, get_args, makeTime(1'000)), "value");

    std::filesystem::remove_all(path);
}
