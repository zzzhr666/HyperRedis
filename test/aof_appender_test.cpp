#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>

#include "hyper/server/resp_codec.hpp"
#include "hyper/storage/aof_appender.hpp"

using namespace hyper;

namespace {
    [[nodiscard]] ExpireTimePoint makeTime(std::int64_t ms) {
        return ExpireTimePoint{Milliseconds{ms}};
    }

    [[nodiscard]] std::filesystem::path testPath(std::string_view name) {
        return std::filesystem::temp_directory_path() / std::string(name);
    }

    [[nodiscard]] std::string readFile(const std::filesystem::path& path) {
        std::ifstream input(path, std::ios::binary);
        return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
    }
}

TEST(AofAppenderTest, AppendCommandWritesRespCommandForDbZero) {
    const auto path = testPath("hyperredis-aof-appender-db0.aof");
    std::filesystem::remove(path);

    {
        AofAppender appender(path);
        const std::array<std::string_view, 3> args{"SET", "key", "value"};
        const auto now = makeTime(1'000);

        ASSERT_TRUE(appender.appendCommand(0, args, now));

        EXPECT_EQ(readFile(path), serializeRespCommand(args));
    }

    std::filesystem::remove(path);
}

TEST(AofAppenderTest, AppendCommandWithAlwaysFsyncWritesRespCommand) {
    const auto path = testPath("hyperredis-aof-appender-always-fsync.aof");
    std::filesystem::remove(path);

    {
        AofAppender appender(path, AofFsyncPolicy::Always);
        const std::array<std::string_view, 3> args{"SET", "key", "value"};
        const auto now = makeTime(1'000);

        ASSERT_TRUE(appender.appendCommand(0, args, now));

        EXPECT_FALSE(appender.isBroken());
        EXPECT_EQ(readFile(path), serializeRespCommand(args));
    }

    std::filesystem::remove(path);
}

TEST(AofAppenderTest, AlwaysFsyncFailureLeavesAppenderBroken) {
    const std::filesystem::path path{"/dev/null"};
    if (!std::filesystem::exists(path)) {
        GTEST_SKIP() << "/dev/null is required for fsync failure coverage";
    }

    AofAppender appender(path, AofFsyncPolicy::Always);
    const std::array<std::string_view, 3> args{"SET", "key", "value"};

    EXPECT_FALSE(appender.appendCommand(0, args, makeTime(1'000)));
    EXPECT_TRUE(appender.isBroken());
    EXPECT_FALSE(appender.appendCommand(0, args, makeTime(1'001)));
}

TEST(AofAppenderTest, EverySecondFsyncFailureIsDeferredUntilIntervalElapses) {
    const std::filesystem::path path{"/dev/null"};
    if (!std::filesystem::exists(path)) {
        GTEST_SKIP() << "/dev/null is required for fsync failure coverage";
    }

    AofAppender appender(path, AofFsyncPolicy::EverySecond);
    const std::array<std::string_view, 3> args{"SET", "key", "value"};

    EXPECT_TRUE(appender.appendCommand(0, args, makeTime(1'000)));
    EXPECT_FALSE(appender.isBroken());

    EXPECT_TRUE(appender.appendCommand(0, args, makeTime(1'999)));
    EXPECT_FALSE(appender.isBroken());

    EXPECT_FALSE(appender.appendCommand(0, args, makeTime(2'000)));
    EXPECT_TRUE(appender.isBroken());
    EXPECT_FALSE(appender.appendCommand(0, args, makeTime(3'000)));
}

TEST(AofAppenderTest, AppendCommandWritesSelectWhenDbChanges) {
    const auto path = testPath("hyperredis-aof-appender-select-db1.aof");
    std::filesystem::remove(path);

    {
        AofAppender appender(path);
        const std::array<std::string_view, 3> set_args{"SET", "key", "value"};
        const std::array<std::string_view, 2> select_args{"SELECT", "1"};
        const auto now = makeTime(1'000);

        ASSERT_TRUE(appender.appendCommand(1, set_args, now));

        EXPECT_EQ(readFile(path), serializeRespCommand(select_args) + serializeRespCommand(set_args));
    }

    std::filesystem::remove(path);
}

TEST(AofAppenderTest, AppendCommandDoesNotRepeatSelectForSameDb) {
    const auto path = testPath("hyperredis-aof-appender-no-repeat-select.aof");
    std::filesystem::remove(path);

    {
        AofAppender appender(path);
        const std::array<std::string_view, 3> first_args{"SET", "one", "1"};
        const std::array<std::string_view, 3> second_args{"SET", "two", "2"};
        const std::array<std::string_view, 2> select_args{"SELECT", "1"};
        const auto now = makeTime(1'000);

        ASSERT_TRUE(appender.appendCommand(1, first_args, now));
        ASSERT_TRUE(appender.appendCommand(1, second_args, now));

        EXPECT_EQ(readFile(path),
                  serializeRespCommand(select_args)
                      + serializeRespCommand(first_args)
                      + serializeRespCommand(second_args));
    }

    std::filesystem::remove(path);
}

TEST(AofAppenderTest, AppendCommandWritesSelectWhenReturningToDbZero) {
    const auto path = testPath("hyperredis-aof-appender-return-db0.aof");
    std::filesystem::remove(path);

    {
        AofAppender appender(path);
        const std::array<std::string_view, 3> db1_args{"SET", "db1", "value"};
        const std::array<std::string_view, 2> db0_args{"DEL", "db0"};
        const std::array<std::string_view, 2> select_db1{"SELECT", "1"};
        const std::array<std::string_view, 2> select_db0{"SELECT", "0"};
        const auto now = makeTime(1'000);

        ASSERT_TRUE(appender.appendCommand(1, db1_args, now));
        ASSERT_TRUE(appender.appendCommand(0, db0_args, now));

        EXPECT_EQ(readFile(path),
                  serializeRespCommand(select_db1)
                      + serializeRespCommand(db1_args)
                      + serializeRespCommand(select_db0)
                      + serializeRespCommand(db0_args));
    }

    std::filesystem::remove(path);
}

TEST(AofAppenderTest, AppendCommandFailureLeavesAppenderBroken) {
    const auto path = testPath("hyperredis-aof-appender-broken.aof");
    std::filesystem::remove_all(path);
    ASSERT_TRUE(std::filesystem::create_directory(path));

    AofAppender appender(path);
    const std::array<std::string_view, 3> args{"SET", "key", "value"};
    const auto now = makeTime(1'000);

    ASSERT_FALSE(appender.appendCommand(0, args, now));
    EXPECT_TRUE(appender.isBroken());

    std::filesystem::remove_all(path);

    EXPECT_FALSE(appender.appendCommand(0, args, now));
    EXPECT_FALSE(std::filesystem::exists(path));
}
