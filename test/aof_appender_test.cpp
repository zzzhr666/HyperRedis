#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>

#include "hyper/server/resp_codec.hpp"
#include "hyper/storage/aof_appender.hpp"

using namespace hyper;

namespace {
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

    AofAppender appender(path);
    const std::array<std::string_view, 3> args{"SET", "key", "value"};

    ASSERT_TRUE(appender.appendCommand(0, args));

    EXPECT_EQ(readFile(path), serializeRespCommand(args));

    std::filesystem::remove(path);
}

TEST(AofAppenderTest, AppendCommandWritesSelectWhenDbChanges) {
    const auto path = testPath("hyperredis-aof-appender-select-db1.aof");
    std::filesystem::remove(path);

    AofAppender appender(path);
    const std::array<std::string_view, 3> set_args{"SET", "key", "value"};
    const std::array<std::string_view, 2> select_args{"SELECT", "1"};

    ASSERT_TRUE(appender.appendCommand(1, set_args));

    EXPECT_EQ(readFile(path), serializeRespCommand(select_args) + serializeRespCommand(set_args));

    std::filesystem::remove(path);
}

TEST(AofAppenderTest, AppendCommandDoesNotRepeatSelectForSameDb) {
    const auto path = testPath("hyperredis-aof-appender-no-repeat-select.aof");
    std::filesystem::remove(path);

    AofAppender appender(path);
    const std::array<std::string_view, 3> first_args{"SET", "one", "1"};
    const std::array<std::string_view, 3> second_args{"SET", "two", "2"};
    const std::array<std::string_view, 2> select_args{"SELECT", "1"};

    ASSERT_TRUE(appender.appendCommand(1, first_args));
    ASSERT_TRUE(appender.appendCommand(1, second_args));

    EXPECT_EQ(readFile(path),
              serializeRespCommand(select_args) + serializeRespCommand(first_args) + serializeRespCommand(second_args));

    std::filesystem::remove(path);
}

TEST(AofAppenderTest, AppendCommandWritesSelectWhenReturningToDbZero) {
    const auto path = testPath("hyperredis-aof-appender-return-db0.aof");
    std::filesystem::remove(path);

    AofAppender appender(path);
    const std::array<std::string_view, 3> db1_args{"SET", "db1", "value"};
    const std::array<std::string_view, 2> db0_args{"DEL", "db0"};
    const std::array<std::string_view, 2> select_db1{"SELECT", "1"};
    const std::array<std::string_view, 2> select_db0{"SELECT", "0"};

    ASSERT_TRUE(appender.appendCommand(1, db1_args));
    ASSERT_TRUE(appender.appendCommand(0, db0_args));

    EXPECT_EQ(readFile(path),
              serializeRespCommand(select_db1)
                  + serializeRespCommand(db1_args)
                  + serializeRespCommand(select_db0)
                  + serializeRespCommand(db0_args));

    std::filesystem::remove(path);
}
