#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <string_view>

#include "hyper/server/resp_codec.hpp"
#include "hyper/storage/aof_appender.hpp"
#include "hyper/storage/aof_replayer.hpp"
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

    void writeFile(const std::filesystem::path& path, std::string_view bytes) {
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(output);
        output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        ASSERT_TRUE(output.good());
    }

    void expectString(RedisDb& db, std::string_view key, std::string_view expected, ExpireTimePoint now) {
        auto obj = db.get(key, now);
        ASSERT_NE(obj, nullptr);
        ASSERT_EQ(obj->getType(), ObjectType::String);
        EXPECT_EQ(obj->asString(), expected);
    }
}

TEST(AofReplayerTest, ReplayRestoresStringCommand) {
    const auto path = testPath("hyperredis-aof-replayer-string.aof");
    std::filesystem::remove(path);

    AofAppender appender(path);
    const std::array<std::string_view, 3> args{"SET", "key", "value"};
    ASSERT_TRUE(appender.appendCommand(0, args, makeTime(1'000)));

    RedisManager manager(1);
    AofReplayer replayer(path);

    ASSERT_TRUE(replayer.replay(manager, makeTime(1'000)));

    ASSERT_NE(manager.db(0), nullptr);
    expectString(*manager.db(0), "key", "value", makeTime(1'000));

    std::filesystem::remove(path);
}

TEST(AofReplayerTest, ReplayRestoresSelectedDatabases) {
    const auto path = testPath("hyperredis-aof-replayer-selected-db.aof");
    std::filesystem::remove(path);

    AofAppender appender(path);
    const std::array<std::string_view, 3> args{"SET", "key", "db1"};
    ASSERT_TRUE(appender.appendCommand(1, args, makeTime(1'000)));

    RedisManager manager(2);
    AofReplayer replayer(path);

    ASSERT_TRUE(replayer.replay(manager, makeTime(1'000)));

    ASSERT_NE(manager.db(0), nullptr);
    ASSERT_NE(manager.db(1), nullptr);
    EXPECT_EQ(manager.db(0)->get("key", makeTime(1'000)), nullptr);
    expectString(*manager.db(1), "key", "db1", makeTime(1'000));

    std::filesystem::remove(path);
}

TEST(AofReplayerTest, ReplayBadFileReturnsFalseAndKeepsManager) {
    const auto path = testPath("hyperredis-aof-replayer-bad-file.aof");
    std::filesystem::remove(path);
    writeFile(path, "not resp");

    RedisManager manager(1);
    ASSERT_NE(manager.db(0), nullptr);
    manager.db(0)->set("keep", RedisObject::createSharedStringObject("old"));

    AofReplayer replayer(path);

    EXPECT_FALSE(replayer.replay(manager, makeTime(1'000)));
    expectString(*manager.db(0), "keep", "old", makeTime(1'000));

    std::filesystem::remove(path);
}

TEST(AofReplayerTest, ReplayCommandErrorReturnsFalseAndKeepsManager) {
    const auto path = testPath("hyperredis-aof-replayer-command-error.aof");
    std::filesystem::remove(path);

    const std::array<std::string_view, 3> set_args{"SET", "key", "not-an-int"};
    const std::array<std::string_view, 2> incr_args{"INCR", "key"};
    writeFile(path, serializeRespCommand(set_args) + serializeRespCommand(incr_args));

    RedisManager manager(1);
    ASSERT_NE(manager.db(0), nullptr);
    manager.db(0)->set("keep", RedisObject::createSharedStringObject("old"));

    AofReplayer replayer(path);

    EXPECT_FALSE(replayer.replay(manager, makeTime(1'000)));
    expectString(*manager.db(0), "keep", "old", makeTime(1'000));
    EXPECT_EQ(manager.db(0)->get("key", makeTime(1'000)), nullptr);

    std::filesystem::remove(path);
}
