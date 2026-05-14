#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string_view>

#include "hyper/storage/rdb_saver.hpp"

using namespace hyper;

namespace {
    [[nodiscard]] ExpireTimePoint makeTime(UnixMilliseconds ms) {
        return ExpireTimePoint{Milliseconds{ms}};
    }

    [[nodiscard]] std::filesystem::path testPath(std::string_view name) {
        return std::filesystem::temp_directory_path() / std::string(name);
    }

    void expectString(RedisDb& db, std::string_view key, std::string_view expected, ExpireTimePoint now) {
        auto obj = db.get(key, now);
        ASSERT_NE(obj, nullptr);
        ASSERT_EQ(obj->getType(), ObjectType::String);
        EXPECT_EQ(obj->asString(), expected);
    }
}

TEST(RdbSaverTest, SaveAndLoadRoundTripsThroughFile) {
    const auto path = testPath("hyperredis-rdb-saver-round-trip.rdb");
    std::filesystem::remove(path);

    RedisManager source(2);
    const auto now = makeTime(1'000);
    ASSERT_NE(source.db(0), nullptr);
    ASSERT_NE(source.db(1), nullptr);
    source.db(0)->set("key", RedisObject::createSharedStringObject("value"));
    source.db(1)->set("other", RedisObject::createSharedStringObject("db1"));
    RdbSaver saver(path);
    ASSERT_TRUE(saver.save(source, now));
    ASSERT_TRUE(std::filesystem::is_regular_file(path));

    RedisManager target(2);
    ASSERT_TRUE(saver.load(target, now));

    expectString(*target.db(0), "key", "value", now);
    expectString(*target.db(1), "other", "db1", now);

    std::filesystem::remove(path);
}

TEST(RdbSaverTest, LoadMissingFileReturnsFalseAndKeepsManager) {
    const auto path = testPath("hyperredis-rdb-saver-missing.rdb");
    std::filesystem::remove(path);

    RedisManager target(1);
    const auto now = makeTime(1'000);
    ASSERT_NE(target.db(0), nullptr);
    target.db(0)->set("keep", RedisObject::createSharedStringObject("old"));

    RdbSaver saver(path);
    EXPECT_FALSE(saver.load(target, now));
    expectString(*target.db(0), "keep", "old", now);
}

TEST(RdbSaverTest, LoadBadFileReturnsFalseAndKeepsManager) {
    const auto path = testPath("hyperredis-rdb-saver-bad.rdb");
    std::filesystem::remove(path);

    {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(out);
        out << "not an rdb";
    }

    RedisManager target(1);
    const auto now = makeTime(1'000);
    ASSERT_NE(target.db(0), nullptr);
    target.db(0)->set("keep", RedisObject::createSharedStringObject("old"));

    RdbSaver saver(path);
    EXPECT_FALSE(saver.load(target, now));
    expectString(*target.db(0), "keep", "old", now);

    std::filesystem::remove(path);
}
