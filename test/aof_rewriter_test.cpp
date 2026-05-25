#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

#include "hyper/storage/aof_replayer.hpp"
#include "hyper/storage/aof_rewriter.hpp"
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

    [[nodiscard]] std::string readFile(const std::filesystem::path& path) {
        std::ifstream input(path, std::ios::binary);
        return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
    }

    void expectString(RedisDb& db, std::string_view key, std::string_view expected, ExpireTimePoint now) {
        auto obj = db.get(key, now);
        ASSERT_NE(obj, nullptr);
        ASSERT_EQ(obj->getType(), ObjectType::String);
        EXPECT_EQ(obj->asString(), expected);
    }

    void expectList(RedisDb& db, std::string_view key, const std::vector<std::string>& expected,
                    ExpireTimePoint now) {
        auto obj = db.get(key, now);
        ASSERT_NE(obj, nullptr);
        ASSERT_EQ(obj->getType(), ObjectType::List);
        EXPECT_EQ(obj->listRangeAsStrings(0, -1), expected);
    }

    void expectHashField(RedisDb& db, std::string_view key, std::string_view field,
                         std::string_view expected, ExpireTimePoint now) {
        auto obj = db.get(key, now);
        ASSERT_NE(obj, nullptr);
        ASSERT_EQ(obj->getType(), ObjectType::Hash);
        auto value = obj->hashGet(field);
        ASSERT_NE(value, nullptr);
        EXPECT_EQ(value->asString(), expected);
    }

    void expectSet(RedisDb& db, std::string_view key, const std::vector<std::string>& expected,
                   ExpireTimePoint now) {
        auto obj = db.get(key, now);
        ASSERT_NE(obj, nullptr);
        ASSERT_EQ(obj->getType(), ObjectType::Set);
        EXPECT_EQ(obj->setSize(), expected.size());
        for (const auto& member : expected) {
            EXPECT_TRUE(obj->setContains(member)) << member;
        }
    }
}

TEST(AofRewriterTest, RewriteEmptyManagerCreatesEmptyAof) {
    const auto path = testPath("hyperredis-aof-rewriter-empty.aof");
    std::filesystem::remove(path);

    RedisManager source(2);
    AofRewriter rewriter(path);

    ASSERT_TRUE(rewriter.rewrite(source, makeTime(1'000)));
    ASSERT_TRUE(std::filesystem::is_regular_file(path));
    EXPECT_TRUE(readFile(path).empty());

    std::filesystem::remove(path);
}

TEST(AofRewriterTest, RewriteStringKeysCanBeReplayed) {
    const auto path = testPath("hyperredis-aof-rewriter-strings.aof");
    std::filesystem::remove(path);

    const auto now = makeTime(1'000);
    RedisManager source(1);
    ASSERT_NE(source.db(0), nullptr);
    source.db(0)->set("plain", RedisObject::createSharedStringObject("value"));
    source.db(0)->set("integer", RedisObject::createSharedLongObject(42));

    AofRewriter rewriter(path);
    ASSERT_TRUE(rewriter.rewrite(source, now));

    RedisManager target(1);
    ASSERT_TRUE(AofReplayer::replay(path, target, now).ok);

    ASSERT_NE(target.db(0), nullptr);
    expectString(*target.db(0), "plain", "value", now);
    expectString(*target.db(0), "integer", "42", now);

    std::filesystem::remove(path);
}

TEST(AofRewriterTest, RewritePreservesSelectedDatabases) {
    const auto path = testPath("hyperredis-aof-rewriter-selected-db.aof");
    std::filesystem::remove(path);

    const auto now = makeTime(1'000);
    RedisManager source(3);
    ASSERT_NE(source.db(0), nullptr);
    ASSERT_NE(source.db(2), nullptr);
    source.db(0)->set("key", RedisObject::createSharedStringObject("db0"));
    source.db(2)->set("key", RedisObject::createSharedStringObject("db2"));

    AofRewriter rewriter(path);
    ASSERT_TRUE(rewriter.rewrite(source, now));

    RedisManager target(3);
    ASSERT_TRUE(AofReplayer::replay(path, target, now).ok);

    ASSERT_NE(target.db(0), nullptr);
    ASSERT_NE(target.db(1), nullptr);
    ASSERT_NE(target.db(2), nullptr);
    expectString(*target.db(0), "key", "db0", now);
    EXPECT_EQ(target.db(1)->get("key", now), nullptr);
    expectString(*target.db(2), "key", "db2", now);

    std::filesystem::remove(path);
}

TEST(AofRewriterTest, RewriteSkipsExpiredKeys) {
    const auto path = testPath("hyperredis-aof-rewriter-expired.aof");
    std::filesystem::remove(path);

    const auto now = makeTime(1'000);
    RedisManager source(1);
    ASSERT_NE(source.db(0), nullptr);
    source.db(0)->set("expired", RedisObject::createSharedStringObject("old"));
    source.db(0)->set("live", RedisObject::createSharedStringObject("new"));
    ASSERT_TRUE(source.db(0)->expireAfter("expired", Milliseconds{5}, now));

    AofRewriter rewriter(path);
    ASSERT_TRUE(rewriter.rewrite(source, now + Milliseconds{5}));

    RedisManager target(1);
    ASSERT_TRUE(AofReplayer::replay(path, target, now + Milliseconds{5}).ok);

    ASSERT_NE(target.db(0), nullptr);
    EXPECT_EQ(target.db(0)->get("expired", now + Milliseconds{5}), nullptr);
    expectString(*target.db(0), "live", "new", now + Milliseconds{5});

    std::filesystem::remove(path);
}

TEST(AofRewriterTest, RewriteListKeysCanBeReplayedInOrder) {
    const auto path = testPath("hyperredis-aof-rewriter-list.aof");
    std::filesystem::remove(path);

    const auto now = makeTime(1'000);
    RedisManager source(1);
    ASSERT_NE(source.db(0), nullptr);
    auto list = RedisObject::createSharedListObject();
    list->listRightPush(RedisObject::createSharedStringObject("first"));
    list->listRightPush(RedisObject::createSharedStringObject("second"));
    list->listRightPush(RedisObject::createSharedStringObject("third"));
    source.db(0)->set("items", list);

    AofRewriter rewriter(path);
    ASSERT_TRUE(rewriter.rewrite(source, now));

    RedisManager target(1);
    ASSERT_TRUE(AofReplayer::replay(path, target, now).ok);

    ASSERT_NE(target.db(0), nullptr);
    expectList(*target.db(0), "items", {"first", "second", "third"}, now);

    std::filesystem::remove(path);
}

TEST(AofRewriterTest, RewriteHashKeysCanBeReplayed) {
    const auto path = testPath("hyperredis-aof-rewriter-hash.aof");
    std::filesystem::remove(path);

    const auto now = makeTime(1'000);
    RedisManager source(1);
    ASSERT_NE(source.db(0), nullptr);
    auto hash = RedisObject::createSharedHashObject();
    EXPECT_TRUE(hash->hashSet("name", RedisObject::createSharedStringObject("alice")));
    EXPECT_TRUE(hash->hashSet("age", RedisObject::createSharedLongObject(20)));
    source.db(0)->set("user", hash);

    AofRewriter rewriter(path);
    ASSERT_TRUE(rewriter.rewrite(source, now));

    RedisManager target(1);
    ASSERT_TRUE(AofReplayer::replay(path, target, now).ok);

    ASSERT_NE(target.db(0), nullptr);
    expectHashField(*target.db(0), "user", "name", "alice", now);
    expectHashField(*target.db(0), "user", "age", "20", now);
    auto obj = target.db(0)->get("user", now);
    ASSERT_NE(obj, nullptr);
    EXPECT_EQ(obj->hashSize(), 2U);

    std::filesystem::remove(path);
}

TEST(AofRewriterTest, RewriteSetKeysCanBeReplayed) {
    const auto path = testPath("hyperredis-aof-rewriter-set.aof");
    std::filesystem::remove(path);

    const auto now = makeTime(1'000);
    RedisManager source(1);
    ASSERT_NE(source.db(0), nullptr);
    auto set = RedisObject::createSharedSetObject();
    EXPECT_TRUE(set->setAdd("alpha"));
    EXPECT_TRUE(set->setAdd("42"));
    EXPECT_TRUE(set->setAdd("beta"));
    source.db(0)->set("tags", set);

    AofRewriter rewriter(path);
    ASSERT_TRUE(rewriter.rewrite(source, now));

    RedisManager target(1);
    ASSERT_TRUE(AofReplayer::replay(path, target, now).ok);

    ASSERT_NE(target.db(0), nullptr);
    expectSet(*target.db(0), "tags", {"alpha", "42", "beta"}, now);

    std::filesystem::remove(path);
}

TEST(AofRewriterTest, RewriteZSetKeysCanBeReplayed) {
    const auto path = testPath("hyperredis-aof-rewriter-zset.aof");
    std::filesystem::remove(path);

    const auto now = makeTime(1'000);
    RedisManager source(1);
    ASSERT_NE(source.db(0), nullptr);
    auto zset = RedisObject::createSharedZSetObject();
    EXPECT_TRUE(zset->zSetAdd("alice", 10.5));
    EXPECT_TRUE(zset->zSetAdd("bob", 7.0));
    EXPECT_TRUE(zset->zSetAdd("carol", 10.5));
    source.db(0)->set("leaders", zset);

    AofRewriter rewriter(path);
    ASSERT_TRUE(rewriter.rewrite(source, now));

    RedisManager target(1);
    ASSERT_TRUE(AofReplayer::replay(path, target, now).ok);

    ASSERT_NE(target.db(0), nullptr);
    auto obj = target.db(0)->get("leaders", now);
    ASSERT_NE(obj, nullptr);
    ASSERT_EQ(obj->getType(), ObjectType::ZSet);
    EXPECT_EQ(obj->zSetSize(), 3U);
    ASSERT_TRUE(obj->zSetScore("alice").has_value());
    ASSERT_TRUE(obj->zSetScore("bob").has_value());
    ASSERT_TRUE(obj->zSetScore("carol").has_value());
    EXPECT_DOUBLE_EQ(obj->zSetScore("alice").value(), 10.5);
    EXPECT_DOUBLE_EQ(obj->zSetScore("bob").value(), 7.0);
    EXPECT_DOUBLE_EQ(obj->zSetScore("carol").value(), 10.5);
    EXPECT_EQ(obj->zSetRange(0, -1), (RedisObject::ZSetRangeResult{{"bob", 7.0}, {"alice", 10.5}, {"carol", 10.5}}));

    std::filesystem::remove(path);
}

TEST(AofRewriterTest, RewritePreservesRemainingTtl) {
    const auto path = testPath("hyperredis-aof-rewriter-ttl.aof");
    std::filesystem::remove(path);

    const auto now = makeTime(10'000);
    const auto rewrite_now = now + Milliseconds{500};
    RedisManager source(3);
    ASSERT_NE(source.db(0), nullptr);
    ASSERT_NE(source.db(2), nullptr);
    source.db(0)->set("volatile", RedisObject::createSharedStringObject("db0-value"));
    source.db(0)->set("persistent", RedisObject::createSharedStringObject("keep"));
    source.db(2)->set("volatile", RedisObject::createSharedStringObject("db2-value"));
    ASSERT_TRUE(source.db(0)->expireAfter("volatile", Milliseconds{2'500}, now));
    ASSERT_TRUE(source.db(2)->expireAfter("volatile", Milliseconds{1'500}, now));

    AofRewriter rewriter(path);
    ASSERT_TRUE(rewriter.rewrite(source, rewrite_now));

    RedisManager target(3);
    ASSERT_TRUE(AofReplayer::replay(path, target, rewrite_now).ok);

    ASSERT_NE(target.db(0), nullptr);
    ASSERT_NE(target.db(2), nullptr);
    expectString(*target.db(0), "volatile", "db0-value", rewrite_now);
    expectString(*target.db(0), "persistent", "keep", rewrite_now);
    expectString(*target.db(2), "volatile", "db2-value", rewrite_now);
    EXPECT_EQ(target.db(0)->pttl("volatile", rewrite_now), 2'000);
    EXPECT_EQ(target.db(0)->pttl("persistent", rewrite_now), -1);
    EXPECT_EQ(target.db(2)->pttl("volatile", rewrite_now), 1'000);

    std::filesystem::remove(path);
}

TEST(AofRewriterTest, RewriteUsesAbsoluteExpireTimeWhenReplayIsDelayed) {
    const auto path = testPath("hyperredis-aof-rewriter-absolute-ttl.aof");
    std::filesystem::remove(path);

    const auto now = makeTime(10'000);
    const auto replay_now = now + Milliseconds{1'500};
    RedisManager source(1);
    ASSERT_NE(source.db(0), nullptr);
    source.db(0)->set("soon", RedisObject::createSharedStringObject("value"));
    ASSERT_TRUE(source.db(0)->expireAfter("soon", Milliseconds{1'000}, now));

    AofRewriter rewriter(path);
    ASSERT_TRUE(rewriter.rewrite(source, now));

    RedisManager target(1);
    ASSERT_TRUE(AofReplayer::replay(path, target, replay_now).ok);

    ASSERT_NE(target.db(0), nullptr);
    EXPECT_EQ(target.db(0)->get("soon", replay_now), nullptr);

    std::filesystem::remove(path);
}
