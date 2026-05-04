#include <gtest/gtest.h>
#include <hyper/storage/object.hpp>
#include <string>

using namespace hyper;

class ZSetObjectTest : public ::testing::Test {
protected:
    std::unique_ptr<RedisObject> zset;
    void SetUp() override {
        zset = RedisObject::createZSetObject();
    }
};

TEST_F(ZSetObjectTest, BasicAddAndScore) {
    EXPECT_EQ(zset->getEncoding(), ObjectEncoding::ZipList);

    EXPECT_TRUE(zset->zSetAdd("user1", 10.5));
    EXPECT_TRUE(zset->zSetAdd("user2", 20.0));
    EXPECT_EQ(zset->zSetSize(), 2);

    auto score1 = zset->zSetScore("user1");
    ASSERT_TRUE(score1.has_value());
    EXPECT_DOUBLE_EQ(*score1, 10.5);

    auto score2 = zset->zSetScore("user2");
    ASSERT_TRUE(score2.has_value());
    EXPECT_DOUBLE_EQ(*score2, 20.0);

    EXPECT_FALSE(zset->zSetScore("nonexistent").has_value());
}

TEST_F(ZSetObjectTest, UpdateScore) {
    zset->zSetAdd("user1", 10.0);
    // 更新分值，返回 false 表示是更新操作
    EXPECT_FALSE(zset->zSetAdd("user1", 15.5));
    EXPECT_EQ(zset->zSetSize(), 1);

    auto score = zset->zSetScore("user1");
    ASSERT_TRUE(score.has_value());
    EXPECT_DOUBLE_EQ(*score, 15.5);
}

TEST_F(ZSetObjectTest, ZipListToSkipListUpgrade) {
    EXPECT_EQ(zset->getEncoding(), ObjectEncoding::ZipList);

    // 插入超过 ZSetZipListMaxEntries (16) 个成员
    for (int i = 0; i < 20; ++i) {
        std::string member = "user" + std::to_string(i);
        zset->zSetAdd(member, static_cast<double>(i * 10));
    }

    // 检查编码是否升级
    EXPECT_EQ(zset->getEncoding(), ObjectEncoding::SkipList);
    EXPECT_EQ(zset->zSetSize(), 20);

    // 验证数据在升级后是否依然正确
    for (int i = 0; i < 20; ++i) {
        std::string member = "user" + std::to_string(i);
        auto score = zset->zSetScore(member);
        ASSERT_TRUE(score.has_value()) << "Failed for " << member;
        EXPECT_DOUBLE_EQ(*score, static_cast<double>(i * 10));
    }
}

TEST_F(ZSetObjectTest, LargeValueTriggersUpgrade) {
    EXPECT_EQ(zset->getEncoding(), ObjectEncoding::ZipList);

    // 插入一个超长字符串成员 (超过 ZipListMaxValue=64)
    std::string large_member(70, 'a');
    zset->zSetAdd(large_member, 100.0);

    EXPECT_EQ(zset->getEncoding(), ObjectEncoding::SkipList);
    auto score = zset->zSetScore(large_member);
    ASSERT_TRUE(score.has_value());
    EXPECT_DOUBLE_EQ(*score, 100.0);
}

TEST_F(ZSetObjectTest, RemoveMember) {
    zset->zSetAdd("user1", 10.0);
    zset->zSetAdd("user2", 20.0);
    EXPECT_EQ(zset->zSetSize(), 2);

    // 删除存在的成员
    EXPECT_TRUE(zset->zSetRemove("user1"));
    EXPECT_EQ(zset->zSetSize(), 1);
    EXPECT_FALSE(zset->zSetScore("user1").has_value());

    // 删除不存在的成员
    EXPECT_FALSE(zset->zSetRemove("nonexistent"));
    EXPECT_EQ(zset->zSetSize(), 1);

    // 触发升级后再删除
    for (int i = 0; i < 20; ++i) {
        zset->zSetAdd("extra" + std::to_string(i), static_cast<double>(i));
    }
    EXPECT_EQ(zset->getEncoding(), ObjectEncoding::SkipList);
    
    EXPECT_TRUE(zset->zSetRemove("user2"));
    EXPECT_FALSE(zset->zSetScore("user2").has_value());
}

TEST_F(ZSetObjectTest, Rank) {
    // Redis ZSet 默认按 score 升序排列
    zset->zSetAdd("a", 30.0);
    zset->zSetAdd("b", 10.0);
    zset->zSetAdd("c", 20.0);

    // 预期顺序: b(10), c(20), a(30)
    EXPECT_EQ(zset->zSetRank("b"), 0);
    EXPECT_EQ(zset->zSetRank("c"), 1);
    EXPECT_EQ(zset->zSetRank("a"), 2);
    EXPECT_FALSE(zset->zSetRank("d").has_value());

    // 升级到 SkipList
    for (int i = 0; i < 20; ++i) {
        zset->zSetAdd("user" + std::to_string(i), 100.0 + i);
    }
    EXPECT_EQ(zset->getEncoding(), ObjectEncoding::SkipList);
    
    EXPECT_EQ(zset->zSetRank("b"), 0);
    EXPECT_EQ(zset->zSetRank("user0"), 3);
}

TEST_F(ZSetObjectTest, LexicalOrderForSameScore) {
    // 当 score 相同时，按成员名的字典序排列
    zset->zSetAdd("banana", 10.0);
    zset->zSetAdd("apple", 10.0);
    zset->zSetAdd("cherry", 10.0);

    // 预期顺序: apple, banana, cherry
    EXPECT_EQ(zset->zSetRank("apple"), 0);
    EXPECT_EQ(zset->zSetRank("banana"), 1);
    EXPECT_EQ(zset->zSetRank("cherry"), 2);
}

