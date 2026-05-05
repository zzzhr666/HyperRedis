#include "hyper/storage/object.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace hyper;

namespace {
    std::shared_ptr<RedisObject> str(std::string_view value) {
        return RedisObject::createSharedStringObject(value);
    }

    std::vector<std::string> toStrings(const std::vector<std::shared_ptr<RedisObject>>& values) {
        std::vector<std::string> result;
        result.reserve(values.size());
        for (const auto& value : values) {
            result.push_back(value->asString());
        }
        return result;
    }

    std::vector<std::string> sorted(std::vector<std::string> values) {
        std::ranges::sort(values);
        return values;
    }

    std::vector<std::pair<std::string, double>> sortedZSetPairs(
        std::vector<std::pair<std::string, double>> values) {
        std::ranges::sort(values, [](const auto& left, const auto& right) {
            if (left.second == right.second) {
                return left.first < right.first;
            }
            return left.second < right.second;
        });
        return values;
    }
}

TEST(RedisObjectRedisBehaviorTest, StringCommandsMatchRedisVisibleSemantics) {
    auto value = RedisObject::createSharedStringObject("42");
    EXPECT_EQ(value->getType(), ObjectType::String);
    EXPECT_EQ(value->getEncoding(), ObjectEncoding::Int);
    EXPECT_EQ(value->asString(), "42");
    EXPECT_EQ(value->stringLen(), 2U);

    ASSERT_EQ(value->stringIncrBy(8), std::optional<long>{50});
    EXPECT_EQ(value->asString(), "50");

    value->append(" apples");
    EXPECT_EQ(value->getEncoding(), ObjectEncoding::Raw);
    EXPECT_EQ(value->asString(), "50 apples");
    EXPECT_FALSE(value->stringIncrBy(1).has_value());

    EXPECT_EQ(value->stringGetRange(0, 1), "50");
    EXPECT_EQ(value->stringGetRange(-6, -1), "apples");
    EXPECT_EQ(value->stringGetRange(20, 30), "");

    auto sparse = RedisObject::createSharedStringObject("abc");
    sparse->stringSetRange(5, "Z");
    EXPECT_EQ(sparse->stringLen(), 6U);
    EXPECT_EQ(sparse->stringGetRange(0, 2), "abc");
    EXPECT_EQ(sparse->stringGetRange(5, 5), "Z");
    EXPECT_EQ(sparse->asString()[3], '\0');
    EXPECT_EQ(sparse->asString()[4], '\0');

    auto floating = RedisObject::createSharedStringObject("1.5");
    auto updated = floating->stringIncrByFloat(2.25);
    ASSERT_TRUE(updated.has_value());
    EXPECT_DOUBLE_EQ(*updated, 3.75);
    EXPECT_EQ(floating->asString(), "3.75");
}

TEST(RedisObjectRedisBehaviorTest, NumericStringOperationsRejectOverflowAndNaN) {
    auto max_value = RedisObject::createSharedLongObject(std::numeric_limits<long>::max());
    EXPECT_FALSE(max_value->stringIncrBy(1).has_value());
    EXPECT_EQ(max_value->asString(), std::to_string(std::numeric_limits<long>::max()));

    auto min_value = RedisObject::createSharedLongObject(std::numeric_limits<long>::min());
    EXPECT_FALSE(min_value->stringIncrBy(-1).has_value());
    EXPECT_EQ(min_value->asString(), std::to_string(std::numeric_limits<long>::min()));

    auto raw = RedisObject::createSharedStringObject(std::to_string(std::numeric_limits<long>::max()));
    raw->append("");
    ASSERT_EQ(raw->getEncoding(), ObjectEncoding::Raw);
    EXPECT_FALSE(raw->stringIncrBy(1).has_value());
    EXPECT_EQ(raw->getEncoding(), ObjectEncoding::Raw);

    auto floating = RedisObject::createSharedStringObject("1.0");
    EXPECT_FALSE(floating->stringIncrByFloat(std::numeric_limits<double>::quiet_NaN()).has_value());
    EXPECT_EQ(floating->asString(), "1.0");
}

TEST(RedisObjectRedisBehaviorTest, ListCommandsPreserveRedisOrderAndIndexRulesAcrossEncodings) {
    auto list = RedisObject::createSharedListObject();
    EXPECT_EQ(list->getType(), ObjectType::List);
    EXPECT_EQ(list->getEncoding(), ObjectEncoding::ZipList);

    list->listRightPush(str("b"));
    list->listLeftPush(str("a"));
    list->listRightPush(str("c"));
    EXPECT_EQ(list->listLen(), 3U);
    EXPECT_EQ(toStrings(list->listRange(0, -1)), (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(toStrings(list->listRange(-2, -1)), (std::vector<std::string>{"b", "c"}));

    ASSERT_NE(list->listIndex(-1), nullptr);
    EXPECT_EQ(list->listIndex(-1)->asString(), "c");
    EXPECT_EQ(list->listIndex(100), nullptr);

    EXPECT_TRUE(list->listSet(1, str("B")));
    EXPECT_FALSE(list->listSet(10, str("x")));
    EXPECT_EQ(toStrings(list->listRange(0, -1)), (std::vector<std::string>{"a", "B", "c"}));

    ASSERT_EQ(list->listInsert("B", str("before-B"), true), std::optional<std::size_t>{4});
    ASSERT_EQ(list->listInsert("B", str("after-B"), false), std::optional<std::size_t>{5});
    EXPECT_FALSE(list->listInsert("missing", str("x"), true).has_value());
    EXPECT_EQ(toStrings(list->listRange(0, -1)),
              (std::vector<std::string>{"a", "before-B", "B", "after-B", "c"}));

    EXPECT_EQ(list->listLeftPop()->asString(), "a");
    EXPECT_EQ(list->listRightPop()->asString(), "c");
    EXPECT_EQ(toStrings(list->listRange(0, -1)),
              (std::vector<std::string>{"before-B", "B", "after-B"}));

    list->listRightPush(str("B"));
    list->listRightPush(str("B"));
    EXPECT_EQ(list->listRemove(2, "B"), 2U);
    EXPECT_EQ(toStrings(list->listRange(0, -1)), (std::vector<std::string>{"before-B", "after-B", "B"}));
    EXPECT_EQ(list->listRemove(-1, "B"), 1U);
    EXPECT_EQ(toStrings(list->listRange(0, -1)), (std::vector<std::string>{"before-B", "after-B"}));

    for (int i = 0; i < 20; ++i) {
        list->listRightPush(RedisObject::createSharedLongObject(i));
    }
    EXPECT_EQ(list->getEncoding(), ObjectEncoding::LinkedList);
    list->listTrim(-5, -1);
    EXPECT_EQ(toStrings(list->listRange(0, -1)),
              (std::vector<std::string>{"15", "16", "17", "18", "19"}));
}

TEST(RedisObjectRedisBehaviorTest, ListRemoveFromBackDeletesOnlyRequestedMatches) {
    auto list = RedisObject::createSharedListObject();
    for (int i = 0; i < 20; ++i) {
        list->listRightPush(str("x"));
    }
    ASSERT_EQ(list->getEncoding(), ObjectEncoding::LinkedList);

    EXPECT_EQ(list->listRemove(-2, "x"), 2U);
    EXPECT_EQ(list->listLen(), 18U);
}

TEST(RedisObjectRedisBehaviorTest, HashCommandsMatchRedisFieldSemanticsAcrossEncodings) {
    auto hash = RedisObject::createSharedHashObject();
    EXPECT_EQ(hash->getType(), ObjectType::Hash);
    EXPECT_EQ(hash->getEncoding(), ObjectEncoding::ZipList);

    EXPECT_TRUE(hash->hashSet("name", str("redis")));
    EXPECT_TRUE(hash->hashSet("version", RedisObject::createSharedLongObject(7)));
    EXPECT_FALSE(hash->hashSet("version", str("7.2")));
    EXPECT_EQ(hash->hashSize(), 2U);

    ASSERT_NE(hash->hashGet("name"), nullptr);
    EXPECT_EQ(hash->hashGet("name")->asString(), "redis");
    ASSERT_NE(hash->hashGet("version"), nullptr);
    EXPECT_EQ(hash->hashGet("version")->asString(), "7.2");
    EXPECT_EQ(hash->hashGet("missing"), nullptr);
    EXPECT_TRUE(hash->hashContains("name"));
    EXPECT_FALSE(hash->hashContains("missing"));

    EXPECT_EQ(sorted(hash->hashKeys()), (std::vector<std::string>{"name", "version"}));
    EXPECT_EQ(sorted(toStrings(hash->hashValues())), (std::vector<std::string>{"7.2", "redis"}));
    EXPECT_EQ(sorted(hash->hashValuesAsStrings()), (std::vector<std::string>{"7.2", "redis"}));

    std::vector<std::pair<std::string, std::string>> visited;
    hash->hashForEach([&visited](std::string_view field, const std::shared_ptr<RedisObject>& value) {
        visited.emplace_back(field, value->asString());
    });
    std::ranges::sort(visited);
    EXPECT_EQ(visited, (std::vector<std::pair<std::string, std::string>>{
                           {"name", "redis"},
                           {"version", "7.2"},
                       }));

    for (std::size_t i = 0; i <= RedisObject::HashZipListMaxEntries; ++i) {
        hash->hashSet("field-" + std::to_string(i), str("value-" + std::to_string(i)));
    }
    EXPECT_EQ(hash->getEncoding(), ObjectEncoding::HashTable);
    EXPECT_EQ(hash->hashGet("field-0")->asString(), "value-0");
    EXPECT_TRUE(hash->hashRemove("field-0"));
    EXPECT_FALSE(hash->hashRemove("field-0"));
    EXPECT_EQ(hash->hashGet("field-0"), nullptr);
    EXPECT_EQ(hash->hashGet("field-1")->asString(), "value-1");
    EXPECT_FALSE(hash->hashGetAllAsStrings().empty());
}

TEST(RedisObjectRedisBehaviorTest, SetCommandsMatchRedisMembershipSemanticsAcrossEncodings) {
    auto set = RedisObject::createSharedSetObject();
    EXPECT_EQ(set->getType(), ObjectType::Set);
    EXPECT_EQ(set->getEncoding(), ObjectEncoding::IntSet);

    EXPECT_TRUE(set->setAdd("1"));
    EXPECT_TRUE(set->setAdd("2"));
    EXPECT_FALSE(set->setAdd("1"));
    EXPECT_EQ(set->setSize(), 2U);
    EXPECT_TRUE(set->setContains("1"));
    EXPECT_FALSE(set->setContains("3"));

    std::vector<std::string> members;
    set->setForEach([&members](std::string_view member) {
        members.emplace_back(member);
    });
    EXPECT_EQ(sorted(members), (std::vector<std::string>{"1", "2"}));

    EXPECT_TRUE(set->setAdd("01"));
    EXPECT_EQ(set->getEncoding(), ObjectEncoding::HashTable);
    EXPECT_TRUE(set->setContains("01"));
    EXPECT_TRUE(set->setContains("1"));
    EXPECT_FALSE(set->setAdd("01"));

    auto random = set->setRandomMember();
    ASSERT_NE(random, nullptr);
    EXPECT_TRUE(set->setContains(random->asString()));
    auto random_string = set->setRandomMemberString();
    ASSERT_TRUE(random_string.has_value());
    EXPECT_TRUE(set->setContains(*random_string));
    EXPECT_EQ(set->setSize(), 3U);

    auto popped = set->setPop();
    ASSERT_NE(popped, nullptr);
    EXPECT_FALSE(set->setContains(popped->asString()));
    EXPECT_EQ(set->setSize(), 2U);

    auto popped_string = set->setPopString();
    ASSERT_TRUE(popped_string.has_value());
    EXPECT_FALSE(set->setContains(*popped_string));
    EXPECT_EQ(set->setSize(), 1U);

    EXPECT_TRUE(set->setRemove("1") || set->setRemove("2") || set->setRemove("01"));
}

TEST(RedisObjectRedisBehaviorTest, ZSetCommandsMatchRedisOrderingSemanticsAcrossEncodings) {
    auto zset = RedisObject::createSharedZSetObject();
    EXPECT_EQ(zset->getType(), ObjectType::ZSet);
    EXPECT_EQ(zset->getEncoding(), ObjectEncoding::ZipList);

    EXPECT_TRUE(zset->zSetAdd("beta", 2.0));
    EXPECT_TRUE(zset->zSetAdd("alpha", 1.0));
    EXPECT_TRUE(zset->zSetAdd("gamma", 2.0));
    EXPECT_EQ(zset->zSetAddDetailed("delta", std::numeric_limits<double>::quiet_NaN()),
              ZSetAddStatus::InvalidScore);
    EXPECT_FALSE(zset->zSetAdd("beta", 3.0));
    EXPECT_EQ(zset->zSetSize(), 3U);
    EXPECT_EQ(zset->zSetRange(0, -1),
              (RedisObject::ZSetRangeResult{{"alpha", 1.0}, {"gamma", 2.0}, {"beta", 3.0}}));
    EXPECT_EQ(zset->zSetRevRange(0, -1),
              (RedisObject::ZSetRangeResult{{"beta", 3.0}, {"gamma", 2.0}, {"alpha", 1.0}}));
    EXPECT_EQ(zset->zSetRank("alpha"), 0U);
    EXPECT_EQ(zset->zSetRevRank("alpha"), 2U);
    EXPECT_EQ(zset->zSetScore("beta"), std::optional<double>{3.0});
    EXPECT_EQ(zset->zSetCount(2.0, 3.0), 2U);

    EXPECT_DOUBLE_EQ(zset->zSetIncrBy("alpha", 5.0), 6.0);
    EXPECT_FALSE(zset->zSetIncrByChecked("alpha", std::numeric_limits<double>::quiet_NaN()).has_value());
    EXPECT_EQ(zset->zSetRevRank("alpha"), 0U);
    EXPECT_TRUE(zset->zSetRemove("gamma"));
    EXPECT_FALSE(zset->zSetRemove("gamma"));
    EXPECT_FALSE(zset->zSetScore("gamma").has_value());

    for (int i = 0; i < 20; ++i) {
        zset->zSetAdd("m" + std::to_string(i), static_cast<double>(i));
    }
    EXPECT_EQ(zset->getEncoding(), ObjectEncoding::SkipList);

    EXPECT_EQ(zset->zSetRemoveRangeByScore(5.0, 8.0), 5U);
    EXPECT_FALSE(zset->zSetScore("alpha").has_value());
    for (int i = 5; i <= 8; ++i) {
        EXPECT_FALSE(zset->zSetScore("m" + std::to_string(i)).has_value());
    }

    EXPECT_EQ(zset->zSetRemoveRangeByRank(0, 1), 2U);
    auto remaining = zset->zSetRange(0, -1);
    EXPECT_EQ(remaining, sortedZSetPairs(remaining));
    EXPECT_FALSE(zset->zSetRank("m0").has_value());
    EXPECT_FALSE(zset->zSetRank("m1").has_value());
}

TEST(RedisObjectRedisBehaviorTest, EncodingTransitionsPreserveCommandResults) {
    auto hash = RedisObject::createSharedHashObject();
    std::string large_value(RedisObject::ZipListMaxValue + 1, 'x');
    hash->hashSet("large", str(large_value));
    ASSERT_EQ(hash->getEncoding(), ObjectEncoding::HashTable);
    EXPECT_EQ(hash->hashGet("large")->asString(), large_value);

    auto list = RedisObject::createSharedListObject();
    list->listRightPush(str("head"));
    list->listRightPush(str(large_value));
    ASSERT_EQ(list->getEncoding(), ObjectEncoding::LinkedList);
    EXPECT_EQ(toStrings(list->listRange(0, -1)), (std::vector<std::string>{"head", large_value}));

    auto set = RedisObject::createSharedSetObject();
    for (std::size_t i = 0; i <= RedisObject::SetMaxIntSetEntries; ++i) {
        set->setAdd(std::to_string(i));
    }
    ASSERT_EQ(set->getEncoding(), ObjectEncoding::HashTable);
    EXPECT_TRUE(set->setContains("0"));
    EXPECT_TRUE(set->setContains(std::to_string(RedisObject::SetMaxIntSetEntries)));

    auto zset = RedisObject::createSharedZSetObject();
    for (std::size_t i = 0; i <= RedisObject::ZSetZipListMaxEntries; ++i) {
        zset->zSetAdd("member-" + std::to_string(i), static_cast<double>(i));
    }
    ASSERT_EQ(zset->getEncoding(), ObjectEncoding::SkipList);
    EXPECT_EQ(zset->zSetRank("member-0"), 0U);
    EXPECT_EQ(zset->zSetRank("member-" + std::to_string(RedisObject::ZSetZipListMaxEntries)),
              RedisObject::ZSetZipListMaxEntries);
}
