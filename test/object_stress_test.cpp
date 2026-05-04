#include <gtest/gtest.h>
#include <hyper/storage/object.hpp>
#include <string>
#include <vector>
#include <random>

using namespace hyper;

// 1. String Stress Tests
TEST(ObjectStressTest, StringBoundaryAndRange) {
    auto obj = RedisObject::createStringObject("base");
    
    // Large offset for setRange
    obj->stringSetRange(100, "end");
    std::string s = obj->asString();
    EXPECT_EQ(s.size(), 103);
    EXPECT_EQ(s.substr(0, 4), "base");
    EXPECT_EQ(s.substr(100), "end");
    for(int i=4; i<100; ++i) EXPECT_EQ(s[i], '\0');

    // stringIncrBy near limits
    auto longObj = RedisObject::createLongObject(2147483647); // max int32
    auto res = longObj->stringIncrBy(1);
    EXPECT_EQ(*res, 2147483648L);

    // stringGetRange with extreme indices
    EXPECT_EQ(obj->stringGetRange(-1000, 1000), s);
    EXPECT_EQ(obj->stringGetRange(500, 600), "");
}

// 2. List Stress Tests (Encoding Transitions & Patterns)
TEST(ObjectStressTest, ListComplexTransitions) {
    auto list = RedisObject::createListObject();
    
    // Fill to near threshold
    for (int i = 0; i < RedisObject::ZipListMaxEntries; ++i) {
        list->listRightPush(RedisObject::createStringObject("item" + std::to_string(i)));
    }
    EXPECT_EQ(list->getEncoding(), ObjectEncoding::ZipList);

    // Trigger conversion via listInsert with a LARGE value
    std::string large(RedisObject::ZipListMaxValue + 1, 'v');
    list->listInsert("item0", RedisObject::createStringObject(large), false);
    
    EXPECT_EQ(list->getEncoding(), ObjectEncoding::LinkedList);
    EXPECT_EQ(list->listLen(), RedisObject::ZipListMaxEntries + 1);
    EXPECT_EQ(list->listIndex(1)->asString(), large);

    // Mixed listRemove patterns
    list->listRightPush(RedisObject::createStringObject("dup"));
    list->listLeftPush(RedisObject::createStringObject("dup"));
    list->listInsert(large, RedisObject::createStringObject("dup"), true);
    
    // Current state might have 3 "dup"s. Let's verify and remove.
    size_t removed = list->listRemove(0, "dup");
    EXPECT_GE(removed, 3);
}

// 3. Hash Nesting Stress
TEST(ObjectStressTest, HashDeepNesting) {
    auto root = RedisObject::createHashObject();
    
    // We need a shared_ptr to walk and assign, but root is unique.
    // In real Redis, the database would own the shared_ptr.
    // For this test, let's use a raw pointer for the root and keep it alive in the unique_ptr.
    RedisObject* current = root.get();
    
    // Create a chain of nested hashes: root -> h1 -> h2 -> h3 -> val
    for (int i = 0; i < 5; ++i) {
        auto next = RedisObject::createHashObject();
        // hashSet takes shared_ptr, we can convert unique to shared
        std::shared_ptr<RedisObject> nextShared = std::move(next);
        current->hashSet("next", nextShared);
        current = nextShared.get();
    }
    
    current->hashSet("leaf", RedisObject::createStringObject("found"));

    // Traverse back down
    RedisObject* walker = root.get();
    for (int i = 0; i < 5; ++i) {
        auto next = walker->hashGet("next");
        ASSERT_NE(next, nullptr);
        walker = next.get();
    }
    EXPECT_EQ(walker->hashGet("leaf")->asString(), "found");
}

// 4. ZSet Ranking & Update Stress
TEST(ObjectStressTest, ZSetRankAndScoreUpdates) {
    auto zset = RedisObject::createZSetObject();
    
    // Add 100 elements with identical scores to test lexical ordering
    for (int i = 0; i < 100; ++i) {
        std::string member = "mem" + std::to_string(i + 1000); // stable sortable strings
        zset->zSetAdd(member, 10.0);
    }
    
    // Verify rank of a middle element
    auto rank = zset->zSetRank("mem1050");
    ASSERT_TRUE(rank.has_value());
    
    // Update score to move it to the end
    zset->zSetAdd("mem1050", 100.0);
    auto newRank = zset->zSetRank("mem1050");
    EXPECT_EQ(*newRank, zset->zSetSize() - 1);

    // Update score to move it to the front
    zset->zSetAdd("mem1050", -1.0);
    EXPECT_EQ(*zset->zSetRank("mem1050"), 0);
}

// 5. Set Encoding Transition during Add
TEST(ObjectStressTest, SetEncodingMidSequence) {
    auto set = RedisObject::createSetObject();
    
    // Add integers
    for (int i = 0; i < 10; ++i) {
        set->setAdd(std::to_string(i));
    }
    EXPECT_EQ(set->getEncoding(), ObjectEncoding::IntSet);

    // Add a string to force transition
    set->setAdd("not-an-int");
    EXPECT_EQ(set->getEncoding(), ObjectEncoding::HashTable);
    
    // Add more integers and verify they are all there
    for (int i = 10; i < 20; ++i) {
        set->setAdd(std::to_string(i));
    }
    
    EXPECT_EQ(set->setSize(), 21); // 0-19 + "not-an-int"
    EXPECT_TRUE(set->setContains("5"));
    EXPECT_TRUE(set->setContains("not-an-int"));
}
