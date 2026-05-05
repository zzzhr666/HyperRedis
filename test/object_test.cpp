#include <gtest/gtest.h>
#include <hyper/storage/object.hpp>
#include <map>
#include <set>

using namespace hyper;

TEST(ObjectTest, StringToLongEncoding) {
    // 正常整数
    auto obj1 = RedisObject::createSharedStringObject("123");
    EXPECT_EQ(obj1->getType(), ObjectType::String);
    EXPECT_EQ(obj1->getEncoding(), ObjectEncoding::Int);

    // 负数
    auto obj2 = RedisObject::createSharedStringObject("-456");
    EXPECT_EQ(obj2->getEncoding(), ObjectEncoding::Int);
}

TEST(ObjectTest, StringNormalization) {
    // 前导零：不应转为整数
    auto obj1 = RedisObject::createSharedStringObject("0123");
    EXPECT_EQ(obj1->getEncoding(), ObjectEncoding::Raw);

    // 负号加前导零：不应转为整数
    auto obj2 = RedisObject::createSharedStringObject("-01");
    EXPECT_EQ(obj2->getEncoding(), ObjectEncoding::Raw);

    // 只有 "0"：应该转为整数
    auto obj3 = RedisObject::createSharedStringObject("0");
    EXPECT_EQ(obj3->getEncoding(), ObjectEncoding::Int);
}

TEST(ObjectTest, RawString) {
    // 非数字
    auto obj1 = RedisObject::createSharedStringObject("hello");
    EXPECT_EQ(obj1->getEncoding(), ObjectEncoding::Raw);

    // 包含空格的数字
    auto obj2 = RedisObject::createSharedStringObject(" 123 ");
    EXPECT_EQ(obj2->getEncoding(), ObjectEncoding::Raw);
}

TEST(ObjectTest, LongObject) {
    auto obj = RedisObject::createSharedLongObject(100);
    EXPECT_EQ(obj->getEncoding(), ObjectEncoding::Int);
}

TEST(ObjectTest, Accessors) {
    // 测试 Int 编码的访问
    auto obj1 = RedisObject::createSharedStringObject("123");
    EXPECT_EQ(obj1->getEncoding(), ObjectEncoding::Int);
    EXPECT_EQ(obj1->asString(), "123");

    // 测试 Raw 编码的访问
    auto obj2 = RedisObject::createSharedStringObject("hello");
    EXPECT_EQ(obj2->getEncoding(), ObjectEncoding::Raw);
    EXPECT_EQ(obj2->asString(), "hello");
}

TEST(ObjectTest, AppendLogic) {
    // 测试从 Int 转换到 Raw 的追加
    auto obj = RedisObject::createSharedStringObject("100");
    EXPECT_EQ(obj->getEncoding(), ObjectEncoding::Int);
    
    obj->append("50"); 
    EXPECT_EQ(obj->getEncoding(), ObjectEncoding::Raw);
    EXPECT_EQ(obj->asString(), "10050");

    // 测试在 Raw 上的持续追加
    obj->append("!");
    EXPECT_EQ(obj->asString(), "10050!");
}

TEST(ObjectTest, HashStoresStringValuesInZipList) {
    auto hashObj = RedisObject::createSharedHashObject();
    EXPECT_EQ(hashObj->getType(), ObjectType::Hash);
    EXPECT_EQ(hashObj->getEncoding(), ObjectEncoding::ZipList);

    auto val1 = RedisObject::createSharedStringObject("val1");
    auto val2 = RedisObject::createSharedLongObject(100);

    EXPECT_TRUE(hashObj->hashSet("field1", val1));
    EXPECT_TRUE(hashObj->hashSet("field2", val2));
    EXPECT_EQ(hashObj->hashSize(), 2);
    EXPECT_TRUE(hashObj->hashContains("field1"));
    EXPECT_TRUE(hashObj->hashContains("field2"));
    EXPECT_FALSE(hashObj->hashContains("missing"));

    auto res1 = hashObj->hashGet("field1");
    ASSERT_NE(res1, nullptr);
    EXPECT_EQ(res1->asString(), "val1");

    auto res2 = hashObj->hashGet("field2");
    ASSERT_NE(res2, nullptr);
    EXPECT_EQ(res2->getEncoding(), ObjectEncoding::Int);
    EXPECT_EQ(res2->asString(), "100");
}

TEST(ObjectTest, HashSetUpdatesExistingField) {
    auto hashObj = RedisObject::createSharedHashObject();

    EXPECT_TRUE(hashObj->hashSet("field", RedisObject::createSharedStringObject("old")));
    EXPECT_EQ(hashObj->hashSize(), 1);

    EXPECT_FALSE(hashObj->hashSet("field", RedisObject::createSharedStringObject("new")));
    EXPECT_EQ(hashObj->hashSize(), 1);

    auto value = hashObj->hashGet("field");
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(value->asString(), "new");
}

TEST(ObjectTest, HashRemoveDeletesExistingField) {
    auto hashObj = RedisObject::createSharedHashObject();

    EXPECT_TRUE(hashObj->hashSet("field1", RedisObject::createSharedStringObject("value1")));
    EXPECT_TRUE(hashObj->hashSet("field2", RedisObject::createSharedStringObject("value2")));
    EXPECT_EQ(hashObj->hashSize(), 2);

    EXPECT_TRUE(hashObj->hashRemove("field1"));
    EXPECT_EQ(hashObj->hashSize(), 1);
    EXPECT_FALSE(hashObj->hashContains("field1"));
    EXPECT_EQ(hashObj->hashGet("field1"), nullptr);
    EXPECT_TRUE(hashObj->hashContains("field2"));

    EXPECT_FALSE(hashObj->hashRemove("field1"));
    EXPECT_FALSE(hashObj->hashRemove("missing"));
    EXPECT_EQ(hashObj->hashSize(), 1);
}

TEST(ObjectTest, HashContainsWorksThroughConstObject) {
    auto hashObj = RedisObject::createSharedHashObject();
    EXPECT_TRUE(hashObj->hashSet("field", RedisObject::createSharedStringObject("value")));

    const auto& const_hash = *hashObj;
    EXPECT_TRUE(const_hash.hashContains("field"));
    EXPECT_FALSE(const_hash.hashContains("missing"));
}

TEST(ObjectTest, HashForEachVisitsAllFieldsAndValues) {
    auto hashObj = RedisObject::createSharedHashObject();

    EXPECT_TRUE(hashObj->hashSet("field1", RedisObject::createSharedStringObject("value1")));
    EXPECT_TRUE(hashObj->hashSet("field2", RedisObject::createSharedLongObject(100)));

    std::map<std::string, std::string> entries;
    std::function<void(std::string_view, const std::shared_ptr<RedisObject>&)> collect =
        [&entries](std::string_view field, const std::shared_ptr<RedisObject>& value) {
            ASSERT_NE(value, nullptr);
            entries.emplace(std::string(field), value->asString());
        };

    hashObj->hashForEach(collect);

    EXPECT_EQ(entries, (std::map<std::string, std::string>{
                           {"field1", "value1"},
                           {"field2", "100"},
                       }));
}

TEST(ObjectTest, HashZipListLookupIgnoresMatchingValueEntries) {
    auto hashObj = RedisObject::createSharedHashObject();

    EXPECT_TRUE(hashObj->hashSet("field1", RedisObject::createSharedStringObject("field2")));
    EXPECT_TRUE(hashObj->hashSet("field2", RedisObject::createSharedStringObject("value2")));
    EXPECT_EQ(hashObj->hashSize(), 2);

    EXPECT_TRUE(hashObj->hashContains("field2"));
    auto value = hashObj->hashGet("field2");
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(value->asString(), "value2");

    EXPECT_FALSE(hashObj->hashRemove("value2"));
    EXPECT_TRUE(hashObj->hashRemove("field2"));
    EXPECT_EQ(hashObj->hashSize(), 1);
    EXPECT_TRUE(hashObj->hashContains("field1"));
    EXPECT_FALSE(hashObj->hashContains("field2"));
}

TEST(ObjectTest, HashUpgradesToHashTableAfterZipListEntryThreshold) {
    auto hashObj = RedisObject::createSharedHashObject();

    for (std::size_t i = 0; i < RedisObject::HashZipListMaxEntries; ++i) {
        EXPECT_TRUE(hashObj->hashSet("field" + std::to_string(i),
                                     RedisObject::createSharedStringObject("value" + std::to_string(i))));
    }

    EXPECT_EQ(hashObj->getEncoding(), ObjectEncoding::ZipList);
    EXPECT_EQ(hashObj->hashSize(), RedisObject::HashZipListMaxEntries);

    EXPECT_TRUE(hashObj->hashSet("field" + std::to_string(RedisObject::HashZipListMaxEntries),
                                 RedisObject::createSharedStringObject("value-over-threshold")));
    EXPECT_EQ(hashObj->getEncoding(), ObjectEncoding::HashTable);
    EXPECT_EQ(hashObj->hashSize(), RedisObject::HashZipListMaxEntries + 1);

    auto first = hashObj->hashGet("field0");
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first->asString(), "value0");

    auto last = hashObj->hashGet("field" + std::to_string(RedisObject::HashZipListMaxEntries));
    ASSERT_NE(last, nullptr);
    EXPECT_EQ(last->asString(), "value-over-threshold");

    EXPECT_FALSE(hashObj->hashSet("field0", RedisObject::createSharedStringObject("updated")));
    EXPECT_EQ(hashObj->hashSize(), RedisObject::HashZipListMaxEntries + 1);
    EXPECT_EQ(hashObj->hashGet("field0")->asString(), "updated");
}

TEST(ObjectTest, HashUpgradesToHashTableForLargeFieldOrValue) {
    auto hashObj = RedisObject::createSharedHashObject();
    std::string large_value(RedisObject::ZipListMaxValue + 1, 'x');

    EXPECT_TRUE(hashObj->hashSet("field", RedisObject::createSharedStringObject(large_value)));
    EXPECT_EQ(hashObj->getEncoding(), ObjectEncoding::HashTable);
    EXPECT_EQ(hashObj->hashSize(), 1);

    auto value = hashObj->hashGet("field");
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(value->asString(), large_value);

    auto hashObj2 = RedisObject::createSharedHashObject();
    std::string large_field(RedisObject::ZipListMaxValue + 1, 'f');

    EXPECT_TRUE(hashObj2->hashSet(large_field, RedisObject::createSharedStringObject("value")));
    EXPECT_EQ(hashObj2->getEncoding(), ObjectEncoding::HashTable);
    EXPECT_EQ(hashObj2->hashSize(), 1);
    EXPECT_EQ(hashObj2->hashGet(large_field)->asString(), "value");
}

TEST(ObjectTest, ListBasicOperations) {
    auto listObj = RedisObject::createSharedListObject();
    EXPECT_EQ(listObj->getEncoding(), ObjectEncoding::ZipList);

    // 测试 LPUSH 和 RPOP
    listObj->listLeftPush(RedisObject::createSharedStringObject("first"));
    listObj->listLeftPush(RedisObject::createSharedStringObject("second"));
    
    // 结构应该是: second -> first
    auto res1 = listObj->listRightPop();
    ASSERT_NE(res1, nullptr);
    EXPECT_EQ(res1->asString(), "first");

    auto res2 = listObj->listRightPop();
    ASSERT_NE(res2, nullptr);
    EXPECT_EQ(res2->asString(), "second");

    // 空 Pop 检查
    EXPECT_EQ(listObj->listLeftPop(), nullptr);
}

TEST(ObjectTest, ListSetUpdatesZipListByIndex) {
    auto listObj = RedisObject::createSharedListObject();

    listObj->listRightPush(RedisObject::createSharedStringObject("a"));
    listObj->listRightPush(RedisObject::createSharedStringObject("b"));
    listObj->listRightPush(RedisObject::createSharedStringObject("c"));

    EXPECT_TRUE(listObj->listSet(1, RedisObject::createSharedStringObject("middle")));
    EXPECT_EQ(listObj->listLen(), 3);
    EXPECT_EQ(listObj->listIndex(0)->asString(), "a");
    EXPECT_EQ(listObj->listIndex(1)->asString(), "middle");
    EXPECT_EQ(listObj->listIndex(2)->asString(), "c");

    EXPECT_TRUE(listObj->listSet(-1, RedisObject::createSharedLongObject(42)));
    EXPECT_EQ(listObj->listIndex(2)->asString(), "42");

    EXPECT_FALSE(listObj->listSet(3, RedisObject::createSharedStringObject("out")));
    EXPECT_FALSE(listObj->listSet(-4, RedisObject::createSharedStringObject("out")));
    EXPECT_EQ(listObj->listLen(), 3);
    EXPECT_EQ(listObj->getEncoding(), ObjectEncoding::ZipList);
}

TEST(ObjectTest, ListSetTriggersLinkedListUpgradeAndKeepsOrder) {
    auto listObj = RedisObject::createSharedListObject();
    std::string large_value(RedisObject::ZipListMaxValue + 1, 'x');

    listObj->listRightPush(RedisObject::createSharedStringObject("head"));
    listObj->listRightPush(RedisObject::createSharedStringObject("target"));
    listObj->listRightPush(RedisObject::createSharedStringObject("tail"));

    EXPECT_TRUE(listObj->listSet(1, RedisObject::createSharedStringObject(large_value)));
    EXPECT_EQ(listObj->getEncoding(), ObjectEncoding::LinkedList);
    EXPECT_EQ(listObj->listLen(), 3);
    EXPECT_EQ(listObj->listIndex(0)->asString(), "head");
    EXPECT_EQ(listObj->listIndex(1)->asString(), large_value);
    EXPECT_EQ(listObj->listIndex(2)->asString(), "tail");

    EXPECT_TRUE(listObj->listSet(-1, RedisObject::createSharedStringObject("new-tail")));
    EXPECT_EQ(listObj->listIndex(2)->asString(), "new-tail");
}

TEST(ObjectTest, ListInsert) {
    auto listObj = RedisObject::createSharedListObject();
    // 初始: ["a", "c"]
    listObj->listRightPush(RedisObject::createSharedStringObject("a"));
    listObj->listRightPush(RedisObject::createSharedStringObject("c"));

    // 1. ZipList 插入 (Before)
    auto res1 = listObj->listInsert("c", RedisObject::createSharedStringObject("b"), true);
    ASSERT_TRUE(res1.has_value());
    EXPECT_EQ(*res1, 3);
    EXPECT_EQ(listObj->listLen(), 3);
    // 现在: ["a", "b", "c"]
    EXPECT_EQ(listObj->listIndex(1)->asString(), "b");

    // 2. ZipList 插入 (After)
    auto res2 = listObj->listInsert("c", RedisObject::createSharedStringObject("d"), false);
    ASSERT_TRUE(res2.has_value());
    EXPECT_EQ(*res2, 4);
    EXPECT_EQ(listObj->listLen(), 4);
    // 现在: ["a", "b", "c", "d"]
    EXPECT_EQ(listObj->listIndex(3)->asString(), "d");

    // 3. Pivot 不存在
    EXPECT_FALSE(listObj->listInsert("e", RedisObject::createSharedStringObject("x"), true).has_value());

    // 4. LinkedList 编码下测试 (插入足够多元素触发转换)
    for (int i = 0; i < 20; ++i) {
        listObj->listRightPush(RedisObject::createSharedStringObject(std::to_string(i)));
    }
    EXPECT_EQ(listObj->getEncoding(), ObjectEncoding::LinkedList);

    // 在 LinkedList 插入
    auto res3 = listObj->listInsert("0", RedisObject::createSharedStringObject("before_0"), true);
    ASSERT_TRUE(res3.has_value());
    EXPECT_EQ(*res3, listObj->listLen());
}

TEST(ObjectTest, ListRemove) {
    auto listObj = RedisObject::createSharedListObject();
    // 初始: ["a", "b", "a", "c", "a"]
    listObj->listRightPush(RedisObject::createSharedStringObject("a"));
    listObj->listRightPush(RedisObject::createSharedStringObject("b"));
    listObj->listRightPush(RedisObject::createSharedStringObject("a"));
    listObj->listRightPush(RedisObject::createSharedStringObject("c"));
    listObj->listRightPush(RedisObject::createSharedStringObject("a"));

    // 1. 删除前 2 个 "a" (from_front)
    EXPECT_EQ(listObj->listRemove(2, "a"), 2);
    EXPECT_EQ(listObj->listLen(), 3);
    
    // 剩下: ["b", "c", "a"]
    EXPECT_EQ(listObj->listIndex(0)->asString(), "b");
    EXPECT_EQ(listObj->listIndex(1)->asString(), "c");
    EXPECT_EQ(listObj->listIndex(2)->asString(), "a");

    // 2. 删除所有 "a" (count = 0)
    EXPECT_EQ(listObj->listRemove(0, "a"), 1);
    EXPECT_EQ(listObj->listLen(), 2);
    
    // 剩下: ["b", "c"]
    EXPECT_EQ(listObj->listIndex(0)->asString(), "b");
    EXPECT_EQ(listObj->listIndex(1)->asString(), "c");

    // 3. 测试反向删除 (from_back)
    listObj->listRightPush(RedisObject::createSharedStringObject("b"));
    listObj->listRightPush(RedisObject::createSharedStringObject("b"));
    // 现在: ["b", "c", "b", "b"]
    
    // 删除最后 2 个 "b"
    EXPECT_EQ(listObj->listRemove(-2, "b"), 2);
    // 剩下: ["b", "c"]
    EXPECT_EQ(listObj->listLen(), 2);
    EXPECT_EQ(listObj->listIndex(0)->asString(), "b");
    EXPECT_EQ(listObj->listIndex(1)->asString(), "c");

    // 4. LinkedList 下的删除测试
    for (int i = 0; i < 20; ++i) {
        listObj->listRightPush(RedisObject::createSharedStringObject("x"));
    }
    EXPECT_EQ(listObj->getEncoding(), ObjectEncoding::LinkedList);
    EXPECT_EQ(listObj->listRemove(10, "x"), 10);
    EXPECT_EQ(listObj->listRemove(-10, "x"), 10);
}

TEST(ObjectTest, ListMixedTypeAndEncoding) {
    auto listObj = RedisObject::createSharedListObject();
    
    // 存入字符串和数字
    listObj->listRightPush(RedisObject::createSharedStringObject("hello"));
    listObj->listRightPush(RedisObject::createSharedLongObject(100));

    // 弹出验证：数字应该被正确重新对象化为 Int 编码
    auto res1 = listObj->listLeftPop(); // "hello"
    EXPECT_EQ(res1->asString(), "hello");

    auto res2 = listObj->listLeftPop(); // 100
    EXPECT_EQ(res2->getEncoding(), ObjectEncoding::Int);
    EXPECT_EQ(res2->asString(), "100");
}

TEST(ObjectTest, ListAutomaticUpgrade) {
    auto listObj = RedisObject::createSharedListObject();
    EXPECT_EQ(listObj->getEncoding(), ObjectEncoding::ZipList);

    // 连续 Push 超过阈值 (我们在头文件设定的 ZipListMaxEntries 是 16)
    for (int i = 0; i < 20; ++i) {
        listObj->listRightPush(RedisObject::createSharedLongObject(i));
    }

    // 验证编码是否已经自动升级为 LinkedList
    EXPECT_EQ(listObj->getEncoding(), ObjectEncoding::LinkedList);

    // 验证数据完整性：按照先进先出顺序弹出
    for (int i = 0; i < 20; ++i) {
        auto res = listObj->listLeftPop();
        ASSERT_NE(res, nullptr);
        EXPECT_EQ(res->asString(), std::to_string(i));
    }
    
    EXPECT_EQ(listObj->listLeftPop(), nullptr);
}

TEST(ObjectTest, ListLargeValueTriggersLinkedListUpgrade) {
    auto listObj = RedisObject::createSharedListObject();
    std::string large_value(RedisObject::ZipListMaxValue + 1, 'x');

    listObj->listRightPush(RedisObject::createSharedStringObject("small"));
    listObj->listRightPush(RedisObject::createSharedStringObject(large_value));

    EXPECT_EQ(listObj->getEncoding(), ObjectEncoding::LinkedList);

    auto first = listObj->listLeftPop();
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first->asString(), "small");

    auto second = listObj->listLeftPop();
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(second->asString(), large_value);

    EXPECT_EQ(listObj->listLeftPop(), nullptr);
}

TEST(ObjectTest, SetStartsAsIntSet) {
    auto setObj = RedisObject::createSharedSetObject();

    EXPECT_EQ(setObj->getType(), ObjectType::Set);
    EXPECT_EQ(setObj->getEncoding(), ObjectEncoding::IntSet);
    EXPECT_EQ(setObj->setSize(), 0);
}

TEST(ObjectTest, SetStoresIntegerMembersInIntSet) {
    auto setObj = RedisObject::createSharedSetObject();

    EXPECT_TRUE(setObj->setAdd("1"));
    EXPECT_EQ(setObj->setSize(), 1);
    EXPECT_TRUE(setObj->setAdd("-2"));
    EXPECT_EQ(setObj->setSize(), 2);
    EXPECT_FALSE(setObj->setAdd("1"));
    EXPECT_EQ(setObj->setSize(), 2);

    EXPECT_TRUE(setObj->setContains("1"));
    EXPECT_TRUE(setObj->setContains("-2"));
    EXPECT_FALSE(setObj->setContains("2"));
    EXPECT_FALSE(setObj->setContains("01"));

    EXPECT_EQ(setObj->getEncoding(), ObjectEncoding::IntSet);
}

TEST(ObjectTest, SetUpgradesToHashTableForStringMember) {
    auto setObj = RedisObject::createSharedSetObject();

    EXPECT_TRUE(setObj->setAdd("1"));
    EXPECT_TRUE(setObj->setAdd("hello"));

    EXPECT_EQ(setObj->getEncoding(), ObjectEncoding::HashTable);
    EXPECT_EQ(setObj->setSize(), 2);
    EXPECT_TRUE(setObj->setContains("1"));
    EXPECT_TRUE(setObj->setContains("hello"));
    EXPECT_FALSE(setObj->setContains("missing"));
    EXPECT_FALSE(setObj->setAdd("hello"));
}

TEST(ObjectTest, SetUpgradesToHashTableAfterIntSetThreshold) {
    auto setObj = RedisObject::createSharedSetObject();

    for (std::size_t i = 0; i < RedisObject::SetMaxIntSetEntries; ++i) {
        EXPECT_TRUE(setObj->setAdd(std::to_string(i)));
    }

    EXPECT_EQ(setObj->getEncoding(), ObjectEncoding::IntSet);
    EXPECT_TRUE(setObj->setAdd(std::to_string(RedisObject::SetMaxIntSetEntries)));
    EXPECT_EQ(setObj->getEncoding(), ObjectEncoding::HashTable);
    EXPECT_EQ(setObj->setSize(), RedisObject::SetMaxIntSetEntries + 1);
    EXPECT_TRUE(setObj->setContains("0"));
    EXPECT_TRUE(setObj->setContains(std::to_string(RedisObject::SetMaxIntSetEntries)));
}

TEST(ObjectTest, SetRemoveFromIntSet) {
    auto setObj = RedisObject::createSharedSetObject();

    EXPECT_TRUE(setObj->setAdd("1"));
    EXPECT_TRUE(setObj->setAdd("2"));
    EXPECT_TRUE(setObj->setAdd("-3"));
    EXPECT_EQ(setObj->setSize(), 3);

    EXPECT_TRUE(setObj->setRemove("2"));
    EXPECT_EQ(setObj->setSize(), 2);
    EXPECT_FALSE(setObj->setContains("2"));
    EXPECT_TRUE(setObj->setContains("1"));
    EXPECT_TRUE(setObj->setContains("-3"));

    EXPECT_FALSE(setObj->setRemove("2"));
    EXPECT_FALSE(setObj->setRemove("not-an-int"));
    EXPECT_EQ(setObj->setSize(), 2);
    EXPECT_EQ(setObj->getEncoding(), ObjectEncoding::IntSet);
}

TEST(ObjectTest, SetRemoveFromHashTableAfterUpgrade) {
    auto setObj = RedisObject::createSharedSetObject();

    EXPECT_TRUE(setObj->setAdd("1"));
    EXPECT_TRUE(setObj->setAdd("hello"));
    EXPECT_TRUE(setObj->setAdd("world"));
    EXPECT_EQ(setObj->getEncoding(), ObjectEncoding::HashTable);
    EXPECT_EQ(setObj->setSize(), 3);

    EXPECT_TRUE(setObj->setRemove("hello"));
    EXPECT_EQ(setObj->setSize(), 2);
    EXPECT_FALSE(setObj->setContains("hello"));
    EXPECT_TRUE(setObj->setContains("1"));
    EXPECT_TRUE(setObj->setContains("world"));

    EXPECT_FALSE(setObj->setRemove("missing"));
    EXPECT_EQ(setObj->setSize(), 2);
}

TEST(ObjectTest, SetForEachVisitsIntSetMembers) {
    auto setObj = RedisObject::createSharedSetObject();

    EXPECT_TRUE(setObj->setAdd("1"));
    EXPECT_TRUE(setObj->setAdd("-2"));
    EXPECT_TRUE(setObj->setAdd("3"));

    std::set<std::string> members;
    std::function<void(std::string_view)> collect = [&members](std::string_view member) {
        members.insert(std::string(member));
    };

    setObj->setForEach(collect);

    EXPECT_EQ(members, (std::set<std::string>{"-2", "1", "3"}));
}

TEST(ObjectTest, SetForEachVisitsHashTableMembersAfterUpgrade) {
    auto setObj = RedisObject::createSharedSetObject();

    EXPECT_TRUE(setObj->setAdd("1"));
    EXPECT_TRUE(setObj->setAdd("hello"));
    EXPECT_TRUE(setObj->setAdd("world"));
    EXPECT_EQ(setObj->getEncoding(), ObjectEncoding::HashTable);

    std::set<std::string> members;
    std::function<void(std::string_view)> collect = [&members](std::string_view member) {
        members.insert(std::string(member));
    };

    setObj->setForEach(collect);

    EXPECT_EQ(members, (std::set<std::string>{"1", "hello", "world"}));
}

TEST(ObjectTest, StringAdvancedCommands) {
    // 1. stringLen
    auto obj1 = RedisObject::createSharedStringObject("hello");
    EXPECT_EQ(obj1->stringLen(), 5);
    auto obj2 = RedisObject::createSharedLongObject(100);
    EXPECT_EQ(obj2->stringLen(), 3);

    // 2. stringIncrBy
    auto obj3 = RedisObject::createSharedStringObject("10");
    auto res3 = obj3->stringIncrBy(5);
    ASSERT_TRUE(res3.has_value());
    EXPECT_EQ(*res3, 15);
    EXPECT_EQ(obj3->getEncoding(), ObjectEncoding::Int);

    auto obj4 = RedisObject::createSharedStringObject("not-a-number");
    EXPECT_FALSE(obj4->stringIncrBy(5).has_value());

    // 3. stringIncrByFloat
    auto obj5 = RedisObject::createSharedStringObject("10.5");
    auto res5 = obj5->stringIncrByFloat(0.5);
    ASSERT_TRUE(res5.has_value());
    EXPECT_DOUBLE_EQ(*res5, 11.0);
    EXPECT_EQ(obj5->getEncoding(), ObjectEncoding::Raw);

    // 4. stringGetRange
    auto obj6 = RedisObject::createSharedStringObject("Hello World");
    EXPECT_EQ(obj6->stringGetRange(0, 4), "Hello");
    EXPECT_EQ(obj6->stringGetRange(-5, -1), "World");
    EXPECT_EQ(obj6->stringGetRange(0, -1), "Hello World");
    EXPECT_EQ(obj6->stringGetRange(100, 200), "");

    // 5. stringSetRange
    auto obj7 = RedisObject::createSharedStringObject("hello");
    obj7->stringSetRange(1, "i");
    EXPECT_EQ(obj7->asString(), "hillo");
    
    obj7->stringSetRange(7, "world");
    std::string expected = "hillo\0\0world";
    std::string actual = obj7->asString();
    EXPECT_EQ(actual.size(), 12);
    EXPECT_EQ(actual[5], '\0');
    EXPECT_EQ(actual[6], '\0');
    EXPECT_EQ(actual.substr(7), "world");
}


TEST(ObjectTest, ListTrim) {
    auto listObj = RedisObject::createSharedListObject();
    // 初始: ["0", "1", "2", "3", "4"]
    for (int i = 0; i < 5; ++i) {
        listObj->listRightPush(RedisObject::createSharedStringObject(std::to_string(i)));
    }

    // 1. 正常裁剪中间部分: LTRIM 1 3 -> ["1", "2", "3"]
    listObj->listTrim(1, 3);
    EXPECT_EQ(listObj->listLen(), 3);
    EXPECT_EQ(listObj->listIndex(0)->asString(), "1");
    EXPECT_EQ(listObj->listIndex(2)->asString(), "3");

    // 2. 使用负数索引: LTRIM 0 -1 (保持不变)
    listObj->listTrim(0, -1);
    EXPECT_EQ(listObj->listLen(), 3);
    EXPECT_EQ(listObj->listIndex(0)->asString(), "1");
    EXPECT_EQ(listObj->listIndex(2)->asString(), "3");

    // 3. 越界裁剪到空: LTRIM 5 2
    listObj->listTrim(5, 2);
    EXPECT_EQ(listObj->listLen(), 0);
    EXPECT_EQ(listObj->getEncoding(), ObjectEncoding::ZipList); // 验证是否重置了编码

    // 4. LinkedList 编码下的裁剪
    for (int i = 0; i < 20; ++i) {
        listObj->listRightPush(RedisObject::createSharedLongObject(i));
    }
    EXPECT_EQ(listObj->getEncoding(), ObjectEncoding::LinkedList);
    
    listObj->listTrim(0, 9); // 保留前 10 个 (0-9)
    EXPECT_EQ(listObj->listLen(), 10);
    EXPECT_EQ(listObj->listIndex(0)->asString(), "0");
    EXPECT_EQ(listObj->listIndex(9)->asString(), "9");
}

TEST(ObjectTest, SetRandomMember) {
    // 1. Test IntSet
    auto setObj = RedisObject::createSharedSetObject();
    for (int i = 0; i < 5; ++i) {
        setObj->setAdd(std::to_string(i));
    }
    EXPECT_EQ(setObj->getEncoding(), ObjectEncoding::IntSet);
    
    auto rand = setObj->setRandomMember();
    ASSERT_NE(rand, nullptr);
    EXPECT_TRUE(setObj->setContains(rand->asString()));
    EXPECT_EQ(setObj->setSize(), 5); // Size should not change

    // 2. Test HashTable
    setObj->setAdd("hello"); // Trigger upgrade
    EXPECT_EQ(setObj->getEncoding(), ObjectEncoding::HashTable);
    
    rand = setObj->setRandomMember();
    ASSERT_NE(rand, nullptr);
    EXPECT_TRUE(setObj->setContains(rand->asString()));
    EXPECT_EQ(setObj->setSize(), 6);
}

TEST(ObjectTest, SetPop) {
    // 1. Test IntSet
    auto setObj = RedisObject::createSharedSetObject();
    std::set<std::string> members = {"1", "2", "3"};
    for (const auto& m : members) {
        setObj->setAdd(m);
    }
    
    auto popped = setObj->setPop();
    ASSERT_NE(popped, nullptr);
    EXPECT_TRUE(members.count(popped->asString()));
    EXPECT_FALSE(setObj->setContains(popped->asString()));
    EXPECT_EQ(setObj->setSize(), 2);

    // 2. Test HashTable
    setObj->setAdd("extra");
    setObj->setAdd("world"); // Force more members
    setObj->setAdd("trigger_upgrade"); // Just in case, though it might already be HashTable if mixed
    
    // Ensure it's HashTable
    if (setObj->getEncoding() == ObjectEncoding::IntSet) {
        setObj->setAdd("force_hash");
    }
    
    size_t initial_size = setObj->setSize();
    popped = setObj->setPop();
    ASSERT_NE(popped, nullptr);
    EXPECT_FALSE(setObj->setContains(popped->asString()));
    EXPECT_EQ(setObj->setSize(), initial_size - 1);
}

TEST(ObjectTest, ZSetAdvancedOps) {
    auto zsetObj = RedisObject::createSharedZSetObject();
    
    // Initial data: a:10, b:20, c:30
    zsetObj->zSetAdd("a", 10.0);
    zsetObj->zSetAdd("b", 20.0);
    zsetObj->zSetAdd("c", 30.0);
    
    // 1. ZREVRANK
    EXPECT_EQ(zsetObj->zSetRank("a"), 0);
    EXPECT_EQ(zsetObj->zSetRevRank("a"), 2);
    EXPECT_EQ(zsetObj->zSetRevRank("c"), 0);
    EXPECT_FALSE(zsetObj->zSetRevRank("nonexistent").has_value());

    // 2. ZCOUNT
    EXPECT_EQ(zsetObj->zSetCount(15.0, 35.0), 2); // b, c
    EXPECT_EQ(zsetObj->zSetCount(5.0, 40.0), 3);
    EXPECT_EQ(zsetObj->zSetCount(35.0, 40.0), 0);

    // 3. ZRANGE
    auto range = zsetObj->zSetRange(0, 1); // a, b
    ASSERT_EQ(range.size(), 2);
    EXPECT_EQ(range[0].first, "a");
    EXPECT_DOUBLE_EQ(range[0].second, 10.0);
    EXPECT_EQ(range[1].first, "b");
    EXPECT_DOUBLE_EQ(range[1].second, 20.0);

    // 4. ZREVRANGE
    auto revRange = zsetObj->zSetRevRange(0, 1); // c, b
    ASSERT_EQ(revRange.size(), 2);
    EXPECT_EQ(revRange[0].first, "c");
    EXPECT_DOUBLE_EQ(revRange[0].second, 30.0);
    EXPECT_EQ(revRange[1].first, "b");
    EXPECT_DOUBLE_EQ(revRange[1].second, 20.0);
}

TEST(ObjectTest, ZSetSkipListUpgradeAndOps) {
    auto zsetObj = RedisObject::createSharedZSetObject();
    // ZSet threshold is ZSetZipListMaxEntries=16
    for (int i = 0; i < 20; ++i) {
        zsetObj->zSetAdd("m" + std::to_string(i), i * 10.0);
    }
    EXPECT_EQ(zsetObj->getEncoding(), ObjectEncoding::SkipList);

    // Test ZCOUNT on SkipList
    EXPECT_EQ(zsetObj->zSetCount(50.0, 150.0), 11); // 50, 60, ..., 150 (m5 to m15)

    // Test ZREVRANK on SkipList
    EXPECT_EQ(zsetObj->zSetRevRank("m19"), 0);
    EXPECT_EQ(zsetObj->zSetRevRank("m0"), 19);

    // Test ZREVRANGE on SkipList
    auto revRange = zsetObj->zSetRevRange(0, 2); // m19, m18, m17
    ASSERT_EQ(revRange.size(), 3);
    EXPECT_EQ(revRange[0].first, "m19");
    EXPECT_EQ(revRange[1].first, "m18");
    EXPECT_EQ(revRange[2].first, "m17");
}
