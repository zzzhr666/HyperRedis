#include <gtest/gtest.h>
#include <hyper/storage/object.hpp>

using namespace hyper;

TEST(ObjectTest, StringToLongEncoding) {
    // 正常整数
    auto obj1 = RedisObject::createStringObject("123");
    EXPECT_EQ(obj1->getType(), ObjectType::String);
    EXPECT_EQ(obj1->getEncoding(), ObjectEncoding::Int);

    // 负数
    auto obj2 = RedisObject::createStringObject("-456");
    EXPECT_EQ(obj2->getEncoding(), ObjectEncoding::Int);
}

TEST(ObjectTest, StringNormalization) {
    // 前导零：不应转为整数
    auto obj1 = RedisObject::createStringObject("0123");
    EXPECT_EQ(obj1->getEncoding(), ObjectEncoding::Raw);

    // 负号加前导零：不应转为整数
    auto obj2 = RedisObject::createStringObject("-01");
    EXPECT_EQ(obj2->getEncoding(), ObjectEncoding::Raw);

    // 只有 "0"：应该转为整数
    auto obj3 = RedisObject::createStringObject("0");
    EXPECT_EQ(obj3->getEncoding(), ObjectEncoding::Int);
}

TEST(ObjectTest, RawString) {
    // 非数字
    auto obj1 = RedisObject::createStringObject("hello");
    EXPECT_EQ(obj1->getEncoding(), ObjectEncoding::Raw);

    // 包含空格的数字
    auto obj2 = RedisObject::createStringObject(" 123 ");
    EXPECT_EQ(obj2->getEncoding(), ObjectEncoding::Raw);
}

TEST(ObjectTest, LongObject) {
    auto obj = RedisObject::createLongObject(100);
    EXPECT_EQ(obj->getEncoding(), ObjectEncoding::Int);
}

TEST(ObjectTest, Accessors) {
    // 测试 Int 编码的访问
    auto obj1 = RedisObject::createStringObject("123");
    EXPECT_EQ(obj1->getEncoding(), ObjectEncoding::Int);
    EXPECT_EQ(obj1->asString(), "123");

    // 测试 Raw 编码的访问
    auto obj2 = RedisObject::createStringObject("hello");
    EXPECT_EQ(obj2->getEncoding(), ObjectEncoding::Raw);
    EXPECT_EQ(obj2->asString(), "hello");
}

TEST(ObjectTest, AppendLogic) {
    // 测试从 Int 转换到 Raw 的追加
    auto obj = RedisObject::createStringObject("100");
    EXPECT_EQ(obj->getEncoding(), ObjectEncoding::Int);
    
    obj->append("50"); 
    EXPECT_EQ(obj->getEncoding(), ObjectEncoding::Raw);
    EXPECT_EQ(obj->asString(), "10050");

    // 测试在 Raw 上的持续追加
    obj->append("!");
    EXPECT_EQ(obj->asString(), "10050!");
}

TEST(ObjectTest, HashRecursiveStorage) {
    // 1. 创建一个 Hash 对象
    auto hashObj = RedisObject::createHashObject();
    
    // 2. 创建两个 String 对象作为 Value
    // 注意：hashSet 接收的是 shared_ptr<RedisObject>
    auto val1 = std::shared_ptr<RedisObject>(RedisObject::createStringObject("val1").release());
    auto val2 = std::shared_ptr<RedisObject>(RedisObject::createLongObject(100).release()); 
    
    // 3. 存入 Hash
    hashObj->hashSet("field1", val1);
    hashObj->hashSet("field2", val2);
    
    // 4. 读取并验证
    auto res1 = hashObj->hashGet("field1");
    ASSERT_NE(res1, nullptr);
    EXPECT_EQ(res1->asString(), "val1");
    
    auto res2 = hashObj->hashGet("field2");
    ASSERT_NE(res2, nullptr);
    EXPECT_EQ(res2->getEncoding(), ObjectEncoding::Int);
    EXPECT_EQ(res2->asString(), "100");
}

TEST(ObjectTest, ListBasicOperations) {
    auto listObj = RedisObject::createListObject();
    EXPECT_EQ(listObj->getEncoding(), ObjectEncoding::ZipList);

    // 测试 LPUSH 和 RPOP
    listObj->listLeftPush(RedisObject::createStringObject("first"));
    listObj->listLeftPush(RedisObject::createStringObject("second"));
    
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

TEST(ObjectTest, ListMixedTypeAndEncoding) {
    auto listObj = RedisObject::createListObject();
    
    // 存入字符串和数字
    listObj->listRightPush(RedisObject::createStringObject("hello"));
    listObj->listRightPush(RedisObject::createLongObject(100));

    // 弹出验证：数字应该被正确重新对象化为 Int 编码
    auto res1 = listObj->listLeftPop(); // "hello"
    EXPECT_EQ(res1->asString(), "hello");

    auto res2 = listObj->listLeftPop(); // 100
    EXPECT_EQ(res2->getEncoding(), ObjectEncoding::Int);
    EXPECT_EQ(res2->asString(), "100");
}

TEST(ObjectTest, ListAutomaticUpgrade) {
    auto listObj = RedisObject::createListObject();
    EXPECT_EQ(listObj->getEncoding(), ObjectEncoding::ZipList);

    // 连续 Push 超过阈值 (我们在头文件设定的 ZipListMaxEntries 是 16)
    for (int i = 0; i < 20; ++i) {
        listObj->listRightPush(RedisObject::createLongObject(i));
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
