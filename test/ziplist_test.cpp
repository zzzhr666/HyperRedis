#include "hyper/datastructures/ziplist.hpp"

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

using namespace hyper;

namespace {
    // 升级版 snapshot：兼容整数和字符串，统统转化为 std::string 返回，方便对比测试
    std::vector<std::string> snapshot(const ziplist& values) {
        std::vector<std::string> result;
        values.forEach([&result](ziplistEntryView value) {
            if (value.isString()) {
                result.emplace_back(value.string());
            } else if (value.isInteger()) {
                result.emplace_back(std::to_string(value.integer()));
            } else {
                result.emplace_back("UNKNOWN");
            }
        });
        return result;
    }

    // 辅助函数：专门用于断言该位置存的是字符串，且值等于预期
    void expectStringAt(const ziplist& values, std::size_t index, std::string_view expected) {
        auto value = values.at(index);
        ASSERT_TRUE(value.has_value());
        EXPECT_TRUE(value->isString());
        EXPECT_FALSE(value->isInteger());
        EXPECT_EQ(value->string(), expected);
    }

    // 辅助函数：专门用于断言该位置存的是整数，且值等于预期
    void expectIntegerAt(const ziplist& values, std::size_t index, std::int64_t expected) {
        auto value = values.at(index);
        ASSERT_TRUE(value.has_value());
        EXPECT_TRUE(value->isInteger());
        EXPECT_FALSE(value->isString());
        EXPECT_EQ(value->integer(), expected);
    }
} // namespace

// ============================================================================
// ========================= 基础功能测试 =======================================
// ============================================================================

TEST(ZiplistEntryViewTest, StringViewReportsStringType) {
    ziplistEntryView value = ziplistEntryView::fromString("alpha");
    EXPECT_TRUE(value.isString());
    EXPECT_FALSE(value.isInteger());
    EXPECT_EQ(value.type(), ziplistEntryView::Type::String);
    EXPECT_EQ(value.string(), "alpha");
}

TEST(ZiplistEntryViewTest, IntegerViewReportsIntegerType) {
    ziplistEntryView value = ziplistEntryView::fromInteger(42);
    EXPECT_TRUE(value.isInteger());
    EXPECT_FALSE(value.isString());
    EXPECT_EQ(value.type(), ziplistEntryView::Type::Integer);
    EXPECT_EQ(value.integer(), 42);
}

TEST(ZiplistTest, StartsEmpty) {
    spdlog::info("StartsEmpty: create a new ziplist and verify empty state");
    ziplist values;
    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);
    // 空链表的大小：4(tail) + 2(len) + 1(end marker) = 7
    EXPECT_EQ(values.byteSize(), 7U);
}

TEST(ZiplistTest, ClearKeepsEmptyState) {
    spdlog::info("ClearKeepsEmptyState: clear an already empty ziplist");
    ziplist values;
    const auto empty_byte_size = values.byteSize();
    values.pushBack("test");
    values.clear();
    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);
    EXPECT_EQ(values.byteSize(), empty_byte_size);
}

TEST(ZiplistTest, PushBackAddsSingleString) {
    ziplist values;
    values.pushBack("hello");
    EXPECT_EQ(values.size(), 1U);
    EXPECT_EQ(snapshot(values), (std::vector<std::string>{"hello"}));
}

TEST(ZiplistTest, PushBackPreservesInsertionOrder) {
    ziplist values;
    values.pushBack("alpha");
    values.pushBack("beta");
    values.pushBack("gamma");
    EXPECT_EQ(values.size(), 3U);
    EXPECT_EQ(snapshot(values), (std::vector<std::string>{"alpha", "beta", "gamma"}));
}

TEST(ZiplistTest, PopFrontRemovesElementsInFrontOrder) {
    ziplist values;
    values.pushBack("alpha");
    values.pushBack("beta");

    EXPECT_EQ(values.popFront(), std::optional<std::string>{"alpha"});
    EXPECT_EQ(values.size(), 1U);
    EXPECT_EQ(snapshot(values), (std::vector<std::string>{"beta"}));

    EXPECT_EQ(values.popFront(), std::optional<std::string>{"beta"});
    EXPECT_TRUE(values.empty());
}

TEST(ZiplistTest, EraseRemovesMiddleElement) {
    ziplist values;
    values.pushBack("alpha");
    values.pushBack("beta");
    values.pushBack("gamma");

    EXPECT_TRUE(values.erase(1));
    EXPECT_EQ(snapshot(values), (std::vector<std::string>{"alpha", "gamma"}));
    expectStringAt(values, 1, "gamma");
}

// ============================================================================
// ========================= 整数编码黑魔法测试 =================================
// ============================================================================

TEST(ZiplistIntegerTest, DetectsAndStoresIntegers) {
    spdlog::info("DetectsAndStoresIntegers: automatically converts numeric strings to integers");
    ziplist values;

    values.pushBack("100"); // 应该被转为整数
    values.pushBack("alpha"); // 保持字符串
    values.pushBack("-500"); // 应该被转为整数

    EXPECT_EQ(values.size(), 3U);

    // 验证底层类型
    expectIntegerAt(values, 0, 100);
    expectStringAt(values, 1, "alpha");
    expectIntegerAt(values, 2, -500);

    // 验证 snapshot 功能（能把整数还原为字符串用于对比）
    EXPECT_EQ(snapshot(values), (std::vector<std::string>{"100", "alpha", "-500"}));
}

TEST(ZiplistIntegerTest, DoesNotConvertInvalidIntegers) {
    spdlog::info("DoesNotConvertInvalidIntegers: correctly keeps malformed numbers as strings");
    ziplist values;

    values.pushBack("0123"); // 前导0，必须是字符串
    values.pushBack("-0"); // 无效的负零，必须是字符串
    values.pushBack("123a"); // 包含字母，必须是字符串
    values.pushBack(""); // 空串，必须是字符串

    EXPECT_EQ(values.size(), 4U);
    expectStringAt(values, 0, "0123");
    expectStringAt(values, 1, "-0");
    expectStringAt(values, 2, "123a");
    expectStringAt(values, 3, "");
}

TEST(ZiplistIntegerTest, IntegerEncodingBoundaries) {
    spdlog::info("IntegerEncodingBoundaries: test all integer size thresholds");
    ziplist values;

    // 1. 极小整数 (0-12) -> 存 0 和 12
    values.pushBack("0");
    values.pushBack("12");

    // 2. 1字节有符号 (-128 ~ 127) -> 存 13 和 -128
    values.pushBack("13");
    values.pushBack("-128");

    // 3. 2字节有符号 (-32768 ~ 32767) -> 存 32767 和 -32768
    values.pushBack("32767");
    values.pushBack("-32768");

    // 4. 24-bit 有符号 (-8388608 ~ 8388607) -> 存 8388607 和 -8388608
    values.pushBack("8388607");
    values.pushBack("-8388608");

    // 5. 4字节有符号 (int32 max/min)
    values.pushBack("2147483647");
    values.pushBack("-2147483648");

    // 6. 8字节有符号 (int64)
    values.pushBack("9223372036854775807");
    values.pushBack("-9223372036854775808");

    EXPECT_EQ(values.size(), 12U);

    // 挨个校验它们被正确还原成整数
    expectIntegerAt(values, 0, 0);
    expectIntegerAt(values, 1, 12);
    expectIntegerAt(values, 2, 13);
    expectIntegerAt(values, 3, -128);
    expectIntegerAt(values, 4, 32767);
    expectIntegerAt(values, 5, -32768);
    expectIntegerAt(values, 6, 8388607);
    expectIntegerAt(values, 7, -8388608);
    expectIntegerAt(values, 8, 2147483647LL);
    expectIntegerAt(values, 9, -2147483648LL);
    expectIntegerAt(values, 10, 9223372036854775807LL);
    // 注意 C++ 编译器解析负极限常量的坑，需要这么写
    expectIntegerAt(values, 11, -9223372036854775807LL - 1);
}

TEST(ZiplistIntegerTest, IntegerSavesMemory) {
    spdlog::info("IntegerSavesMemory: verify integers use less memory than their string equivalents");

    ziplist string_list;
    // 强制作为字符串存入
    string_list.pushBack("012345678"); // 9个字符

    ziplist int_list;
    // 自动作为整数存入
    int_list.pushBack("12345678"); // 4字节整数

    // 字符串需要的载荷: 1(prev) + 1(enc) + 9(payload) = 11 bytes
    // 整数需要的载荷: 1(prev) + 1(enc) + 4(payload) = 6 bytes
    EXPECT_LT(int_list.byteSize(), string_list.byteSize());
}

// ============================================================================
// ========================= 高阶变态压测区域 ===================================
// ============================================================================

TEST(ZiplistAdvancedTest, CascadeUpdateTriggeredByErase) {
    spdlog::info(
        "Advanced: Erasing a small node brings a HUGE node adjacent to another small node, triggering cascade expansion");
    ziplist values;

    std::string huge_string(300, 'H');
    std::string edge_string(250, 'E');

    values.pushBack(huge_string);
    values.pushBack("small");
    values.pushBack(edge_string);
    values.pushBack(edge_string);
    values.pushBack("tail");

    values.erase(1);

    EXPECT_EQ(values.size(), 4U);
    EXPECT_EQ(snapshot(values), (std::vector<std::string>{huge_string, edge_string, edge_string, "tail"}));

    EXPECT_EQ(values.popBack(), std::optional<std::string>{"tail"});
    EXPECT_EQ(values.popBack(), std::optional<std::string>{edge_string});
    EXPECT_EQ(values.popBack(), std::optional<std::string>{edge_string});
    EXPECT_EQ(values.popBack(), std::optional<std::string>{huge_string});
}

TEST(ZiplistAdvancedTest, StressFuzzingCombinations) {
    spdlog::info("Advanced: Stress test with mixed push, pop, insert, and erase operations");
    ziplist values;

    std::vector<std::string> reference_list;

    for (int i = 0; i < 50; ++i) {
        int op = (i * 17) % 6;
        // 混合插入整数和字符串
        std::string payload = (i % 2 == 0) ? std::to_string(i * 1000) : "data_" + std::to_string(i);
        if (i % 7 == 0) payload = std::string(300, 'Z');

        switch (op) {
        case 0:
        case 1:
            values.pushBack(payload);
            reference_list.push_back(payload);
            break;
        case 2:
            values.pushFront(payload);
            reference_list.insert(reference_list.begin(), payload);
            break;
        case 3:
            if (!reference_list.empty()) {
                size_t idx = reference_list.size() / 2;
                values.insert(idx, payload);
                reference_list.insert(reference_list.begin() + idx, payload);
            }
            break;
        case 4:
            if (!reference_list.empty()) {
                values.popFront();
                reference_list.erase(reference_list.begin());
            }
            break;
        case 5:
            if (reference_list.size() > 2) {
                size_t idx = reference_list.size() / 2;
                values.erase(idx);
                reference_list.erase(reference_list.begin() + idx);
            }
            break;
        }
    }

    EXPECT_EQ(values.size(), reference_list.size());
    EXPECT_EQ(snapshot(values), reference_list);

    while (!values.empty()) {
        values.popBack();
    }

    // 验证 Ziplist 退回到了初始状态
    EXPECT_EQ(values.byteSize(), 7U);
}

TEST(ZiplistAdvancedTest, MassiveCascadeUpdate) {
    spdlog::info("Testing deep cascade update with 50 nodes...");
    ziplist values;
    // 构造 50 个长度为 250 的节点
    // 初始状态下，每个节点的 prev_len 只需要 1 字节来记录前一个节点的大小
    std::string edge_string(250, 'A');
    for (int i = 0; i < 50; ++i) {
        values.pushBack(edge_string);
    }

    // 此时插入一个巨大的头部节点 (例如 500 字节)
    // 这会导致第 1 个节点的 prev_len 必须从 1 字节变为 5 字节
    // 进而导致第 1 个节点的总长度增加，触发第 2 个节点的 prev_len 变化...以此类推
    std::string giant_boss(500, 'G');
    values.pushFront(giant_boss);

    EXPECT_EQ(values.size(), 51U);
    auto head = values.at(0);
    ASSERT_TRUE(head.has_value());
    EXPECT_EQ(head->string().size(), 500);

    // 验证最后一个节点是否依然完好
    auto tail = values.at(50);
    ASSERT_TRUE(tail.has_value());
    EXPECT_EQ(tail->string(), edge_string);
}

TEST(ZiplistAdvancedTest, TypeSwitchingStress) {
    spdlog::info("Testing frequent type switching at the same index...");
    ziplist values;
    values.pushBack("initial");

    for (int i = 0; i < 100; ++i) {
        values.erase(0);
        if (i % 2 == 0) {
            values.pushFront(std::to_string(i * 1234567LL)); // 插入整数
        } else {
            values.pushFront("string_data_" + std::to_string(i)); // 插入字符串
        }
    }

    EXPECT_EQ(values.size(), 1U);
    // 检查最后的元素是否符合最后一次迭代 (i=99, 奇数, 应该是字符串)
    expectStringAt(values, 0, "string_data_99");
}

TEST(ZiplistIntegerTest, Int24SpecificBoundaries) {
    spdlog::info("Testing 24-bit integer specific edge cases...");
    ziplist values;

    // 24位有符号数范围: [-8388608, 8388607]
    std::vector<int64_t> test_cases = {
        8388607,
        // Max
        -8388608,
        // Min
        8388600,
        -8388600,
        1,
        -1
    };

    for (auto val : test_cases) {
        values.pushBack(std::to_string(val));
    }

    for (size_t i = 0; i < test_cases.size(); ++i) {
        expectIntegerAt(values, i, test_cases[i]);
    }
}

TEST(ZiplistAdvancedTest, TailPointerConsistency) {
    spdlog::info("Testing tail pointer consistency after various erasures...");
    ziplist values;

    values.pushBack("node1");
    values.pushBack("node2");
    values.pushBack(std::string(1000, 'B')); // 大节点
    values.pushBack("node4");

    // 1. 删除中间的大节点，观察 tail 是否正确指向 node4
    values.erase(2);
    expectStringAt(values, values.size() - 1, "node4");

    // 2. 连续弹出尾部
    values.popBack();
    expectStringAt(values, values.size() - 1, "node2");

    // 3. 再次 push，看是否会写坏内存
    values.pushBack("new_tail");
    expectStringAt(values, 2, "new_tail");

    EXPECT_EQ(values.size(), 3U);
}

TEST(ZiplistAdvancedTest, ZeroAndEmptyValues) {
    spdlog::info("Testing empty strings and zero integers...");
    ziplist values;

    values.pushBack("");      // 空字符串，编码应占用 1 字节长度
    values.pushBack("0");     // 整数 0，属于 Small Integer (1111xxxx)
    values.pushBack("");      // 再次空串

    EXPECT_EQ(values.size(), 3U);
    expectStringAt(values, 0, "");
    expectIntegerAt(values, 1, 0);
    expectStringAt(values, 2, "");

    values.erase(1); // 删掉中间的 0
    EXPECT_EQ(values.size(), 2U);
    expectStringAt(values, 0, "");
    expectStringAt(values, 1, "");
}

TEST(ZiplistAdvancedTest, MassiveStringEntry) {
    spdlog::info("Testing entry jumping from small to > 64KB...");
    ziplist values;

    values.pushBack("small");
    // 构造一个 70,000 字节的字符串，超过了 16 位整数范围
    std::string big_boy(70000, 'X');
    values.pushBack(big_boy);
    values.pushBack("tail");

    EXPECT_EQ(values.size(), 3U);
    expectStringAt(values, 1, big_boy);

    // 关键点：从后面往前找，看 tail 指针是否跳过了这 70KB
    EXPECT_EQ(values.popBack(), std::optional<std::string>{"tail"});
}

TEST(ZiplistAdvancedTest, DeleteLeadingToPotentialShrink) {
    spdlog::info("Testing delete that would normally trigger shrink...");
    ziplist values;

    // 1. 创造一个大节点
    std::string big(300, 'B');
    values.pushBack(big);

    // 2. 后面跟一个节点，它的 prev_len 现在是 5 字节
    values.pushBack("I_have_5_byte_prevlen");

    // 3. 删掉前面的大节点，现在第二个节点的 prev_len 其实可以缩回 1 字节
    // 你的代码通过 force_flag 保持了它的长度，测试它是否依然能正确读写
    values.erase(0);

    EXPECT_EQ(values.size(), 1U);
    expectStringAt(values, 0, "I_have_5_byte_prevlen");
}

TEST(ZiplistIntegerTest, Int24SignExtension) {
    ziplist values;
    // 测试一些在 24 位下是负数，但在 32 位下需要正确补 1 的值
    // -1 在 24 位补码是 0xFFFFFF
    values.pushBack("-1");
    // -8388608 是 24 位最小负数，补码 0x800000
    values.pushBack("-8388608");

    expectIntegerAt(values, 0, -1);
    expectIntegerAt(values, 1, -8388608);
}