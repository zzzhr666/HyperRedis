#include "hyper/datastructures/linked_list.hpp"

#include <gtest/gtest.h>
#include <spdlog/fmt/ranges.h>
#include <spdlog/spdlog.h>

#include <string>
#include <type_traits>
#include <utility>
#include <vector>
using namespace hyper;
template<typename T>
std::vector<T> snapshot(const list<T>& values) {
    std::vector<T> result;
    values.for_each([&result](const T& value) {
        result.push_back(value);
    });
    return result;
}

template<typename T>
void log_snapshot(const char* label, const list<T>& values) {
    const auto items = snapshot(values);
    spdlog::info("{} => [{}]", label, fmt::join(items, ", "));
}

TEST(LinkedListTest, PushAndPopMaintainExpectedOrder) {
    list<int> values;
    spdlog::info("starting push/pop test");

    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);

    values.push_back(2);
    values.push_front(1);
    values.push_back(3);
    log_snapshot("after push_front/push_back", values);

    EXPECT_FALSE(values.empty());
    EXPECT_EQ(values.size(), 3U);
    EXPECT_EQ(values.front(), 1);
    EXPECT_EQ(values.back(), 3);
    EXPECT_EQ(snapshot(values), (std::vector<int>{1, 2, 3}));

    values.pop_front();
    log_snapshot("after pop_front", values);
    EXPECT_EQ(values.size(), 2U);
    EXPECT_EQ(values.front(), 2);
    EXPECT_EQ(values.back(), 3);

    values.pop_back();
    log_snapshot("after pop_back", values);
    EXPECT_EQ(values.size(), 1U);
    EXPECT_EQ(values.front(), 2);
    EXPECT_EQ(values.back(), 2);

    values.pop_back();
    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);

    values.pop_front();
    values.pop_back();
    EXPECT_TRUE(values.empty());
}

TEST(LinkedListTest, FindAndEraseRemoveExpectedNodes) {
    list<int> values;
    values.push_back(10);
    values.push_back(20);
    values.push_back(20);
    values.push_back(30);
    log_snapshot("initial erase test values", values);

    EXPECT_TRUE(values.contains(20));
    EXPECT_FALSE(values.contains(40));

    auto* node = values.find(20);
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(node->data, 20);

    values.erase(node);
    log_snapshot("after erase(node)", values);
    EXPECT_EQ(snapshot(values), (std::vector<int>{10, 20, 30}));

    EXPECT_TRUE(values.erase(20));
    log_snapshot("after erase(value)", values);
    EXPECT_EQ(snapshot(values), (std::vector<int>{10, 30}));

    EXPECT_FALSE(values.erase(99));
    EXPECT_EQ(snapshot(values), (std::vector<int>{10, 30}));
}

TEST(LinkedListTest, ForEachAndIteratorTraverseElements) {
    list<std::string> values;
    values.push_back("aa");
    values.push_back("bbb");
    values.push_back("cccc");
    log_snapshot("initial iterator test values", values);

    values.for_each([](std::string& value) {
        value.push_back('!');
    });
    log_snapshot("after mutable for_each", values);

    const auto& const_values = values;
    std::vector<std::string> visited;
    const_values.for_each([&visited](const std::string& value) {
        visited.push_back(value);
    });
    EXPECT_EQ(visited, (std::vector<std::string>{"aa!", "bbb!", "cccc!"}));

    auto it = values.begin();
    ASSERT_NE(it, values.end());
    EXPECT_EQ(*it, "aa!");
    EXPECT_EQ(it->size(), 3U);

    ++it;
    ASSERT_NE(it, values.end());
    EXPECT_EQ(*it, "bbb!");

    it++;
    ASSERT_NE(it, values.end());
    EXPECT_EQ(*it, "cccc!");

    ++it;
    EXPECT_EQ(it, values.end());
}

TEST(LinkedListTest, MoveOperationsTransferOwnership) {
    static_assert(!std::is_copy_constructible_v<list<int>>);
    static_assert(!std::is_copy_assignable_v<list<int>>);
    static_assert(std::is_move_constructible_v<list<int>>);
    static_assert(std::is_move_assignable_v<list<int>>);

    list<int> source;
    source.push_back(1);
    source.push_back(2);
    source.push_back(3);
    log_snapshot("move source before move", source);

    list<int> moved_to(std::move(source));
    EXPECT_TRUE(source.empty());
    EXPECT_EQ(snapshot(moved_to), (std::vector<int>{1, 2, 3}));
    log_snapshot("after move construction", moved_to);

    list<int> assigned;
    assigned.push_back(99);
    assigned = std::move(moved_to);
    EXPECT_TRUE(moved_to.empty());
    EXPECT_EQ(snapshot(assigned), (std::vector<int>{1, 2, 3}));
    log_snapshot("after move assignment", assigned);
}

int main(int argc, char** argv) {
    spdlog::set_level(spdlog::level::info);
    spdlog::set_pattern("[%H:%M:%S] [%^%l%$] %v");
    spdlog::info("starting GoogleTest suite for linked_list");

    ::testing::InitGoogleTest(&argc, argv);
    const int result = RUN_ALL_TESTS();

    spdlog::info("GoogleTest suite finished with code {}", result);
    return result;
}
