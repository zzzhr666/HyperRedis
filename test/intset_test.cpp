#include "hyper/datastructures/intset.hpp"

#include <gtest/gtest.h>
#include <spdlog/fmt/ranges.h>
#include <spdlog/spdlog.h>

#include <cstdint>
#include <vector>

using namespace hyper;

namespace {

std::vector<std::int64_t> snapshot(const intset& values) {
    std::vector<std::int64_t> result;
    values.forEach([&result](std::int64_t value) {
        result.push_back(value);
    });
    return result;
}

void log_snapshot(const char* label, const intset& values) {
    const auto items = snapshot(values);
    spdlog::info("{} => [{}]", label, fmt::join(items, ", "));
}

} // namespace

TEST(IntsetTest, StartsEmpty) {
    spdlog::info("StartsEmpty: create a new intset and verify empty state");
    intset values;

    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);
    EXPECT_EQ(values.byteSize(), 0U);
    EXPECT_EQ(values.encoding(), intset::Encoding::Int16);
    EXPECT_EQ(snapshot(values), (std::vector<std::int64_t>{}));
}

TEST(IntsetTest, ClearResetsToInitialState) {
    spdlog::info("ClearResetsToInitialState: clear populated intset");
    intset values;

    ASSERT_TRUE(values.insert(10));
    ASSERT_TRUE(values.insert(20));

    values.clear();

    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);
    EXPECT_EQ(values.byteSize(), 0U);
    EXPECT_EQ(values.encoding(), intset::Encoding::Int16);
    EXPECT_FALSE(values.contains(10));
    EXPECT_EQ(snapshot(values), (std::vector<std::int64_t>{}));
}

TEST(IntsetTest, InsertMakesValueVisible) {
    spdlog::info("InsertMakesValueVisible: insert a single Int16 value");
    intset values;

    EXPECT_TRUE(values.insert(10));

    EXPECT_FALSE(values.empty());
    EXPECT_EQ(values.size(), 1U);
    EXPECT_EQ(values.byteSize(), sizeof(std::int16_t));
    EXPECT_TRUE(values.contains(10));
    EXPECT_FALSE(values.contains(20));
    EXPECT_EQ(values.encoding(), intset::Encoding::Int16);
    EXPECT_EQ(snapshot(values), (std::vector<std::int64_t>{10}));
}

TEST(IntsetTest, InsertRejectsDuplicateValue) {
    spdlog::info("InsertRejectsDuplicateValue: duplicate insert should be rejected");
    intset values;

    EXPECT_TRUE(values.insert(10));
    EXPECT_FALSE(values.insert(10));

    EXPECT_EQ(values.size(), 1U);
    EXPECT_TRUE(values.contains(10));
    EXPECT_EQ(snapshot(values), (std::vector<std::int64_t>{10}));
}

TEST(IntsetTest, InsertKeepsValuesSorted) {
    spdlog::info("InsertKeepsValuesSorted: insert Int16 values out of order");
    intset values;

    EXPECT_TRUE(values.insert(30));
    EXPECT_TRUE(values.insert(10));
    EXPECT_TRUE(values.insert(20));
    EXPECT_TRUE(values.insert(-5));
    log_snapshot("sorted intset", values);

    EXPECT_EQ(values.size(), 4U);
    EXPECT_EQ(values.encoding(), intset::Encoding::Int16);
    EXPECT_EQ(snapshot(values), (std::vector<std::int64_t>{-5, 10, 20, 30}));
}

TEST(IntsetTest, ContainsReturnsFalseForValueOutsideCurrentEncoding) {
    spdlog::info("ContainsReturnsFalseForValueOutsideCurrentEncoding: query wider value");
    intset values;

    ASSERT_TRUE(values.insert(10));

    EXPECT_FALSE(values.contains(100000));
    EXPECT_EQ(values.encoding(), intset::Encoding::Int16);
}

TEST(IntsetTest, InsertUpgradesToInt32AndAppendsPositiveValue) {
    spdlog::info("InsertUpgradesToInt32AndAppendsPositiveValue: positive wide value upgrades and appends");
    intset values;

    ASSERT_TRUE(values.insert(-5));
    ASSERT_TRUE(values.insert(10));

    EXPECT_TRUE(values.insert(100000));

    EXPECT_TRUE(values.contains(100000));
    EXPECT_EQ(values.size(), 3U);
    EXPECT_EQ(values.byteSize(), 3U * sizeof(std::int32_t));
    EXPECT_EQ(values.encoding(), intset::Encoding::Int32);
    EXPECT_EQ(snapshot(values), (std::vector<std::int64_t>{-5, 10, 100000}));
}

TEST(IntsetTest, InsertUpgradesToInt32AndPrependsNegativeValue) {
    spdlog::info("InsertUpgradesToInt32AndPrependsNegativeValue: negative wide value upgrades and prepends");
    intset values;

    ASSERT_TRUE(values.insert(10));
    ASSERT_TRUE(values.insert(20));

    EXPECT_TRUE(values.insert(-100000));

    EXPECT_TRUE(values.contains(-100000));
    EXPECT_EQ(values.size(), 3U);
    EXPECT_EQ(values.byteSize(), 3U * sizeof(std::int32_t));
    EXPECT_EQ(values.encoding(), intset::Encoding::Int32);
    EXPECT_EQ(snapshot(values), (std::vector<std::int64_t>{-100000, 10, 20}));
}

TEST(IntsetTest, InsertUpgradesFromInt32ToInt64) {
    spdlog::info("InsertUpgradesFromInt32ToInt64: Int32 contents upgrade to Int64");
    intset values;

    ASSERT_TRUE(values.insert(-100000));
    ASSERT_TRUE(values.insert(10));

    EXPECT_TRUE(values.insert(5000000000LL));

    EXPECT_TRUE(values.contains(5000000000LL));
    EXPECT_EQ(values.size(), 3U);
    EXPECT_EQ(values.byteSize(), 3U * sizeof(std::int64_t));
    EXPECT_EQ(values.encoding(), intset::Encoding::Int64);
    EXPECT_EQ(snapshot(values), (std::vector<std::int64_t>{-100000, 10, 5000000000LL}));
}

TEST(IntsetTest, ByteSizeFollowsCurrentEncodingAndLength) {
    spdlog::info("ByteSizeFollowsCurrentEncodingAndLength: byte size tracks compact encoding");
    intset values;

    EXPECT_EQ(values.byteSize(), 0U);

    ASSERT_TRUE(values.insert(10));
    ASSERT_TRUE(values.insert(20));

    EXPECT_EQ(values.encoding(), intset::Encoding::Int16);
    EXPECT_EQ(values.byteSize(), 2U * sizeof(std::int16_t));

    ASSERT_TRUE(values.insert(100000));

    EXPECT_EQ(values.encoding(), intset::Encoding::Int32);
    EXPECT_EQ(values.byteSize(), 3U * sizeof(std::int32_t));

    ASSERT_TRUE(values.insert(5000000000LL));

    EXPECT_EQ(values.encoding(), intset::Encoding::Int64);
    EXPECT_EQ(values.byteSize(), 4U * sizeof(std::int64_t));

    ASSERT_TRUE(values.erase(5000000000LL));

    EXPECT_EQ(values.encoding(), intset::Encoding::Int64);
    EXPECT_EQ(values.byteSize(), 3U * sizeof(std::int64_t));
    EXPECT_EQ(snapshot(values), (std::vector<std::int64_t>{10, 20, 100000}));
}

TEST(IntsetTest, EraseMissingValueReturnsFalse) {
    spdlog::info("EraseMissingValueReturnsFalse: erase missing values and preserve contents");
    intset values;

    EXPECT_FALSE(values.erase(10));

    ASSERT_TRUE(values.insert(10));
    ASSERT_TRUE(values.insert(20));

    EXPECT_FALSE(values.erase(30));
    EXPECT_FALSE(values.erase(100000));
    EXPECT_EQ(values.size(), 2U);
    EXPECT_EQ(values.encoding(), intset::Encoding::Int16);
    EXPECT_EQ(snapshot(values), (std::vector<std::int64_t>{10, 20}));
}

TEST(IntsetTest, EraseRemovesOnlyValue) {
    spdlog::info("EraseRemovesOnlyValue: erase single element and verify empty state");
    intset values;

    ASSERT_TRUE(values.insert(10));

    EXPECT_TRUE(values.erase(10));

    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);
    EXPECT_FALSE(values.contains(10));
    EXPECT_EQ(values.encoding(), intset::Encoding::Int16);
    EXPECT_EQ(snapshot(values), (std::vector<std::int64_t>{}));
}

TEST(IntsetTest, EraseHeadMiddleAndTailPreservesOrder) {
    spdlog::info("EraseHeadMiddleAndTailPreservesOrder: erase first, middle, and last values");
    intset values;

    ASSERT_TRUE(values.insert(10));
    ASSERT_TRUE(values.insert(20));
    ASSERT_TRUE(values.insert(30));
    ASSERT_TRUE(values.insert(40));
    ASSERT_TRUE(values.insert(50));

    EXPECT_TRUE(values.erase(10));
    EXPECT_TRUE(values.erase(30));
    EXPECT_TRUE(values.erase(50));
    log_snapshot("after erase", values);

    EXPECT_EQ(values.size(), 2U);
    EXPECT_FALSE(values.contains(10));
    EXPECT_FALSE(values.contains(30));
    EXPECT_FALSE(values.contains(50));
    EXPECT_TRUE(values.contains(20));
    EXPECT_TRUE(values.contains(40));
    EXPECT_EQ(values.encoding(), intset::Encoding::Int16);
    EXPECT_EQ(snapshot(values), (std::vector<std::int64_t>{20, 40}));
}

int main(int argc, char** argv) {
    spdlog::set_level(spdlog::level::info);
    spdlog::set_pattern("[%H:%M:%S] [%^%l%$] %v");
    spdlog::info("starting GoogleTest suite for intset");

    ::testing::InitGoogleTest(&argc, argv);
    const int result = RUN_ALL_TESTS();

    spdlog::info("GoogleTest suite finished with code {}", result);
    return result;
}
