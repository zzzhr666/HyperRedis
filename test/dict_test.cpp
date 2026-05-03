#include "hyper/datastructures/dict.hpp"

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

using namespace hyper;

namespace {

struct ConstantHash {
    std::size_t operator()(int) const noexcept {
        return 0;
    }
};

} // namespace

TEST(DictTest, StartsEmptyAndSupportsConstGet) {
    spdlog::info("StartsEmptyAndSupportsConstGet: create empty dict");
    dict<std::string, int> values;

    spdlog::info("StartsEmptyAndSupportsConstGet: verify empty state and missing key");
    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);
    EXPECT_FALSE(values.contains("missing"));
    EXPECT_EQ(values.get("missing"), nullptr);

    spdlog::info("StartsEmptyAndSupportsConstGet: insert alpha => 1 and read through const interface");
    values.insert("alpha", 1);
    const auto& const_values = values;
    const auto* value = const_values.get("alpha");

    ASSERT_NE(value, nullptr);
    EXPECT_TRUE(const_values.contains("alpha"));
    EXPECT_EQ(*value, 1);
    spdlog::info("StartsEmptyAndSupportsConstGet: alpha resolved to {}", *value);
}

TEST(DictTest, InsertRejectsDuplicateKeys) {
    spdlog::info("InsertRejectsDuplicateKeys: insert alpha twice with different values");
    dict<std::string, int> values;

    EXPECT_TRUE(values.insert("alpha", 1));
    EXPECT_FALSE(values.insert("alpha", 99));
    EXPECT_EQ(values.size(), 1U);

    auto* value = values.get("alpha");
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(*value, 1);
    spdlog::info("InsertRejectsDuplicateKeys: duplicate insert rejected, alpha kept value {}", *value);
}

TEST(DictTest, EraseRemovesExistingAndMissingKeysBehaveAsExpected) {
    spdlog::info("EraseRemovesExistingAndMissingKeysBehaveAsExpected: insert alpha beta gamma");
    dict<std::string, int> values;

    ASSERT_TRUE(values.insert("alpha", 1));
    ASSERT_TRUE(values.insert("beta", 2));
    ASSERT_TRUE(values.insert("gamma", 3));

    spdlog::info("EraseRemovesExistingAndMissingKeysBehaveAsExpected: erase existing beta");
    EXPECT_TRUE(values.erase("beta"));
    EXPECT_EQ(values.size(), 2U);
    EXPECT_FALSE(values.contains("beta"));
    EXPECT_TRUE(values.contains("alpha"));
    EXPECT_TRUE(values.contains("gamma"));

    spdlog::info("EraseRemovesExistingAndMissingKeysBehaveAsExpected: erase beta again and erase missing key");
    EXPECT_FALSE(values.erase("beta"));
    EXPECT_FALSE(values.erase("missing"));
    EXPECT_EQ(values.size(), 2U);
    spdlog::info("EraseRemovesExistingAndMissingKeysBehaveAsExpected: remaining size {}", values.size());
}

TEST(DictTest, ClearRemovesAllEntriesAndAllowsReuse) {
    spdlog::info("ClearRemovesAllEntriesAndAllowsReuse: insert alpha beta then clear");
    dict<std::string, int> values;

    ASSERT_TRUE(values.insert("alpha", 1));
    ASSERT_TRUE(values.insert("beta", 2));

    values.clear();

    spdlog::info("ClearRemovesAllEntriesAndAllowsReuse: verify cleared state and reuse with gamma");
    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);
    EXPECT_EQ(values.get("alpha"), nullptr);
    EXPECT_EQ(values.get("beta"), nullptr);

    EXPECT_TRUE(values.insert("gamma", 3));
    auto* value = values.get("gamma");
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(*value, 3);
    spdlog::info("ClearRemovesAllEntriesAndAllowsReuse: gamma inserted after clear with value {}", *value);
}

TEST(DictTest, CollisionChainsSupportFindAndEraseAcrossPositions) {
    spdlog::info("CollisionChainsSupportFindAndEraseAcrossPositions: use ConstantHash so all keys share one bucket");
    dict<int, std::string, ConstantHash> values;

    ASSERT_TRUE(values.insert(1, "one"));
    ASSERT_TRUE(values.insert(2, "two"));
    ASSERT_TRUE(values.insert(3, "three"));

    spdlog::info("CollisionChainsSupportFindAndEraseAcrossPositions: verify all colliding keys are retrievable");
    ASSERT_NE(values.get(1), nullptr);
    ASSERT_NE(values.get(2), nullptr);
    ASSERT_NE(values.get(3), nullptr);

    spdlog::info("CollisionChainsSupportFindAndEraseAcrossPositions: erase middle/earlier colliding key 2");
    EXPECT_TRUE(values.erase(2));
    EXPECT_EQ(values.get(2), nullptr);
    ASSERT_NE(values.get(1), nullptr);
    ASSERT_NE(values.get(3), nullptr);

    spdlog::info("CollisionChainsSupportFindAndEraseAcrossPositions: erase key 3 then final key 1");
    EXPECT_TRUE(values.erase(3));
    EXPECT_EQ(values.get(3), nullptr);
    ASSERT_NE(values.get(1), nullptr);

    EXPECT_TRUE(values.erase(1));
    EXPECT_TRUE(values.empty());
    spdlog::info("CollisionChainsSupportFindAndEraseAcrossPositions: collision chain fully removed");
}

TEST(DictTest, FindWorksWhileRehashIsInProgress) {
    spdlog::info("FindWorksWhileRehashIsInProgress: insert enough keys to trigger rehash");
    dict<int, int> values(4);

    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(values.insert(i, i * 10));
        spdlog::info("FindWorksWhileRehashIsInProgress: inserted key {} value {}", i, i * 10);
    }

    spdlog::info("FindWorksWhileRehashIsInProgress: verify all entries remain visible during rehash");
    EXPECT_EQ(values.size(), 5U);
    for (int i = 0; i < 5; ++i) {
        auto* value = values.get(i);
        ASSERT_NE(value, nullptr);
        EXPECT_EQ(*value, i * 10);
    }

    spdlog::info("FindWorksWhileRehashIsInProgress: duplicate insert for key 3 must be rejected");
    EXPECT_FALSE(values.insert(3, 300));
    auto* existing = values.get(3);
    ASSERT_NE(existing, nullptr);
    EXPECT_EQ(*existing, 30);
    spdlog::info("FindWorksWhileRehashIsInProgress: key 3 still holds {}", *existing);
}

TEST(DictTest, ManyInsertsPreserveAllEntriesAcrossResizeAndRehash) {
    spdlog::info("ManyInsertsPreserveAllEntriesAcrossResizeAndRehash: insert 64 keys through repeated resize/rehash");
    dict<int, int> values(4);

    for (int i = 0; i < 64; ++i) {
        ASSERT_TRUE(values.insert(i, i + 100));
        EXPECT_EQ(values.size(), static_cast<std::size_t>(i + 1));
        if (i == 0 || i == 3 || i == 4 || i == 7 || i == 15 || i == 31 || i == 63) {
            spdlog::info("ManyInsertsPreserveAllEntriesAcrossResizeAndRehash: checkpoint after inserting key {} size {}", i, values.size());
        }
    }

    spdlog::info("ManyInsertsPreserveAllEntriesAcrossResizeAndRehash: verify every key survived all growth steps");
    for (int i = 0; i < 64; ++i) {
        auto* value = values.get(i);
        ASSERT_NE(value, nullptr);
        EXPECT_EQ(*value, i + 100);
    }
    spdlog::info("ManyInsertsPreserveAllEntriesAcrossResizeAndRehash: final size {}", values.size());
}

TEST(DictTest, EraseMaintainsCorrectContentsWhileAdvancingRehash) {
    spdlog::info("EraseMaintainsCorrectContentsWhileAdvancingRehash: insert 32 keys then erase every even key");
    dict<int, int> values(4);

    for (int i = 0; i < 32; ++i) {
        ASSERT_TRUE(values.insert(i, i * 2));
    }

    for (int i = 0; i < 32; i += 2) {
        EXPECT_TRUE(values.erase(i));
        if (i <= 6 || i >= 24) {
            spdlog::info("EraseMaintainsCorrectContentsWhileAdvancingRehash: erased even key {} remaining size {}", i, values.size());
        }
    }

    EXPECT_EQ(values.size(), 16U);

    spdlog::info("EraseMaintainsCorrectContentsWhileAdvancingRehash: verify evens removed and odds preserved");
    for (int i = 0; i < 32; ++i) {
        auto* value = values.get(i);
        if (i % 2 == 0) {
            EXPECT_EQ(value, nullptr);
        } else {
            ASSERT_NE(value, nullptr);
            EXPECT_EQ(*value, i * 2);
        }
    }
    spdlog::info("EraseMaintainsCorrectContentsWhileAdvancingRehash: final size {}", values.size());
}

TEST(DictTest, ShrinkAfterHeavyErasePreservesRemainingAndFutureInsertions) {
    spdlog::info("ShrinkAfterHeavyErasePreservesRemainingAndFutureInsertions: insert 64 keys to grow table");
    dict<int, int> values(4);

    for (int i = 0; i < 64; ++i) {
        ASSERT_TRUE(values.insert(i, i + 1000));
    }
    EXPECT_EQ(values.size(), 64U);

    spdlog::info("ShrinkAfterHeavyErasePreservesRemainingAndFutureInsertions: erase keys 0..59 to trigger shrink");
    for (int i = 0; i < 60; ++i) {
        ASSERT_TRUE(values.erase(i));
        if (i == 0 || i == 15 || i == 31 || i == 47 || i == 59) {
            spdlog::info("ShrinkAfterHeavyErasePreservesRemainingAndFutureInsertions: erased key {} current size {}", i, values.size());
        }
    }

    EXPECT_EQ(values.size(), 4U);
    spdlog::info("ShrinkAfterHeavyErasePreservesRemainingAndFutureInsertions: verify only 60..63 remain");

    for (int i = 0; i < 60; ++i) {
        EXPECT_EQ(values.get(i), nullptr);
    }
    for (int i = 60; i < 64; ++i) {
        auto* value = values.get(i);
        ASSERT_NE(value, nullptr);
        EXPECT_EQ(*value, i + 1000);
    }

    spdlog::info("ShrinkAfterHeavyErasePreservesRemainingAndFutureInsertions: insert new keys after shrink");
    EXPECT_TRUE(values.insert(100, 2000));
    EXPECT_TRUE(values.insert(101, 2001));

    auto* hundred = values.get(100);
    auto* hundred_one = values.get(101);
    ASSERT_NE(hundred, nullptr);
    ASSERT_NE(hundred_one, nullptr);
    EXPECT_EQ(*hundred, 2000);
    EXPECT_EQ(*hundred_one, 2001);
    EXPECT_EQ(values.size(), 6U);
    spdlog::info("ShrinkAfterHeavyErasePreservesRemainingAndFutureInsertions: post-shrink reinsertion succeeded, final size {}", values.size());
}

TEST(DictTest, ConstForEachVisitsAllEntriesWithoutExposingInternalNodes) {
    spdlog::info("ConstForEachVisitsAllEntriesWithoutExposingInternalNodes: collect keys and accumulate values through const traversal");
    dict<std::string, int> values;
    ASSERT_TRUE(values.insert("alpha", 1));
    ASSERT_TRUE(values.insert("beta", 2));
    ASSERT_TRUE(values.insert("gamma", 3));

    const auto& const_values = values;
    std::vector<std::string> visited_keys;
    int value_sum = 0;

    const_values.forEach([&](const std::string& key, const int& value) {
        visited_keys.push_back(key);
        value_sum += value;
        spdlog::info("ConstForEachVisitsAllEntriesWithoutExposingInternalNodes: visited {} => {}", key, value);
    });

    std::sort(visited_keys.begin(), visited_keys.end());
    EXPECT_EQ(visited_keys, (std::vector<std::string>{"alpha", "beta", "gamma"}));
    EXPECT_EQ(value_sum, 6);
}

TEST(DictTest, MutableForEachCanUpdateValuesAcrossRehash) {
    spdlog::info("MutableForEachCanUpdateValuesAcrossRehash: insert 5 keys to force traversal across ht[0] and ht[1]");
    dict<int, int> values(4);

    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(values.insert(i, i * 10));
    }

    int visited_count = 0;
    values.forEach([&](const int& key, int& value) {
        value += 7;
        ++visited_count;
        spdlog::info("MutableForEachCanUpdateValuesAcrossRehash: updated key {} to {}", key, value);
    });

    EXPECT_EQ(visited_count, 5);
    for (int i = 0; i < 5; ++i) {
        auto* value = values.get(i);
        ASSERT_NE(value, nullptr);
        EXPECT_EQ(*value, i * 10 + 7);
    }
}

TEST(DictTest, InsertOrAssignInsertsMissingAndUpdatesExisting) {
    spdlog::info("InsertOrAssignInsertsMissingAndUpdatesExisting: insert missing alpha, update alpha, insert beta");
    dict<std::string, int> values;

    EXPECT_TRUE(values.insertOrAssign("alpha", 1));
    auto* alpha = values.get("alpha");
    ASSERT_NE(alpha, nullptr);
    EXPECT_EQ(*alpha, 1);
    EXPECT_EQ(values.size(), 1U);
    spdlog::info("InsertOrAssignInsertsMissingAndUpdatesExisting: alpha inserted with {}", *alpha);

    EXPECT_FALSE(values.insertOrAssign("alpha", 9));
    alpha = values.get("alpha");
    ASSERT_NE(alpha, nullptr);
    EXPECT_EQ(*alpha, 9);
    EXPECT_EQ(values.size(), 1U);
    spdlog::info("InsertOrAssignInsertsMissingAndUpdatesExisting: alpha updated to {}", *alpha);

    EXPECT_TRUE(values.insertOrAssign("beta", 2));
    auto* beta = values.get("beta");
    ASSERT_NE(beta, nullptr);
    EXPECT_EQ(*beta, 2);
    EXPECT_EQ(values.size(), 2U);
    spdlog::info("InsertOrAssignInsertsMissingAndUpdatesExisting: beta inserted with {}, final size {}", *beta, values.size());
}

TEST(DictTest, TransparentStringLookupAcceptsStringViewAndCString) {
    dict<std::string, int, transparentStringHash, transparentStringEqual> values;

    ASSERT_TRUE(values.insert("alpha", 1));
    ASSERT_TRUE(values.insert("beta", 2));

    std::string_view alpha_view{"alpha"};
    EXPECT_TRUE(values.contains(alpha_view));
    ASSERT_NE(values.get(alpha_view), nullptr);
    EXPECT_EQ(*values.get(alpha_view), 1);

    EXPECT_TRUE(values.contains("beta"));
    ASSERT_NE(values.get("beta"), nullptr);
    EXPECT_EQ(*values.get("beta"), 2);

    EXPECT_TRUE(values.erase(std::string_view{"alpha"}));
    EXPECT_FALSE(values.contains(alpha_view));
    EXPECT_EQ(values.get(alpha_view), nullptr);
    EXPECT_EQ(values.size(), 1U);
}

TEST(DictTest, CopyAndMoveOperationsRemainDisabled) {
    spdlog::info("CopyAndMoveOperationsRemainDisabled: verify dict disables copy and move semantics");
    static_assert(!std::is_copy_constructible_v<dict<int, int>>);
    static_assert(!std::is_copy_assignable_v<dict<int, int>>);
    static_assert(!std::is_move_constructible_v<dict<int, int>>);
    static_assert(!std::is_move_assignable_v<dict<int, int>>);
}

int main(int argc, char** argv) {
    spdlog::set_level(spdlog::level::info);
    spdlog::set_pattern("[%H:%M:%S] [%^%l%$] %v");
    spdlog::info("starting GoogleTest suite for dict");

    ::testing::InitGoogleTest(&argc, argv);
    const int result = RUN_ALL_TESTS();

    spdlog::info("GoogleTest suite finished with code {}", result);
    return result;
}
