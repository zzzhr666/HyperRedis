#include "hyper/datastructures/skip_list.hpp"

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <iterator>
#include <optional>
#include <random>
#include <set>
#include <type_traits>
#include <utility>
#include <vector>

using namespace hyper;

ScoreRange<int> scoreRange(int min, int max, bool min_inclusive = true, bool max_inclusive = true) {
    return {min, max, min_inclusive, max_inclusive};
}

bool containsScore(const ScoreRange<int>& range, int score) {
    const bool above_min = range.min_inclusive ? score >= range.min : score > range.min;
    const bool below_max = range.max_inclusive ? score <= range.max : score < range.max;
    return above_min && below_max;
}

bool invalidRange(const ScoreRange<int>& range) {
    return range.min > range.max || (range.min == range.max && (!range.min_inclusive || !range.max_inclusive));
}

template <typename Score, typename Value>
std::vector<std::pair<Score, Value>> snapshot(const skipList<Score, Value>& values) {
    std::vector<std::pair<Score, Value>> result;
    values.forEach([&result](const Score& score, const Value& value) {
        result.emplace_back(score, value);
    });
    return result;
}

TEST(SkipListTest, StartsEmpty) {
    spdlog::info("StartsEmpty: create a new skipList and verify empty state");
    skipList<int, int> values;

    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);
    spdlog::info("StartsEmpty: size={}, empty={}", values.size(), values.empty());
}

TEST(SkipListTest, ClearKeepsEmptyState) {
    spdlog::info("ClearKeepsEmptyState: clear an already empty skipList");
    skipList<int, int> values;

    values.clear();

    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);
    spdlog::info("ClearKeepsEmptyState: size={}, empty={}", values.size(), values.empty());
}

TEST(SkipListTest, ClearIsIdempotent) {
    spdlog::info("ClearIsIdempotent: call clear twice on an empty skipList");
    skipList<int, int> values;

    values.clear();
    values.clear();

    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);
    spdlog::info("ClearIsIdempotent: repeated clear kept size={}", values.size());
}

TEST(SkipListTest, InsertMakesElementVisible) {
    spdlog::info("InsertMakesElementVisible: insert (10, 100) and verify lookup behavior");
    skipList<int, int> values;

    EXPECT_TRUE(values.insert(10, 100));

    EXPECT_FALSE(values.empty());
    EXPECT_EQ(values.size(), 1U);
    EXPECT_TRUE(values.contains(10, 100));
    EXPECT_FALSE(values.contains(10, 200));
    EXPECT_FALSE(values.contains(20, 100));
    spdlog::info("InsertMakesElementVisible: inserted element visible, size={}", values.size());
}

TEST(SkipListTest, InsertRejectsDuplicateScoreAndValue) {
    spdlog::info("InsertRejectsDuplicateScoreAndValue: duplicate (10, 100) should be rejected");
    skipList<int, int> values;

    EXPECT_TRUE(values.insert(10, 100));
    EXPECT_FALSE(values.insert(10, 100));

    EXPECT_EQ(values.size(), 1U);
    EXPECT_TRUE(values.contains(10, 100));
    spdlog::info("InsertRejectsDuplicateScoreAndValue: duplicate rejected, size={}", values.size());
}

TEST(SkipListTest, AllowsSameScoreWithDifferentValue) {
    spdlog::info("AllowsSameScoreWithDifferentValue: same score with different values should coexist");
    skipList<int, int> values;

    EXPECT_TRUE(values.insert(10, 100));
    EXPECT_TRUE(values.insert(10, 200));

    EXPECT_EQ(values.size(), 2U);
    EXPECT_TRUE(values.contains(10, 100));
    EXPECT_TRUE(values.contains(10, 200));
    spdlog::info("AllowsSameScoreWithDifferentValue: both values found, size={}", values.size());
}

TEST(SkipListTest, ForEachDoesNotVisitEmptyList) {
    spdlog::info("ForEachDoesNotVisitEmptyList: verify empty traversal invokes no callback");
    skipList<int, int> values;
    int visit_count = 0;

    values.forEach([&visit_count](const int&, const int&) {
        ++visit_count;
    });

    EXPECT_EQ(visit_count, 0);
    spdlog::info("ForEachDoesNotVisitEmptyList: callback count={}", visit_count);
}

TEST(SkipListTest, ForEachVisitsElementsInSortedOrder) {
    spdlog::info("ForEachVisitsElementsInSortedOrder: insert scores 30, 10, 20 and expect sorted traversal");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));

    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {10, 100},
        {20, 200},
        {30, 300},
    }));
    spdlog::info("ForEachVisitsElementsInSortedOrder: traversal order verified");
}

TEST(SkipListTest, ForEachOrdersSameScoreByValue) {
    spdlog::info("ForEachOrdersSameScoreByValue: same score should be ordered by value");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 300));
    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(10, 200));

    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {10, 100},
        {10, 200},
        {10, 300},
    }));
    spdlog::info("ForEachOrdersSameScoreByValue: tie-break order verified");
}

TEST(SkipListTest, EraseMissingElementReturnsFalse) {
    spdlog::info("EraseMissingElementReturnsFalse: erase missing keys and preserve existing data");
    skipList<int, int> values;

    EXPECT_FALSE(values.erase(10, 100));

    ASSERT_TRUE(values.insert(10, 100));
    EXPECT_FALSE(values.erase(10, 200));
    EXPECT_FALSE(values.erase(20, 100));
    EXPECT_EQ(values.size(), 1U);
    EXPECT_TRUE(values.contains(10, 100));
    spdlog::info("EraseMissingElementReturnsFalse: missing erases rejected, size={}", values.size());
}

TEST(SkipListTest, EraseRemovesExistingElement) {
    spdlog::info("EraseRemovesExistingElement: erase the only element and verify empty result");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));

    EXPECT_TRUE(values.erase(10, 100));

    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);
    EXPECT_FALSE(values.contains(10, 100));
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{}));
    spdlog::info("EraseRemovesExistingElement: element removed, size={}", values.size());
}

TEST(SkipListTest, EraseHeadMiddleAndTailPreservesOrder) {
    spdlog::info("EraseHeadMiddleAndTailPreservesOrder: erase first, middle, and last elements");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(40, 400));
    ASSERT_TRUE(values.insert(50, 500));

    EXPECT_TRUE(values.erase(10, 100));
    EXPECT_TRUE(values.erase(30, 300));
    EXPECT_TRUE(values.erase(50, 500));

    EXPECT_EQ(values.size(), 2U);
    EXPECT_FALSE(values.contains(10, 100));
    EXPECT_FALSE(values.contains(30, 300));
    EXPECT_FALSE(values.contains(50, 500));
    EXPECT_TRUE(values.contains(20, 200));
    EXPECT_TRUE(values.contains(40, 400));
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {20, 200},
        {40, 400},
    }));
    spdlog::info("EraseHeadMiddleAndTailPreservesOrder: remaining order is (20,200), (40,400)");
}

TEST(SkipListTest, GetRankReturnsOneBasedRank) {
    spdlog::info("GetRankReturnsOneBasedRank: verify rank after out-of-order inserts");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));

    EXPECT_EQ(values.getRank(10, 100), 1U);
    EXPECT_EQ(values.getRank(20, 200), 2U);
    EXPECT_EQ(values.getRank(30, 300), 3U);
    spdlog::info("GetRankReturnsOneBasedRank: ranks 1, 2, 3 verified");
}

TEST(SkipListTest, GetRankOrdersSameScoreByValue) {
    spdlog::info("GetRankOrdersSameScoreByValue: verify rank tie-break by value");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 300));
    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(10, 200));

    EXPECT_EQ(values.getRank(10, 100), 1U);
    EXPECT_EQ(values.getRank(10, 200), 2U);
    EXPECT_EQ(values.getRank(10, 300), 3U);
    spdlog::info("GetRankOrdersSameScoreByValue: same-score ranks verified");
}

TEST(SkipListTest, GetRankReturnsZeroForMissingElement) {
    spdlog::info("GetRankReturnsZeroForMissingElement: missing rank should be zero");
    skipList<int, int> values;

    EXPECT_EQ(values.getRank(10, 100), 0U);

    ASSERT_TRUE(values.insert(10, 100));
    EXPECT_EQ(values.getRank(10, 200), 0U);
    EXPECT_EQ(values.getRank(20, 100), 0U);
    spdlog::info("GetRankReturnsZeroForMissingElement: missing ranks returned zero");
}

TEST(SkipListTest, GetRankUpdatesAfterErase) {
    spdlog::info("GetRankUpdatesAfterErase: erase rank 2 and verify remaining ranks collapse");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(40, 400));

    ASSERT_TRUE(values.erase(20, 200));

    EXPECT_EQ(values.getRank(10, 100), 1U);
    EXPECT_EQ(values.getRank(20, 200), 0U);
    EXPECT_EQ(values.getRank(30, 300), 2U);
    EXPECT_EQ(values.getRank(40, 400), 3U);
    spdlog::info("GetRankUpdatesAfterErase: rank update after erase verified");
}

TEST(SkipListTest, GetElementByRankReturnsEmptyForOutOfRangeRank) {
    spdlog::info("GetElementByRankReturnsEmptyForOutOfRangeRank: invalid ranks should return empty optional");
    skipList<int, int> values;

    EXPECT_FALSE(values.getElementByRank(0).has_value());
    EXPECT_FALSE(values.getElementByRank(1).has_value());

    ASSERT_TRUE(values.insert(10, 100));
    EXPECT_FALSE(values.getElementByRank(0).has_value());
    EXPECT_FALSE(values.getElementByRank(2).has_value());
    spdlog::info("GetElementByRankReturnsEmptyForOutOfRangeRank: out-of-range checks passed");
}

TEST(SkipListTest, GetElementByRankReturnsElementAtOneBasedRank) {
    spdlog::info("GetElementByRankReturnsElementAtOneBasedRank: rank lookup should return sorted elements");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));

    EXPECT_EQ(values.getElementByRank(1), (std::optional<std::pair<int, int>>{{10, 100}}));
    EXPECT_EQ(values.getElementByRank(2), (std::optional<std::pair<int, int>>{{20, 200}}));
    EXPECT_EQ(values.getElementByRank(3), (std::optional<std::pair<int, int>>{{30, 300}}));
    spdlog::info("GetElementByRankReturnsElementAtOneBasedRank: ranks 1..3 resolved correctly");
}

TEST(SkipListTest, GetElementByRankOrdersSameScoreByValue) {
    spdlog::info("GetElementByRankOrdersSameScoreByValue: same-score rank lookup should use value order");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 300));
    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(10, 200));

    EXPECT_EQ(values.getElementByRank(1), (std::optional<std::pair<int, int>>{{10, 100}}));
    EXPECT_EQ(values.getElementByRank(2), (std::optional<std::pair<int, int>>{{10, 200}}));
    EXPECT_EQ(values.getElementByRank(3), (std::optional<std::pair<int, int>>{{10, 300}}));
    spdlog::info("GetElementByRankOrdersSameScoreByValue: same-score rank lookup verified");
}

TEST(SkipListTest, GetElementByRankUpdatesAfterErase) {
    spdlog::info("GetElementByRankUpdatesAfterErase: erase one element and verify rank lookup updates");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(40, 400));

    ASSERT_TRUE(values.erase(20, 200));

    EXPECT_EQ(values.getElementByRank(1), (std::optional<std::pair<int, int>>{{10, 100}}));
    EXPECT_EQ(values.getElementByRank(2), (std::optional<std::pair<int, int>>{{30, 300}}));
    EXPECT_EQ(values.getElementByRank(3), (std::optional<std::pair<int, int>>{{40, 400}}));
    EXPECT_FALSE(values.getElementByRank(4).has_value());
    spdlog::info("GetElementByRankUpdatesAfterErase: rank lookup after erase verified");
}

TEST(SkipListTest, IsInScoreRangeRejectsEmptyAndInvalidRange) {
    spdlog::info("IsInScoreRangeRejectsEmptyAndInvalidRange: empty list and invalid ranges should return false");
    skipList<int, int> values;

    EXPECT_FALSE(values.isInScoreRange(scoreRange(10, 20)));

    ASSERT_TRUE(values.insert(10, 100));
    EXPECT_FALSE(values.isInScoreRange(scoreRange(20, 10)));
    spdlog::info("IsInScoreRangeRejectsEmptyAndInvalidRange: range rejection verified");
}

TEST(SkipListTest, IsInScoreRangeRejectsRangesOutsideExistingScores) {
    spdlog::info("IsInScoreRangeRejectsRangesOutsideExistingScores: ranges below or above all scores should return false");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));

    EXPECT_FALSE(values.isInScoreRange(scoreRange(1, 9)));
    EXPECT_FALSE(values.isInScoreRange(scoreRange(31, 40)));
    spdlog::info("IsInScoreRangeRejectsRangesOutsideExistingScores: outside ranges rejected");
}

TEST(SkipListTest, IsInScoreRangeAcceptsOverlappingRanges) {
    spdlog::info("IsInScoreRangeAcceptsOverlappingRanges: boundary and middle overlaps should return true");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));

    EXPECT_TRUE(values.isInScoreRange(scoreRange(10, 10)));
    EXPECT_TRUE(values.isInScoreRange(scoreRange(30, 30)));
    EXPECT_TRUE(values.isInScoreRange(scoreRange(15, 25)));
    EXPECT_TRUE(values.isInScoreRange(scoreRange(1, 100)));
    spdlog::info("IsInScoreRangeAcceptsOverlappingRanges: overlapping ranges accepted");
}

TEST(SkipListTest, FirstInScoreRangeReturnsEmptyForMissingRanges) {
    spdlog::info("FirstInScoreRangeReturnsEmptyForMissingRanges: missing ranges should return empty optional");
    skipList<int, int> values;

    EXPECT_FALSE(values.firstInScoreRange(scoreRange(10, 20)).has_value());

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));

    EXPECT_FALSE(values.firstInScoreRange(scoreRange(20, 10)).has_value());
    EXPECT_FALSE(values.firstInScoreRange(scoreRange(1, 9)).has_value());
    EXPECT_FALSE(values.firstInScoreRange(scoreRange(31, 40)).has_value());
    spdlog::info("FirstInScoreRangeReturnsEmptyForMissingRanges: missing range checks passed");
}

TEST(SkipListTest, FirstInScoreRangeReturnsFirstMatchingElement) {
    spdlog::info("FirstInScoreRangeReturnsFirstMatchingElement: first matching score should be returned");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(40, 400));

    EXPECT_EQ(values.firstInScoreRange(scoreRange(10, 10)), (std::optional<std::pair<int, int>>{{10, 100}}));
    EXPECT_EQ(values.firstInScoreRange(scoreRange(15, 35)), (std::optional<std::pair<int, int>>{{20, 200}}));
    EXPECT_EQ(values.firstInScoreRange(scoreRange(25, 100)), (std::optional<std::pair<int, int>>{{30, 300}}));
    EXPECT_EQ(values.firstInScoreRange(scoreRange(1, 100)), (std::optional<std::pair<int, int>>{{10, 100}}));
    spdlog::info("FirstInScoreRangeReturnsFirstMatchingElement: first matches verified");
}

TEST(SkipListTest, FirstInScoreRangeOrdersSameScoreByValue) {
    spdlog::info("FirstInScoreRangeOrdersSameScoreByValue: same score should return smallest value in range");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 300));
    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(10, 200));
    ASSERT_TRUE(values.insert(20, 400));

    EXPECT_EQ(values.firstInScoreRange(scoreRange(10, 10)), (std::optional<std::pair<int, int>>{{10, 100}}));
    EXPECT_EQ(values.firstInScoreRange(scoreRange(10, 20)), (std::optional<std::pair<int, int>>{{10, 100}}));
    EXPECT_EQ(values.firstInScoreRange(scoreRange(11, 20)), (std::optional<std::pair<int, int>>{{20, 400}}));
    spdlog::info("FirstInScoreRangeOrdersSameScoreByValue: same-score range lookup verified");
}

TEST(SkipListTest, LastInScoreRangeReturnsEmptyForMissingRanges) {
    spdlog::info("LastInScoreRangeReturnsEmptyForMissingRanges: missing ranges should return empty optional");
    skipList<int, int> values;

    EXPECT_FALSE(values.lastInScoreRange(scoreRange(10, 20)).has_value());

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));

    EXPECT_FALSE(values.lastInScoreRange(scoreRange(20, 10)).has_value());
    EXPECT_FALSE(values.lastInScoreRange(scoreRange(1, 9)).has_value());
    EXPECT_FALSE(values.lastInScoreRange(scoreRange(31, 40)).has_value());
    spdlog::info("LastInScoreRangeReturnsEmptyForMissingRanges: missing range checks passed");
}

TEST(SkipListTest, LastInScoreRangeReturnsLastMatchingElement) {
    spdlog::info("LastInScoreRangeReturnsLastMatchingElement: last matching score should be returned");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(40, 400));

    EXPECT_EQ(values.lastInScoreRange(scoreRange(40, 40)), (std::optional<std::pair<int, int>>{{40, 400}}));
    EXPECT_EQ(values.lastInScoreRange(scoreRange(15, 35)), (std::optional<std::pair<int, int>>{{30, 300}}));
    EXPECT_EQ(values.lastInScoreRange(scoreRange(1, 25)), (std::optional<std::pair<int, int>>{{20, 200}}));
    EXPECT_EQ(values.lastInScoreRange(scoreRange(1, 100)), (std::optional<std::pair<int, int>>{{40, 400}}));
    spdlog::info("LastInScoreRangeReturnsLastMatchingElement: last matches verified");
}

TEST(SkipListTest, LastInScoreRangeOrdersSameScoreByValue) {
    spdlog::info("LastInScoreRangeOrdersSameScoreByValue: same score should return largest value in range");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 300));
    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(10, 200));
    ASSERT_TRUE(values.insert(20, 400));

    EXPECT_EQ(values.lastInScoreRange(scoreRange(10, 10)), (std::optional<std::pair<int, int>>{{10, 300}}));
    EXPECT_EQ(values.lastInScoreRange(scoreRange(1, 10)), (std::optional<std::pair<int, int>>{{10, 300}}));
    EXPECT_EQ(values.lastInScoreRange(scoreRange(10, 20)), (std::optional<std::pair<int, int>>{{20, 400}}));
    spdlog::info("LastInScoreRangeOrdersSameScoreByValue: same-score range lookup verified");
}

TEST(SkipListTest, ScoreRangeHonorsExclusiveBoundsForLookup) {
    spdlog::info("ScoreRangeHonorsExclusiveBoundsForLookup: exclusive bounds should affect range queries");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(40, 400));

    EXPECT_FALSE(values.isInScoreRange(scoreRange(10, 10, false, true)));
    EXPECT_TRUE(values.isInScoreRange(scoreRange(10, 20, false, true)));
    EXPECT_TRUE(values.isInScoreRange(scoreRange(10, 20, true, false)));

    EXPECT_FALSE(values.firstInScoreRange(scoreRange(10, 20, false, false)).has_value());
    EXPECT_FALSE(values.lastInScoreRange(scoreRange(10, 20, false, false)).has_value());
    EXPECT_EQ(values.firstInScoreRange(scoreRange(10, 30, false, true)),
              (std::optional<std::pair<int, int>>{{20, 200}}));
    EXPECT_EQ(values.firstInScoreRange(scoreRange(10, 30, true, false)),
              (std::optional<std::pair<int, int>>{{10, 100}}));
    EXPECT_EQ(values.lastInScoreRange(scoreRange(10, 30, true, false)),
              (std::optional<std::pair<int, int>>{{20, 200}}));
    EXPECT_EQ(values.lastInScoreRange(scoreRange(10, 30, false, true)),
              (std::optional<std::pair<int, int>>{{30, 300}}));
    spdlog::info("ScoreRangeHonorsExclusiveBoundsForLookup: exclusive lookup bounds verified");
}

TEST(SkipListTest, EraseRangeByScoreReturnsZeroForMissingRanges) {
    spdlog::info("EraseRangeByScoreReturnsZeroForMissingRanges: missing ranges should remove nothing");
    skipList<int, int> values;

    EXPECT_EQ(values.eraseRangeByScore(scoreRange(10, 20)), 0U);

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));

    EXPECT_EQ(values.eraseRangeByScore(scoreRange(20, 10)), 0U);
    EXPECT_EQ(values.eraseRangeByScore(scoreRange(1, 9)), 0U);
    EXPECT_EQ(values.eraseRangeByScore(scoreRange(31, 40)), 0U);
    EXPECT_EQ(values.size(), 3U);
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {10, 100},
        {20, 200},
        {30, 300},
    }));
    spdlog::info("EraseRangeByScoreReturnsZeroForMissingRanges: missing range checks passed");
}

TEST(SkipListTest, EraseRangeByScoreRemovesMiddleRange) {
    spdlog::info("EraseRangeByScoreRemovesMiddleRange: remove middle scores and preserve rank spans");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(40, 400));
    ASSERT_TRUE(values.insert(50, 500));

    EXPECT_EQ(values.eraseRangeByScore(scoreRange(20, 40)), 3U);

    EXPECT_EQ(values.size(), 2U);
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {10, 100},
        {50, 500},
    }));
    EXPECT_EQ(values.getRank(10, 100), 1U);
    EXPECT_EQ(values.getRank(50, 500), 2U);
    EXPECT_EQ(values.getRank(30, 300), 0U);
    EXPECT_EQ(values.getElementByRank(1), (std::optional<std::pair<int, int>>{{10, 100}}));
    EXPECT_EQ(values.getElementByRank(2), (std::optional<std::pair<int, int>>{{50, 500}}));
    EXPECT_FALSE(values.getElementByRank(3).has_value());
    spdlog::info("EraseRangeByScoreRemovesMiddleRange: middle removal verified");
}

TEST(SkipListTest, EraseRangeByScoreRemovesHeadAndTailRanges) {
    spdlog::info("EraseRangeByScoreRemovesHeadAndTailRanges: remove boundary ranges in sequence");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(40, 400));
    ASSERT_TRUE(values.insert(50, 500));

    EXPECT_EQ(values.eraseRangeByScore(scoreRange(1, 20)), 2U);

    EXPECT_EQ(values.size(), 3U);
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {30, 300},
        {40, 400},
        {50, 500},
    }));
    EXPECT_EQ(values.getRank(30, 300), 1U);
    EXPECT_EQ(values.getElementByRank(1), (std::optional<std::pair<int, int>>{{30, 300}}));

    EXPECT_EQ(values.eraseRangeByScore(scoreRange(40, 100)), 2U);

    EXPECT_EQ(values.size(), 1U);
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {30, 300},
    }));
    EXPECT_EQ(values.getRank(30, 300), 1U);
    EXPECT_EQ(values.getElementByRank(1), (std::optional<std::pair<int, int>>{{30, 300}}));
    EXPECT_FALSE(values.getElementByRank(2).has_value());
    spdlog::info("EraseRangeByScoreRemovesHeadAndTailRanges: boundary removals verified");
}

TEST(SkipListTest, EraseRangeByScoreRemovesWholeList) {
    spdlog::info("EraseRangeByScoreRemovesWholeList: remove all scores and allow reuse");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));

    EXPECT_EQ(values.eraseRangeByScore(scoreRange(1, 100)), 3U);

    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{}));
    EXPECT_FALSE(values.getElementByRank(1).has_value());
    EXPECT_EQ(values.getRank(10, 100), 0U);

    EXPECT_TRUE(values.insert(40, 400));
    EXPECT_EQ(values.size(), 1U);
    EXPECT_EQ(values.getElementByRank(1), (std::optional<std::pair<int, int>>{{40, 400}}));
    spdlog::info("EraseRangeByScoreRemovesWholeList: whole-list removal and reuse verified");
}

TEST(SkipListTest, EraseRangeByScoreRemovesAllValuesWithSameScore) {
    spdlog::info("EraseRangeByScoreRemovesAllValuesWithSameScore: all values with matching score should be removed");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(5, 50));
    ASSERT_TRUE(values.insert(10, 300));
    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(10, 200));
    ASSERT_TRUE(values.insert(20, 400));

    EXPECT_EQ(values.eraseRangeByScore(scoreRange(10, 10)), 3U);

    EXPECT_EQ(values.size(), 2U);
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {5, 50},
        {20, 400},
    }));
    EXPECT_FALSE(values.contains(10, 100));
    EXPECT_FALSE(values.contains(10, 200));
    EXPECT_FALSE(values.contains(10, 300));
    EXPECT_EQ(values.getRank(5, 50), 1U);
    EXPECT_EQ(values.getRank(20, 400), 2U);
    EXPECT_EQ(values.getElementByRank(1), (std::optional<std::pair<int, int>>{{5, 50}}));
    EXPECT_EQ(values.getElementByRank(2), (std::optional<std::pair<int, int>>{{20, 400}}));
    spdlog::info("EraseRangeByScoreRemovesAllValuesWithSameScore: same-score removal verified");
}

TEST(SkipListTest, EraseRangeByScoreHonorsExclusiveBounds) {
    spdlog::info("EraseRangeByScoreHonorsExclusiveBounds: exclusive bounds should affect score removal");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(40, 400));

    EXPECT_EQ(values.eraseRangeByScore(scoreRange(10, 30, false, false)), 1U);

    EXPECT_EQ(values.size(), 3U);
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {10, 100},
        {30, 300},
        {40, 400},
    }));
    EXPECT_EQ(values.getRank(10, 100), 1U);
    EXPECT_EQ(values.getRank(30, 300), 2U);
    EXPECT_EQ(values.getRank(40, 400), 3U);
    EXPECT_FALSE(values.contains(20, 200));
    spdlog::info("EraseRangeByScoreHonorsExclusiveBounds: exclusive removal bounds verified");
}

TEST(SkipListTest, EraseRangeByRankReturnsZeroForInvalidRanges) {
    spdlog::info("EraseRangeByRankReturnsZeroForInvalidRanges: invalid rank ranges should remove nothing");
    skipList<int, int> values;

    EXPECT_EQ(values.eraseRangeByRank(1, 2), 0U);

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));

    EXPECT_EQ(values.eraseRangeByRank(0, 1), 0U);
    EXPECT_EQ(values.eraseRangeByRank(3, 2), 0U);
    EXPECT_EQ(values.eraseRangeByRank(4, 5), 0U);
    EXPECT_EQ(values.size(), 3U);
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {10, 100},
        {20, 200},
        {30, 300},
    }));
    spdlog::info("EraseRangeByRankReturnsZeroForInvalidRanges: invalid range checks passed");
}

TEST(SkipListTest, EraseRangeByRankRemovesMiddleRanks) {
    spdlog::info("EraseRangeByRankRemovesMiddleRanks: remove middle ranks and preserve spans");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(40, 400));
    ASSERT_TRUE(values.insert(50, 500));

    EXPECT_EQ(values.eraseRangeByRank(2, 4), 3U);

    EXPECT_EQ(values.size(), 2U);
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {10, 100},
        {50, 500},
    }));
    EXPECT_EQ(values.getRank(10, 100), 1U);
    EXPECT_EQ(values.getRank(50, 500), 2U);
    EXPECT_EQ(values.getRank(30, 300), 0U);
    EXPECT_EQ(values.getElementByRank(1), (std::optional<std::pair<int, int>>{{10, 100}}));
    EXPECT_EQ(values.getElementByRank(2), (std::optional<std::pair<int, int>>{{50, 500}}));
    EXPECT_FALSE(values.getElementByRank(3).has_value());
    spdlog::info("EraseRangeByRankRemovesMiddleRanks: middle rank removal verified");
}

TEST(SkipListTest, EraseRangeByRankRemovesHeadAndTailRanks) {
    spdlog::info("EraseRangeByRankRemovesHeadAndTailRanks: remove boundary ranks in sequence");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(40, 400));
    ASSERT_TRUE(values.insert(50, 500));

    EXPECT_EQ(values.eraseRangeByRank(1, 2), 2U);

    EXPECT_EQ(values.size(), 3U);
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {30, 300},
        {40, 400},
        {50, 500},
    }));
    EXPECT_EQ(values.getRank(30, 300), 1U);
    EXPECT_EQ(values.getElementByRank(1), (std::optional<std::pair<int, int>>{{30, 300}}));

    EXPECT_EQ(values.eraseRangeByRank(2, 3), 2U);

    EXPECT_EQ(values.size(), 1U);
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {30, 300},
    }));
    EXPECT_EQ(values.getRank(30, 300), 1U);
    EXPECT_EQ(values.getElementByRank(1), (std::optional<std::pair<int, int>>{{30, 300}}));
    EXPECT_FALSE(values.getElementByRank(2).has_value());
    spdlog::info("EraseRangeByRankRemovesHeadAndTailRanks: boundary rank removals verified");
}

TEST(SkipListTest, EraseRangeByRankClampsEndToSize) {
    spdlog::info("EraseRangeByRankClampsEndToSize: end rank beyond size should remove through tail");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));
    ASSERT_TRUE(values.insert(40, 400));

    EXPECT_EQ(values.eraseRangeByRank(3, 100), 2U);

    EXPECT_EQ(values.size(), 2U);
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {10, 100},
        {20, 200},
    }));
    EXPECT_EQ(values.getRank(10, 100), 1U);
    EXPECT_EQ(values.getRank(20, 200), 2U);
    EXPECT_FALSE(values.getElementByRank(3).has_value());
    spdlog::info("EraseRangeByRankClampsEndToSize: clamped rank removal verified");
}

TEST(SkipListTest, EraseRangeByRankRemovesWholeList) {
    spdlog::info("EraseRangeByRankRemovesWholeList: remove all ranks and allow reuse");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(20, 200));
    ASSERT_TRUE(values.insert(30, 300));

    EXPECT_EQ(values.eraseRangeByRank(1, 3), 3U);

    EXPECT_TRUE(values.empty());
    EXPECT_EQ(values.size(), 0U);
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{}));
    EXPECT_FALSE(values.getElementByRank(1).has_value());
    EXPECT_EQ(values.getRank(10, 100), 0U);

    EXPECT_TRUE(values.insert(40, 400));
    EXPECT_EQ(values.size(), 1U);
    EXPECT_EQ(values.getElementByRank(1), (std::optional<std::pair<int, int>>{{40, 400}}));
    spdlog::info("EraseRangeByRankRemovesWholeList: whole-list rank removal and reuse verified");
}

TEST(SkipListTest, EraseRangeByRankFollowsSameScoreValueOrder) {
    spdlog::info("EraseRangeByRankFollowsSameScoreValueOrder: rank deletion should use value order for same score");
    skipList<int, int> values;

    ASSERT_TRUE(values.insert(5, 50));
    ASSERT_TRUE(values.insert(10, 300));
    ASSERT_TRUE(values.insert(10, 100));
    ASSERT_TRUE(values.insert(10, 200));
    ASSERT_TRUE(values.insert(20, 400));

    EXPECT_EQ(values.eraseRangeByRank(2, 3), 2U);

    EXPECT_EQ(values.size(), 3U);
    EXPECT_EQ(snapshot(values), (std::vector<std::pair<int, int>>{
        {5, 50},
        {10, 300},
        {20, 400},
    }));
    EXPECT_FALSE(values.contains(10, 100));
    EXPECT_FALSE(values.contains(10, 200));
    EXPECT_TRUE(values.contains(10, 300));
    EXPECT_EQ(values.getRank(10, 300), 2U);
    EXPECT_EQ(values.getElementByRank(2), (std::optional<std::pair<int, int>>{{10, 300}}));
    spdlog::info("EraseRangeByRankFollowsSameScoreValueOrder: same-score rank removal verified");
}

TEST(SkipListTest, RandomizedOperationsMatchStdSetModel) {
    spdlog::info("RandomizedOperationsMatchStdSetModel: compare skipList against std::set model");
    skipList<int, int> values;
    std::set<std::pair<int, int>> model;
    std::mt19937 rng(0x5eed);
    std::uniform_int_distribution<int> operation_dist(0, 4);
    std::uniform_int_distribution<int> score_dist(-10, 10);
    std::uniform_int_distribution<int> value_dist(0, 20);
    std::uniform_int_distribution<int> inclusive_dist(0, 1);

    auto expectedSnapshot = [&model]() {
        return std::vector<std::pair<int, int>>(model.begin(), model.end());
    };

    auto eraseModelByScore = [&model](const ScoreRange<int>& range) {
        if (invalidRange(range)) {
            return std::size_t{0};
        }

        std::size_t removed = 0;
        for (auto it = model.begin(); it != model.end();) {
            if (containsScore(range, it->first)) {
                it = model.erase(it);
                ++removed;
            } else {
                ++it;
            }
        }
        return removed;
    };

    auto eraseModelByRank = [&model](std::size_t start, std::size_t end) {
        if (start == 0 || start > end || start > model.size()) {
            return std::size_t{0};
        }

        end = std::min(end, model.size());
        std::size_t removed = 0;
        auto it = model.begin();
        std::advance(it, static_cast<std::ptrdiff_t>(start - 1));
        while (it != model.end() && removed < end - start + 1) {
            it = model.erase(it);
            ++removed;
        }
        return removed;
    };

    auto expectedFirstInRange = [&model](const ScoreRange<int>& range) {
        if (invalidRange(range)) {
            return std::optional<std::pair<int, int>>{};
        }
        auto it = std::find_if(model.begin(), model.end(), [&range](const auto& item) {
            return containsScore(range, item.first);
        });
        if (it == model.end()) {
            return std::optional<std::pair<int, int>>{};
        }
        return std::optional<std::pair<int, int>>{*it};
    };

    auto expectedLastInRange = [&model](const ScoreRange<int>& range) {
        if (invalidRange(range)) {
            return std::optional<std::pair<int, int>>{};
        }
        for (auto it = model.rbegin(); it != model.rend(); ++it) {
            if (containsScore(range, it->first)) {
                return std::optional<std::pair<int, int>>{*it};
            }
        }
        return std::optional<std::pair<int, int>>{};
    };

    auto verify = [&]() {
        const auto expected = expectedSnapshot();
        EXPECT_EQ(values.size(), model.size());
        EXPECT_EQ(snapshot(values), expected);
        EXPECT_EQ(values.getElementByRank(0), std::nullopt);
        EXPECT_EQ(values.getElementByRank(model.size() + 1), std::nullopt);

        for (std::size_t i = 0; i < expected.size(); ++i) {
            const auto& item = expected[i];
            EXPECT_EQ(values.getElementByRank(i + 1), (std::optional<std::pair<int, int>>{item}));
            EXPECT_EQ(values.getRank(item.first, item.second), i + 1);
        }

        for (int attempt = 0; attempt < 3; ++attempt) {
            const int score = score_dist(rng);
            const int value = value_dist(rng);
            if (!model.contains({score, value})) {
                EXPECT_EQ(values.getRank(score, value), 0U);
            }
        }

        for (int attempt = 0; attempt < 3; ++attempt) {
            const int a = score_dist(rng);
            const int b = score_dist(rng);
            const auto range = scoreRange(std::min(a, b), std::max(a, b),
                                          inclusive_dist(rng) == 1, inclusive_dist(rng) == 1);
            EXPECT_EQ(values.firstInScoreRange(range), expectedFirstInRange(range));
            EXPECT_EQ(values.lastInScoreRange(range), expectedLastInRange(range));
        }
    };

    for (int step = 0; step < 400; ++step) {
        SCOPED_TRACE(step);
        const int operation = operation_dist(rng);
        if (operation == 0) {
            const int score = score_dist(rng);
            const int value = value_dist(rng);
            EXPECT_EQ(values.insert(score, value), model.insert({score, value}).second);
        } else if (operation == 1) {
            const int score = score_dist(rng);
            const int value = value_dist(rng);
            EXPECT_EQ(values.erase(score, value), model.erase({score, value}) == 1);
        } else if (operation == 2) {
            const int a = score_dist(rng);
            const int b = score_dist(rng);
            const auto range = scoreRange(a, b, inclusive_dist(rng) == 1, inclusive_dist(rng) == 1);
            EXPECT_EQ(values.eraseRangeByScore(range), eraseModelByScore(range));
        } else if (operation == 3) {
            const std::size_t start = static_cast<std::size_t>(std::uniform_int_distribution<int>(
                0, static_cast<int>(model.size()) + 3)(rng));
            const std::size_t end = static_cast<std::size_t>(std::uniform_int_distribution<int>(
                0, static_cast<int>(model.size()) + 5)(rng));
            EXPECT_EQ(values.eraseRangeByRank(start, end), eraseModelByRank(start, end));
        } else {
            const int a = score_dist(rng);
            const int b = score_dist(rng);
            const auto range = scoreRange(std::min(a, b), std::max(a, b),
                                          inclusive_dist(rng) == 1, inclusive_dist(rng) == 1);
            EXPECT_EQ(values.firstInScoreRange(range), expectedFirstInRange(range));
            EXPECT_EQ(values.lastInScoreRange(range), expectedLastInRange(range));
        }

        verify();
    }

    spdlog::info("RandomizedOperationsMatchStdSetModel: randomized model check verified");
}


TEST(SkipListTest, CopyOperationsRemainDisabled) {
    static_assert(!std::is_copy_constructible_v<skipList<int, int>>);
    static_assert(!std::is_copy_assignable_v<skipList<int, int>>);
    static_assert(!std::is_move_constructible_v<skipList<int,int>>);
    static_assert(!std::is_move_assignable_v<skipList<int,int>>);
}



int main(int argc, char** argv) {
    spdlog::set_level(spdlog::level::info);
    spdlog::set_pattern("[%H:%M:%S] [%^%l%$] %v");
    spdlog::info("starting GoogleTest suite for skip_list");

    ::testing::InitGoogleTest(&argc, argv);
    const int result = RUN_ALL_TESTS();

    spdlog::info("GoogleTest suite finished with code {}", result);
    return result;
}
