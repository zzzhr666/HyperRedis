#pragma once
#include <array>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <functional>
#include <optional>
#include <random>
#include <utility>
#include <vector>

namespace hyper {
    template <typename Score>
    struct ScoreRange {
        Score min;
        Score max;
        bool min_inclusive = true;
        bool max_inclusive = true;

        ScoreRange(Score min, Score max, bool min_inclusive = true, bool max_inclusive = true)
            : min(std::move(min)), max(std::move(max)), min_inclusive(min_inclusive), max_inclusive(max_inclusive) {}
    };

    template <typename Score, typename Value, typename ValueCompare = std::less<>>
        requires std::totally_ordered<Score>
    class skipList {
    private:
        struct skipListNode {
            struct skipListLevel {
                skipListNode* forward;
                std::size_t span;
                skipListLevel() : forward(nullptr), span(0) {}
            };

            explicit skipListNode(std::size_t level_count) : levels(level_count), backward(nullptr) {}

            skipListNode(std::size_t level_count, Score score, Value value)
                : levels(level_count), backward(nullptr), score(std::move(score)), value(std::move(value)) {}

            [[nodiscard]] bool isHeader() const noexcept {
                return !score.has_value();
            }

            std::vector<skipListLevel> levels;
            skipListNode* backward;
            std::optional<Score> score;
            std::optional<Value> value;
        };

    public:
        static constexpr std::size_t MaxLevel = 32;
        static constexpr double Probability = 0.25;
        skipList() : head_(new skipListNode(MaxLevel)), tail_(nullptr), length_(0), level_(1) {}


        skipList(skipList const&) = delete;
        skipList& operator=(skipList const&) = delete;
        skipList(skipList&&) = delete;
        skipList& operator=(skipList&&) = delete;

        [[nodiscard]] std::size_t size() const {
            return length_;
        }

        [[nodiscard]] bool empty() const {
            return length_ == 0;
        }

        bool insert(const Score& score, const Value& value) {
            std::array<skipListNode*, MaxLevel> update{};
            std::array<std::size_t, MaxLevel> rank{};
            findInsertPosition_(score, value, update, rank);
            if (skipListNode* next = update[0]->levels[0].forward; next && equal_(*next, score, value)) {
                return false;
            }
            std::size_t new_height = randomLevel_();
            if (new_height > level_) {
                for (std::size_t i = level_; i < new_height; ++i) {
                    update[i] = head_;
                    rank[i] = 0;
                    head_->levels[i].span = length_;
                }
                level_ = new_height;
            }
            auto new_node = new skipListNode(new_height, score, value);
            for (std::size_t i = 0; i < new_height; ++i) {
                new_node->levels[i].forward = update[i]->levels[i].forward;
                update[i]->levels[i].forward = new_node;
                new_node->levels[i].span = update[i]->levels[i].span - (rank[0] - rank[i]);
                update[i]->levels[i].span = rank[0] - rank[i] + 1;
            }
            for (std::size_t i = new_height; i < level_; ++i) {
                ++update[i]->levels[i].span;
            }
            new_node->backward = update[0] == head_ ? nullptr : update[0];
            if (skipListNode* next = new_node->levels[0].forward) {
                next->backward = new_node;
            } else {
                tail_ = new_node;
            }

            ++length_;
            return true;
        }

        template <typename V>
        bool erase(const Score& score, const V& value) {
            std::array<skipListNode*, MaxLevel> update{};
            std::array<std::size_t, MaxLevel> rank{};
            findInsertPosition_(score, value, update, rank);
            skipListNode* next = update[0]->levels[0].forward;
            if (next && equal_(*next, score, value)) {
                deleteNode_(next, update);
                return true;
            }
            return false;
        }

        std::size_t eraseRangeByScore(const ScoreRange<Score>& range) {
            return eraseRangeByScore(range, [](const Score&, const Value&) {});
        }

        template <typename OnErase>
            requires std::invocable<OnErase&, Score const&, Value const&>
        std::size_t eraseRangeByScore(const ScoreRange<Score>& range, OnErase&& on_erase) {
            if (!isInScoreRange(range)) {
                return 0;
            }
            std::array<skipListNode*, MaxLevel> update{};
            skipListNode* current = head_;
            for (int i = level_ - 1; i >= 0; --i) {
                while (current->levels[i].forward && !
                    greaterThanMin_(current->levels[i].forward->score.value(), range)) {
                    current = current->levels[i].forward;
                }
                update[i] = current;
            }
            current = update[0]->levels[0].forward;
            std::size_t res{0};
            while (current && lessThanMax_(current->score.value(), range)) {
                skipListNode* next = current->levels[0].forward;
                std::invoke(on_erase, current->score.value(), current->value.value());
                deleteNode_(current, update);
                current = next;
                ++res;
            }

            return res;
        }

        std::size_t eraseRangeByRank(std::size_t start, std::size_t end) {
            return eraseRangeByRank(start, end, [](const Score&, const Value&) {});
        }

        template <typename OnErase>
            requires std::invocable<OnErase&, Score const&, Value const&>
        std::size_t eraseRangeByRank(std::size_t start, std::size_t end, OnErase&& on_erase) {
            if (start == 0 || start > end || start > length_) {
                return 0;
            }
            if (end > length_) {
                end = length_;
            }
            std::array<skipListNode*, MaxLevel> update{};
            std::size_t current_index = 0;
            skipListNode* current = head_;
            for (int i = level_ - 1; i >= 0; --i) {
                while (current->levels[i].forward && current->levels[i].span + current_index < start) {
                    current_index += current->levels[i].span;
                    current = current->levels[i].forward;
                }
                update[i] = current;
            }

            current = update[0]->levels[0].forward;
            const std::size_t target_count = end - start + 1;
            std::size_t res{0};
            while (current && res < target_count) {
                skipListNode* next = current->levels[0].forward;
                std::invoke(on_erase, current->score.value(), current->value.value());
                deleteNode_(current, update);
                current = next;
                ++res;
            }

            return res;
        }

        template <typename V>
        bool contains(const Score& score, const V& value) const {
            std::array<skipListNode*, MaxLevel> update{};
            std::array<std::size_t, MaxLevel> rank{};
            findInsertPosition_(score, value, update, rank);
            skipListNode* next = update[0]->levels[0].forward;
            return next && equal_(*next, score, value);
        }

        void clear() {
            skipListNode* current = head_->levels[0].forward;
            while (current) {
                skipListNode* next = current->levels[0].forward;
                delete current;
                current = next;
            }
            tail_ = nullptr;
            length_ = 0;
            for (auto& level : head_->levels) {
                level.forward = nullptr;
                level.span = 0;
            }
            level_ = 1;
        }

        template <typename Pred>
            requires std::invocable<Pred&, Score const&, Value const&>
        void forEach(Pred&& pred) const {
            skipListNode* cur = head_->levels[0].forward;
            while (cur) {
                skipListNode* next = cur->levels[0].forward;
                pred(cur->score.value(), cur->value.value());
                cur = next;
            }
        }

        template <typename V>
        [[nodiscard]] std::size_t getRank(const Score& score, const V& value) const {
            std::size_t rank{0};
            skipListNode* current = head_;
            for (int i = level_ - 1; i >= 0; --i) {
                while (current->levels[i].forward && (less_(*current->levels[i].forward, score, value) || equal_(
                    *current->levels[i].forward, score, value))) {
                    rank += current->levels[i].span;
                    current = current->levels[i].forward;
                    if (current && equal_(*current, score, value)) {
                        return rank;
                    }
                }
            }

            return 0;
        }

        [[nodiscard]] std::optional<std::pair<Score, Value>> getElementByRank(std::size_t rank) const {
            if (rank == 0 || rank > length_) {
                return std::nullopt;
            }
            std::size_t current_index{0};
            skipListNode* current = head_;
            for (int i = level_ - 1; i >= 0; --i) {
                while (current->levels[i].forward && current_index + current->levels[i].span <= rank) {
                    current_index += current->levels[i].span;
                    current = current->levels[i].forward;
                }
            }
            if (current_index == rank) {
                return std::make_pair(current->score.value(), current->value.value());
            }

            return std::nullopt;
        }

        template <typename Pred>
            requires std::invocable<Pred&, Score const&, Value const&>
        void forEachByRank(std::size_t start_rank, std::size_t count, Pred&& pred) const {
            if (start_rank == 0 || start_rank > length_ || count == 0) {
                return;
            }

            const skipListNode* current = getNodeByRank(start_rank);
            for (std::size_t i = 0; i < count && current != nullptr; ++i) {
                pred(current->score.value(), current->value.value());
                current = current->levels[0].forward;
            }
        }

        template <typename Pred>
            requires std::invocable<Pred&, Score const&, Value const&>
        void forEachReverseByRank(std::size_t start_rank, std::size_t count, Pred&& pred) const {
            if (start_rank == 0 || start_rank > length_ || count == 0) {
                return;
            }

            const skipListNode* current = getNodeByRank(start_rank);
            for (std::size_t i = 0; i < count && current != nullptr; ++i) {
                pred(current->score.value(), current->value.value());
                current = current->backward;
            }
        }

        bool isInScoreRange(const ScoreRange<Score>& range) const {
            if (isInvalidRange_(range) || empty()) {
                return false;
            }
            if (!greaterThanMin_(tail_->score.value(), range)) {
                return false;
            }
            if (!lessThanMax_(head_->levels[0].forward->score.value(), range)) {
                return false;
            }
            return true;
        }

        [[nodiscard]] std::optional<std::pair<Score, Value>>
        firstInScoreRange(const ScoreRange<Score>& range) const {
            if (!isInScoreRange(range)) {
                return std::nullopt;
            }
            skipListNode* current = head_;
            for (int i = level_ - 1; i >= 0; --i) {
                while (current->levels[i].forward && !
                    greaterThanMin_(current->levels[i].forward->score.value(), range)) {
                    current = current->levels[i].forward;
                }
            }
            skipListNode* possible = current->levels[0].forward;
            if (possible && lessThanMax_(possible->score.value(), range)) {
                return std::make_pair(possible->score.value(), possible->value.value());
            }

            return std::nullopt;
        }


        [[nodiscard]] std::optional<std::pair<Score, Value>>
        lastInScoreRange(const ScoreRange<Score>& range) const {
            if (!isInScoreRange(range)) {
                return std::nullopt;
            }
            skipListNode* current = head_;
            for (int i = level_ - 1; i >= 0; --i) {
                while (current->levels[i].forward && lessThanMax_(current->levels[i].forward->score.value(), range)) {
                    current = current->levels[i].forward;
                }
            }
            if (current != head_ && greaterThanMin_(current->score.value(), range)) {
                return std::make_pair(current->score.value(), current->value.value());
            }

            return std::nullopt;
        }

        std::size_t getRankOfFirstInRange(const ScoreRange<Score> range) const {
            if (empty() || !isInScoreRange(range)) {
                return 0;
            }
            std::size_t rank{0};
            skipListNode* current = head_;
            for (int i = level_ - 1; i >= 0; --i) {
                while (current->levels[i].forward && !
                    greaterThanMin_(current->levels[i].forward->score.value(), range)) {
                    rank += current->levels[i].span;
                    current = current->levels[i].forward;
                }
            }
            skipListNode* first = current->levels[0].forward;
            if (first && lessThanMax_(first->score.value(), range)) {
                return rank + 1;
            }

            return 0;
        }

        std::size_t getRankOfLastInRange(const ScoreRange<Score> range) const {
            if (empty() || !isInScoreRange(range)) return 0;

            std::size_t rank = 0;
            skipListNode* current = head_;
            for (int i = level_ - 1; i >= 0; --i) {
                while (current->levels[i].forward &&
                    lessThanMax_(current->levels[i].forward->score.value(), range)) {
                    rank += current->levels[i].span;
                    current = current->levels[i].forward;
                }
            }


            if (current != head_ && greaterThanMin_(current->score.value(), range)) {
                return rank;
            }
            return 0;
        }

    private:
        const skipListNode* getNodeByRank(std::size_t rank) const {
            if (rank == 0 || rank > length_) {
                return nullptr;
            }

            std::size_t current_index{0};
            skipListNode* current = head_;
            for (int i = level_ - 1; i >= 0; --i) {
                while (current->levels[i].forward && current_index + current->levels[i].span <= rank) {
                    current_index += current->levels[i].span;
                    current = current->levels[i].forward;
                }
            }

            return current;
        }


    public:
        ~skipList() {
            clear();
            delete head_;
        }

    private:
        template <typename V>
        bool less_(skipListNode const& current, Score const& target_score, const V& target_value) const {
            assert(!current.isHeader());
            if (current.score.value() == target_score) {
                return value_compare_(current.value.value(), target_value);
            }
            return current.score.value() < target_score;
        }

        template <typename V>
        bool equal_(skipListNode const& current, Score const& target_score, const V& target_value) const {
            assert(!current.isHeader());
            return current.score.value() == target_score && !value_compare_(current.value.value(), target_value) && !
                value_compare_(target_value, current.value.value());
        }

        template <typename V>
        void findInsertPosition_(const Score& score, const V& value,
                                 std::array<skipListNode*, MaxLevel>& update,
                                 std::array<std::size_t, MaxLevel>& rank) const {
            skipListNode* current = head_;
            for (int i = level_ - 1; i >= 0; --i) {
                rank[i] = i == level_ - 1 ? 0 : rank[i + 1];
                while (current->levels[i].forward && less_(*current->levels[i].forward, score, value)) {
                    rank[i] += current->levels[i].span;
                    current = current->levels[i].forward;
                }
                update[i] = current;
            }
        }

        static std::size_t randomLevel_() {
            static std::random_device rd;
            static std::mt19937 gen(rd());
            static std::uniform_real_distribution<double> dis(0, 1);
            std::size_t height = 1;
            while (dis(gen) < Probability && height < MaxLevel) {
                ++height;
            }
            return height;
        }

        void deleteNode_(skipListNode* node, std::array<skipListNode*, MaxLevel>& update) {
            for (std::size_t i = 0; i < level_; ++i) {
                if (i < node->levels.size() && update[i]->levels[i].forward == node) {
                    update[i]->levels[i].forward = node->levels[i].forward;
                    update[i]->levels[i].span += node->levels[i].span - 1;
                } else {
                    --update[i]->levels[i].span;
                }
            }
            if (node->levels[0].forward) {
                node->levels[0].forward->backward = node->backward;
            } else {
                tail_ = node->backward;
            }

            delete node;
            --length_;
            while (level_ > 1 && head_->levels[level_ - 1].forward == nullptr) {
                --level_;
            }
        }

        static bool greaterThanMin_(const Score& score, const ScoreRange<Score>& range) {
            return range.min_inclusive ? score >= range.min : score > range.min;
        }

        static bool lessThanMax_(const Score& score, const ScoreRange<Score>& range) {
            return range.max_inclusive ? score <= range.max : score < range.max;
        }

        static bool inRange_(const Score& score, const ScoreRange<Score>& range) {
            return greaterThanMin_(score, range) && lessThanMax_(score, range);
        }

        static bool isInvalidRange_(const ScoreRange<Score>& range) {
            if (range.min > range.max) {
                return true;
            }
            if (range.min == range.max && !(range.max_inclusive && range.min_inclusive)) {
                return true;
            }

            return false;
        }

    private:
        skipListNode* head_;
        skipListNode* tail_;
        std::size_t length_;
        std::size_t level_;
        ValueCompare value_compare_;
    };
}
