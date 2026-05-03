#pragma once
#include <algorithm>
#include <array>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace hyper {
    struct transparentStringHash {
        using is_transparent = void;

        [[nodiscard]] std::size_t operator()(std::string_view value) const noexcept {
            return std::hash<std::string_view>{}(value);
        }
    };

    struct transparentStringEqual {
        using is_transparent = void;

        [[nodiscard]] bool operator()(std::string_view lhs, std::string_view rhs) const noexcept {
            return lhs == rhs;
        }
    };

    template<typename K, typename V, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
    class dict {
    public:
        explicit dict(std::size_t size = 4)
            : rehash_index_(-1) {
            resetTable_(hash_tables_[0], normalizeBucketCount_(size));
            hash_tables_[1].table.clear();
            hash_tables_[1].used = 0;
            hash_tables_[1].mask = 0;
        }


        ~dict() {
            clear();
        }

        dict(const dict&) = delete;

        dict& operator=(const dict&) = delete;

        dict(dict&&) = delete;

        dict& operator=(dict&&) = delete;

        [[nodiscard]] std::size_t size() const noexcept {
            return hash_tables_[0].used + hash_tables_[1].used;
        }

        [[nodiscard]] bool empty() const noexcept {
            return size() == 0;
        }

        void clear() {
            for (auto& ht: hash_tables_) {
                for (auto& entry: ht.table) {
                    auto current = entry;
                    while (current) {
                        auto next = current->next;
                        delete current;
                        current = next;
                    }
                    entry = nullptr;
                }
                ht.used = 0;
            }

            hash_tables_[1].table.clear();
            hash_tables_[1].mask = 0;
            rehash_index_ = -1;
        }


        //不覆盖，若存在返回false
        bool insert(K k, V v) {
            if (isRehashing_()) {
                rehashStep_();
            } else if (needRehash_()) {
                startRehash_(hash_tables_[0].table.size() * 2);
            }
            if (find_(k)) {
                return false;
            }
            insertAtHead_(std::move(k), std::move(v));
            return true;
        }

        bool erase(const K& k) {
            return eraseImpl_(k);
        }

        template<typename Q>
            requires (!std::same_as<std::remove_cvref_t<Q>, K> &&
                      std::invocable<const Hash&, const Q&> &&
                      std::predicate<const KeyEqual&, const Q&, const K&> &&
                      std::predicate<const KeyEqual&, const K&, const Q&>)
        bool erase(const Q& k) {
            return eraseImpl_(k);
        }

        bool contains(const K& k) const {
            return find_(k) != nullptr;
        }

        template<typename Q>
            requires (!std::same_as<std::remove_cvref_t<Q>, K> &&
                      std::invocable<const Hash&, const Q&> &&
                      std::predicate<const KeyEqual&, const Q&, const K&> &&
                      std::predicate<const KeyEqual&, const K&, const Q&>)
        bool contains(const Q& k) const {
            return find_(k) != nullptr;
        }


        V* get(const K& k) noexcept {
            if (dictEntry* res = find_(k)) {
                return &res->value;
            }
            return nullptr;
        }

        template<typename Q>
            requires (!std::same_as<std::remove_cvref_t<Q>, K> &&
                      std::invocable<const Hash&, const Q&> &&
                      std::predicate<const KeyEqual&, const Q&, const K&> &&
                      std::predicate<const KeyEqual&, const K&, const Q&>)
        V* get(const Q& k) noexcept {
            if (dictEntry* res = find_(k)) {
                return &res->value;
            }
            return nullptr;
        }

        const V* get(const K& k) const noexcept {
            if (const dictEntry* res = find_(k)) {
                return &res->value;
            }
            return nullptr;
        }

        template<typename Q>
            requires (!std::same_as<std::remove_cvref_t<Q>, K> &&
                      std::invocable<const Hash&, const Q&> &&
                      std::predicate<const KeyEqual&, const Q&, const K&> &&
                      std::predicate<const KeyEqual&, const K&, const Q&>)
        const V* get(const Q& k) const noexcept {
            if (const dictEntry* res = find_(k)) {
                return &res->value;
            }
            return nullptr;
        }


        //修改-> false 插入-> true
        bool insertOrAssign(K k, V v) {
            if (isRehashing_()) {
                rehashStep_();
            } else if (needRehash_()) {
                startRehash_(hash_tables_[0].table.size() * 2);
            }

            if (dictEntry* it = find_(k)) {
                it->value = std::move(v);
                return false;
            }
            insertAtHead_(std::move(k), std::move(v));
            return true;
        }

        template<typename FUNCTION>
            requires std::invocable<FUNCTION,const K&,const V&>
        void forEach(FUNCTION&& f) const {
            forEachInTable_(hash_tables_[0],std::forward<FUNCTION>(f));
            if (isRehashing_()) {
                forEachInTable_(hash_tables_[1],std::forward<FUNCTION>(f));
            }
        }

        template<typename FUNCTION>
            requires std::invocable<FUNCTION,const K&,V&>
        void forEach(FUNCTION&& f) {
            forEachInTable_(hash_tables_[0],std::forward<FUNCTION>(f));
            if (isRehashing_()) {
                forEachInTable_(hash_tables_[1],std::forward<FUNCTION>(f));
            }
        }

    private:
        struct dictEntry {
            K key;
            V value;
            dictEntry* next;
            dictEntry(K k, V v, dictEntry* next = nullptr) : key(std::move(k)), value(std::move(v)), next(next) {}
        };

        struct dictHt {
            std::vector<dictEntry*> table;
            std::size_t mask{};
            std::size_t used{};
        };

        template<typename Q>
        bool eraseImpl_(const Q& k) {
            if (isRehashing_()) {
                rehashStep_();
            }

            int ht_num = isRehashing_() ? 2 : 1;
            for (int i = 0; i < ht_num; ++i) {
                if (hash_tables_[i].table.empty()) {
                    continue;
                }
                std::size_t bucket_index = getBucketIndex_(hash_tables_[i], k);
                dictEntry* current = hash_tables_[i].table[bucket_index];
                dictEntry* prev = nullptr;
                while (current) {
                    if (key_equal_(current->key, k)) {
                        if (prev == nullptr) {
                            hash_tables_[i].table[bucket_index] = current->next;
                        } else {
                            prev->next = current->next;
                        }

                        delete current;
                        --hash_tables_[i].used;
                        if (!isRehashing_() && needShrink_()) {
                            startRehash_(getShrinkTarget_());
                            rehashStep_();
                        }
                        return true;
                    }
                    prev = current;
                    current = current->next;
                }
            }
            return false;
        }

        void insertAtHead_(K k, V v) {
            dictHt& current_ht = isRehashing_() ? hash_tables_[1] : hash_tables_[0];
            std::size_t bucket_index = getBucketIndex_(current_ht, k);
            dictEntry* head = current_ht.table[bucket_index];
            auto new_entry = new dictEntry{std::move(k), std::move(v), head};
            current_ht.table[bucket_index] = new_entry;
            ++current_ht.used;
        }

        template<typename Q>
        dictEntry* find_(const Q& k) {
            if (auto res = findInTable_(hash_tables_[0], k)) {
                return res;
            }
            if (isRehashing_()) {
                return findInTable_(hash_tables_[1], k);
            }
            return nullptr;
        }

        template<typename Q>
        const dictEntry* find_(const Q& k) const {
            if (auto res = findInTable_(hash_tables_[0], k)) {
                return res;
            }
            if (isRehashing_()) {
                return findInTable_(hash_tables_[1], k);
            }
            return nullptr;
        }

        template<typename Q>
        dictEntry* findInTable_(dictHt& ht, const Q& k) noexcept {
            if (ht.table.empty()) {
                return nullptr;
            }
            auto index = getBucketIndex_(ht, k);
            auto entry = ht.table[index];
            while (entry) {
                if (key_equal_(k, entry->key)) {
                    return entry;
                }
                entry = entry->next;
            }

            return nullptr;
        }

        template<typename Q>
        const dictEntry* findInTable_(const dictHt& ht, const Q& k) const noexcept {
            if (ht.table.empty()) {
                return nullptr;
            }
            auto index = getBucketIndex_(ht, k);
            const dictEntry* entry = ht.table[index];
            while (entry) {
                if (key_equal_(k, entry->key)) {
                    return entry;
                }
                entry = entry->next;
            }

            return nullptr;
        }


        [[nodiscard]] bool isRehashing_() const noexcept {
            return rehash_index_ != -1;
        }

        template<typename Q>
        std::size_t getBucketIndex_(const dictHt& ht, const Q& k) const noexcept {
            assert(!ht.table.empty());
            return hash_(k) & ht.mask;
        }

        static std::size_t normalizeBucketCount_(std::size_t size) {
            if (size < 4) {
                size = 4;
            }
            std::size_t bucket_count = 1;
            while (bucket_count < size) {
                bucket_count <<= 1; //保证size是2的幂次
            }
            return bucket_count;
        }

        static void resetTable_(dictHt& ht, std::size_t bucket_count) {
            ht.table.clear();
            ht.table.resize(bucket_count, nullptr);
            ht.mask = bucket_count - 1;
            ht.used = 0;
        }

        void rehashStep_() {
            if (!isRehashing_()) {
                return;
            }
            auto& old_ht = hash_tables_[0];
            auto& new_ht = hash_tables_[1];
            while (rehash_index_ < old_ht.table.size() && old_ht.table[rehash_index_] == nullptr) {
                ++rehash_index_;
            }
            if (rehash_index_ == old_ht.table.size()) {
                rehash_index_ = -1;
                old_ht.table.clear();
                old_ht.mask = 0;
                old_ht.used = 0;
                std::swap(old_ht, new_ht);
                return;
            }
            dictEntry* to_do = old_ht.table[rehash_index_];
            while (to_do) {
                auto next = to_do->next;
                auto new_index = getBucketIndex_(new_ht, to_do->key);
                dictEntry* new_pos = new_ht.table[new_index];
                to_do->next = new_pos;
                hash_tables_[1].table[new_index] = to_do;
                to_do = next;
                ++new_ht.used;
                --old_ht.used;
            }

            old_ht.table[rehash_index_] = nullptr;
            ++rehash_index_;
            if (old_ht.used == 0) {
                old_ht.table.clear();
                old_ht.mask = 0;
                old_ht.used = 0;
                std::swap(old_ht, new_ht);
                rehash_index_ = -1;
            }
        }


        void startRehash_(std::size_t size) {
            assert(!isRehashing_());
            auto bucket_count = normalizeBucketCount_(size);
            resetTable_(hash_tables_[1], bucket_count);
            rehash_index_ = 0;
        }

        [[nodiscard]] bool needRehash_() const noexcept {
            return hash_tables_[0].used / static_cast<double>(hash_tables_[0].table.size()) >= ExpandFactor;
        }

        [[nodiscard]] bool needShrink_() const noexcept {
            return !isRehashing_() &&
                   hash_tables_[0].table.size() > MinBucketSize &&
                   hash_tables_[0].used / static_cast<double>(hash_tables_[0].table.size()) <= ShrinkFactor;
        }

        [[nodiscard]] std::size_t getShrinkTarget_() const noexcept {
            return normalizeBucketCount_(std::max(static_cast<std::size_t>(4), hash_tables_[0].used * 2));
        }

        template<typename FUNCTION>
            requires std::invocable<FUNCTION,const K&,const V&>
        void forEachInTable_(const dictHt& ht, FUNCTION&& fn) const {
            for (auto& bucket: ht.table) {
                if (bucket != nullptr) {
                    const dictEntry* cur = bucket;
                    while (cur) {
                        fn(cur->key, cur->value);
                        cur = cur->next;
                    }
                }
            }
        }

        template<typename FUNCTION>
            requires std::invocable<FUNCTION,const K&,V&>
        void forEachInTable_(dictHt& ht, FUNCTION&& fn) {
            for (auto& bucket: ht.table) {
                if (bucket != nullptr) {
                    dictEntry* cur = bucket;
                    while (cur) {
                        fn(cur->key, cur->value);
                        cur = cur->next;
                    }
                }
            }
        }

    private:
        static constexpr double ExpandFactor = 1;
        static constexpr std::size_t MinBucketSize = 4;
        static constexpr double ShrinkFactor = 0.25;

        std::array<dictHt,2> hash_tables_;
        std::ptrdiff_t rehash_index_;
        Hash hash_;
        KeyEqual key_equal_;
    };
}
