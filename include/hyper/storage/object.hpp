#pragma once
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "hyper/datastructures/ziplist.hpp"

namespace hyper {
    class RedisObject;
    using RedisObjectPtr = std::shared_ptr<RedisObject>;
    using RedisObjectUPtr = std::unique_ptr<RedisObject>;

    class intset;
    class ziplist;
    using RedisIntset = intset;
    using RedisZiplist = ziplist;
    class ziplistEntryView;

    struct RedisDict;
    struct RedisList;
    struct RedisZSet;
    struct RedisSet;


    enum class ObjectType : std::uint8_t {
        String = 0,
        List,
        Set,
        ZSet,
        Hash
    };

    enum class ObjectEncoding : std::uint8_t {
        Raw = 0,
        Int,
        HashTable,
        ZipList,
        IntSet,
        SkipList,
        LinkedList
    };

    /**
     * @brief Redis 对象数据变体
     * 使用统一的 Redis* 命名风格
     */
    using ObjectData = std::variant<
        long,
        std::string,
        std::unique_ptr<RedisDict>,
        std::unique_ptr<RedisIntset>,
        std::unique_ptr<RedisZiplist>,
        std::unique_ptr<RedisZSet>,
        std::unique_ptr<RedisList>,
        std::unique_ptr<RedisSet>
    >;

    enum class ZSetAddStatus : std::uint8_t {
        Added,
        Updated,
        Unchanged,
        InvalidScore
    };

    class RedisObject {
        class Token {
            Token() = default;
            friend class RedisObject;
        };

    public:
        RedisObject(ObjectType type, ObjectEncoding encoding, ObjectData data, Token token);
        // 禁止拷贝，支持移动
        RedisObject(const RedisObject&) = delete;
        RedisObject& operator=(const RedisObject&) = delete;
        RedisObject(RedisObject&&) noexcept = default;
        RedisObject& operator=(RedisObject&&) noexcept = default;

        ~RedisObject();

        static RedisObjectUPtr createUniqueStringObject(std::string_view val);

        static RedisObjectUPtr createUniqueLongObject(long val);

        static RedisObjectUPtr createUniqueHashObject();

        static RedisObjectUPtr createUniqueListObject();

        static RedisObjectUPtr createUniqueSetObject();

        static RedisObjectUPtr createUniqueZSetObject();

        static RedisObjectPtr createSharedStringObject(std::string_view val);

        static RedisObjectPtr createSharedLongObject(long val);

        static RedisObjectPtr createSharedHashObject();

        static RedisObjectPtr createSharedListObject();

        static RedisObjectPtr createSharedSetObject();

        static RedisObjectPtr createSharedZSetObject();

        [[nodiscard]] ObjectType getType() const noexcept {
            return type_;
        }

        [[nodiscard]] ObjectEncoding getEncoding() const noexcept {
            return encoding_;
        }

        // String object operations.
        [[nodiscard]] std::string asString() const;

        void append(std::string_view str);

        [[nodiscard]] std::size_t stringLen() const;

        std::optional<long> stringIncrBy(long increment);

        std::optional<double> stringIncrByFloat(double increment);

        [[nodiscard]] std::string stringGetRange(int start, int end) const;

        void stringSetRange(std::size_t offset, std::string_view value);


        // Hash object operations.
        bool hashSet(std::string field, RedisObjectPtr value);

        [[nodiscard]] RedisObjectPtr hashGet(std::string_view field) const;

        bool hashRemove(std::string_view field);

        [[nodiscard]] std::size_t hashSize() const;

        [[nodiscard]] bool hashContains(std::string_view field) const;

        void hashForEach(const std::function<void(std::string_view, const RedisObjectPtr&)>& func) const;

        [[nodiscard]] std::vector<std::string> hashKeys() const;
        [[nodiscard]] std::vector<RedisObjectPtr> hashValues() const;
        [[nodiscard]] std::vector<std::pair<std::string, RedisObjectPtr>> hashGetAll() const;
        [[nodiscard]] std::vector<std::string> hashValuesAsStrings() const;
        [[nodiscard]] std::vector<std::pair<std::string, std::string>> hashGetAllAsStrings() const;


        // List object operations.
        void listLeftPush(const RedisObjectPtr& value);

        void listRightPush(const RedisObjectPtr& value);

        RedisObjectPtr listLeftPop();

        RedisObjectPtr listRightPop();

        [[nodiscard]] std::size_t listLen() const;

        [[nodiscard]] RedisObjectPtr listIndex(int index) const;

        [[nodiscard]] std::optional<std::string> listIndexAsString(int index) const;

        bool listSet(int index, const RedisObjectPtr& value);

        std::optional<std::size_t> listInsert(std::string_view pivot, const RedisObjectPtr& value,
                                              bool before);

        std::size_t listRemove(int count, std::string_view value);

        void listTrim(int start, int end);

        std::vector<RedisObjectPtr> listRange(int start, int end) const;
        std::vector<std::string> listRangeAsStrings(int start, int end) const;

        // Set object operations.
        bool setAdd(std::string_view member);

        bool setRemove(std::string_view member);

        [[nodiscard]] bool setContains(std::string_view member) const;

        [[nodiscard]] std::size_t setSize() const;

        void setForEach(const std::function<void(std::string_view)>& func) const;

        [[nodiscard]] RedisObjectPtr setRandomMember() const;

        [[nodiscard]] std::optional<std::string> setRandomMemberString() const;

        RedisObjectPtr setPop();

        std::optional<std::string> setPopString();

        // ZSet Object operation
        bool zSetAdd(std::string member, double score);

        ZSetAddStatus zSetAddDetailed(std::string member, double score);

        [[nodiscard]] std::optional<double> zSetScore(std::string_view member) const;

        bool zSetRemove(std::string_view member);

        [[nodiscard]] std::size_t zSetSize() const;

        [[nodiscard]] std::optional<std::size_t> zSetRank(std::string_view member) const;

        [[nodiscard]] std::optional<std::size_t> zSetRevRank(std::string_view member) const;

        [[nodiscard]] std::size_t zSetCount(double min, double max) const;

        using ZSetRangeResult = std::vector<std::pair<std::string, double>>;

        [[nodiscard]] ZSetRangeResult zSetRange(int start, int end) const;

        [[nodiscard]] ZSetRangeResult zSetRevRange(int start, int end) const;

        double zSetIncrBy(std::string member, double increment);
        std::optional<double> zSetIncrByChecked(std::string member, double increment);
        std::size_t zSetRemoveRangeByRank(int start, int end);
        std::size_t zSetRemoveRangeByScore(double min, double max);

        // Encoding thresholds kept public for focused tests and learning checks.
        static constexpr std::size_t ZipListMaxEntries = 16; //for test, original: 512
        static constexpr std::size_t ZipListMaxValue = 64;
        static constexpr std::size_t HashZipListMaxEntries = 16;
        static constexpr std::size_t SetMaxIntSetEntries = 16;
        static constexpr std::size_t ZSetZipListMaxEntries = 16;

    private:
        [[nodiscard]] static bool shouldConvertZiplistToList_(std::string_view value,
                                                              std::size_t next_size) noexcept;


        void convertHashZipListToDict_();

        void convertZiplistToList_();

        void convertIntSetToSet_();

        void convertZSetToSkiplist_();

        [[nodiscard]] static double parseScore_(const ziplistEntryView& entry_view);
        [[nodiscard]] static std::string getEntryAsString_(const ziplistEntryView& entry);
        [[nodiscard]] static std::string formatScore_(double score);

        static std::optional<long> parseSetIntegerMember_(std::string_view member);

        ObjectType type_;
        ObjectEncoding encoding_;
        ObjectData data_;
    };
} // namespace hyper
