#pragma once
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "hyper/datastructures/ziplist.hpp"

namespace hyper {
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

        static std::unique_ptr<RedisObject> createStringObject(std::string_view val);

        static std::unique_ptr<RedisObject> createLongObject(long val);

        static std::unique_ptr<RedisObject> createHashObject();

        static std::unique_ptr<RedisObject> createListObject();

        static std::unique_ptr<RedisObject> createSetObject();

        static std::unique_ptr<RedisObject> createZSetObject();

        [[nodiscard]] ObjectType getType() const noexcept {
            return type_;
        }

        [[nodiscard]] ObjectEncoding getEncoding() const noexcept {
            return encoding_;
        }

        // String object operations.
        [[nodiscard]] std::string asString() const;

        void append(std::string_view str);

        // Hash object operations.
        bool hashSet(std::string field, std::shared_ptr<RedisObject> value);

        [[nodiscard]] std::shared_ptr<RedisObject> hashGet(std::string_view field) const;

        bool hashRemove(std::string_view field);

        [[nodiscard]] std::size_t hashSize() const;

        [[nodiscard]] bool hashContains(std::string_view field) const;

        void hashForEach(const std::function<void(std::string_view, const std::shared_ptr<RedisObject>&)>& func) const;

        // List object operations.
        void listLeftPush(const std::shared_ptr<RedisObject>& value);

        void listRightPush(const std::shared_ptr<RedisObject>& value);

        std::shared_ptr<RedisObject> listLeftPop();

        std::shared_ptr<RedisObject> listRightPop();

        // Set object operations.
        bool setAdd(std::string_view member);

        bool setRemove(std::string_view member);

        [[nodiscard]] bool setContains(std::string_view member) const;

        [[nodiscard]] std::size_t setSize() const;

        void setForEach(const std::function<void(std::string_view)>& func) const;


        // ZSet Object operation
        bool zSetAdd(std::string member, double score);

        [[nodiscard]] std::optional<double> zSetScore(std::string_view member) const;

        bool zSetRemove(std::string_view member);

        [[nodiscard]] std::size_t zSetSize() const;

        // Encoding thresholds kept public for focused tests and learning checks.
        static constexpr std::size_t ZipListMaxEntries = 16; //for test, original: 512
        static constexpr std::size_t ZipListMaxValue = 64;
        static constexpr std::size_t SetMaxIntSetEntries = 16;
        static constexpr std::size_t ZSetZipListMaxEntries = 16;

    private:
        [[nodiscard]] static bool shouldConvertZiplistToList_(std::string_view value,
                                                              std::size_t next_size) noexcept;

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
