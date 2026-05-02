#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <variant>
#include <string_view>

namespace hyper {
    class intset;
    class ziplist;
    using RedisIntset = intset;
    using RedisZiplist = ziplist;

    struct RedisDict;
    struct RedisList;
    struct RedisSkipList;


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
        std::unique_ptr<RedisSkipList>,
        std::unique_ptr<RedisList>
    >;

    class RedisObject {

        struct Token {
            Token() = default;
        };
    public:
        static constexpr std::size_t ZipListMaxEntries = 16;//for test, original: 512
        static constexpr std::size_t ZipListMaxValue = 64;

        RedisObject(ObjectType type, ObjectEncoding encoding, ObjectData data,Token token = Token{});
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

        [[nodiscard]] ObjectType getType() const noexcept {
            return type_;
        }

        [[nodiscard]] ObjectEncoding getEncoding() const noexcept {
            return encoding_;
        }

        [[nodiscard]] std::string asString() const;

        void append(std::string_view str);

        bool hashSet(std::string field,std::shared_ptr<RedisObject> value) ;

        [[nodiscard]] std::shared_ptr<RedisObject> hashGet(const std::string& filed) const;

        void listLeftPush(const std::shared_ptr<RedisObject>& value);

        void listRightPush(const std::shared_ptr<RedisObject>& value);

        std::shared_ptr<RedisObject> listLeftPop();

        std::shared_ptr<RedisObject> listRightPop();

    private:
        void convertZiplistToList_();

        ObjectType type_;
        ObjectEncoding encoding_;
        ObjectData data_;
    };
} // namespace hyper
