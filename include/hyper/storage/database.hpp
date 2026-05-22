#pragma once


#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "hyper/datastructures/dict.hpp"
#include "hyper/storage/object.hpp"
#include "hyper/time.hpp"


namespace hyper {
    class RedisDb {
    public:
        enum class ExpireCondition {
            Always,
            NX,
            XX,
            GT,
            LT
        };

        RedisDb();

        RedisDb(const RedisDb&) = delete;
        RedisDb& operator=(const RedisDb&) = delete;

        void set(std::string key, RedisObjectPtr value);

        RedisObjectPtr get(std::string_view key, ExpireTimePoint now);
        bool del(std::string_view key);

        [[nodiscard]] bool exists(std::string_view key, ExpireTimePoint now);

        [[nodiscard]] std::size_t size() const {
            return main_dict_.size();
        }



        bool expireAt(std::string_view key, ExpireTimePoint now, ExpireTimePoint deadline, ExpireCondition condition = ExpireCondition::Always);

        bool expireAfter(std::string_view key, Milliseconds ttl, ExpireTimePoint now, ExpireCondition condition = ExpireCondition::Always);

        [[nodiscard]] std::optional<UnixMilliseconds> expireTime(std::string_view key,ExpireTimePoint now);

        [[nodiscard]] UnixMilliseconds pttl(std::string_view key, ExpireTimePoint now);

        [[nodiscard]] std::int64_t ttl(std::string_view key, ExpireTimePoint now);

        bool persist(std::string_view key, ExpireTimePoint now);

        std::size_t activeExpireCycle(ExpireTimePoint now,std::size_t max_checks);

        std::optional<ObjectType> type(std::string_view key, ExpireTimePoint now);

        bool rename(std::string_view old_key,std::string_view new_key,ExpireTimePoint now);

        void clear();

        std::optional<std::string> randomKey(ExpireTimePoint now);

        template<typename FUNC>
        requires std::invocable<FUNC,std::string_view,const RedisObjectPtr&>
        void forEach(ExpireTimePoint now,FUNC&& callback) {
            std::vector<std::string> keys;
            main_dict_.forEach([&keys](const std::string& key,const RedisObjectPtr&) {
                keys.push_back(key);
            });

            for (const auto& key : keys) {
                if (auto value = get(key,now)) {
                    callback(key,value);
                }
            }
        }

        ~RedisDb();

    private:
        bool expireIfNeeded_(std::string_view key, ExpireTimePoint now);

    private:
        dict<std::string, RedisObjectPtr, transparentStringHash, transparentStringEqual> main_dict_;
        dict<std::string, UnixMilliseconds, transparentStringHash, transparentStringEqual> expire_dict_;
    };
}
