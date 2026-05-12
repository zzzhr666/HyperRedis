#pragma once
#include <vector>
#include <cstddef>
#include <memory>

namespace hyper {
    class RedisDb;

    class RedisManager {
    public:
        static constexpr std::size_t DefaultDbCount = 16;

        explicit RedisManager(std::size_t db_count = DefaultDbCount);
        ~RedisManager();

        RedisManager(const RedisManager&) = delete;
        RedisManager& operator=(const RedisManager&) = delete;

        RedisManager(RedisManager&&) = delete;
        RedisManager& operator=(RedisManager&&) = delete;

        [[nodiscard]] std::size_t dbCount() const noexcept {
            return dbs_.size();
        }

        [[nodiscard]] RedisDb* db(std::size_t index) noexcept;

        [[nodiscard]] const RedisDb* db(std::size_t index) const noexcept;

        bool clearDb(std::size_t index);

        void clearAll();



    private:
        std::vector<std::unique_ptr<RedisDb>> dbs_;
    };
}
