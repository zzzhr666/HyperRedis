#pragma once
#include <cstddef>


namespace hyper {
    class RedisManager;
    class RedisDb;

    class RedisClientContext {
    public:
        RedisClientContext() : db_index_(0) {}

        [[nodiscard]] std::size_t dbIndex() const noexcept {
            return db_index_;
        }

        bool selectDb(const RedisManager& manager, std::size_t index) noexcept;

        [[nodiscard]] RedisDb* currentDb(RedisManager& manager) const noexcept;

        [[nodiscard]] const RedisDb* currentDb(const RedisManager& manager) const noexcept;

    private:
        std::size_t db_index_;
    };
}
