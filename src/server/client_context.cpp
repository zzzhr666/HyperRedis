#include "hyper/server/client_context.hpp"

#include "hyper/storage/redis_manager.hpp"


bool hyper::RedisClientContext::selectDb(const RedisManager& manager, std::size_t index) noexcept {
    if (auto res = manager.db(index)) {
        db_index_ = index;
        return true;
    }
    return false;
}

hyper::RedisDb* hyper::RedisClientContext::currentDb(RedisManager& manager) const noexcept {
    return manager.db(db_index_);
}

const hyper::RedisDb* hyper::RedisClientContext::currentDb(const RedisManager& manager) const noexcept {
    return manager.db(db_index_);
}
