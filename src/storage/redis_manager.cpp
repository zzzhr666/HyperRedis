#include "hyper/storage/redis_manager.hpp"
#include "hyper/storage/database.hpp"

#include <algorithm>

hyper::RedisManager::RedisManager(std::size_t db_count)
    : dbs_(std::max(db_count, static_cast<std::size_t>(1))) {
    for (auto& db : dbs_) {
        db = std::make_unique<RedisDb>();
    }
}

hyper::RedisManager::~RedisManager() = default;

hyper::RedisDb* hyper::RedisManager::db(std::size_t index) noexcept {
    return index >= dbs_.size() ? nullptr : dbs_[index].get();
}

const hyper::RedisDb* hyper::RedisManager::db(std::size_t index) const noexcept {
    return index >= dbs_.size() ? nullptr : dbs_[index].get();
}

bool hyper::RedisManager::clearDb(std::size_t index) {
    if (index >= dbs_.size()) {
        return false;
    }
    dbs_[index]->clear();
    return true;
}

void hyper::RedisManager::clearAll() {
    for (auto& db : dbs_) {
        db->clear();
    }
}

void hyper::RedisManager::swapAll(RedisManager& other) noexcept {
    dbs_.swap(other.dbs_);
}
