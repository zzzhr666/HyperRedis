#include "hyper/storage/database.hpp"
#include <cassert>
#include <limits>

namespace {
    hyper::UnixMilliseconds toUnixMilliseconds(hyper::ExpireTimePoint time) {
        return std::chrono::duration_cast<hyper::Milliseconds>(time.time_since_epoch()).count();
    }

    hyper::ExpireTimePoint fromUnixMilliseconds(hyper::UnixMilliseconds ms) {
        return hyper::ExpireTimePoint(hyper::Milliseconds(ms));
    }
}

hyper::RedisDb::RedisDb() = default;

void hyper::RedisDb::set(std::string key, RedisObjectPtr value) {
    assert(value != nullptr);
    expire_dict_.erase(key);
    main_dict_.insertOrAssign(std::move(key), std::move(value));
}

hyper::RedisObjectPtr hyper::RedisDb::get(std::string_view key, ExpireTimePoint now) {
    expireIfNeeded_(key, now);
    if (const auto res = main_dict_.get(key)) {
        return *res;
    }
    return nullptr;
}

bool hyper::RedisDb::del(std::string_view key) {
    expire_dict_.erase(key);
    return main_dict_.erase(key);
}

bool hyper::RedisDb::exists(std::string_view key, ExpireTimePoint now) {
    expireIfNeeded_(key, now);
    return main_dict_.contains(key);
}

bool hyper::RedisDb::expireAt(std::string_view key, ExpireTimePoint now, ExpireTimePoint deadline,
                              ExpireCondition condition) {
    expireIfNeeded_(key, now);
    if (!main_dict_.contains(key)) {
        return false;
    }

    // case: key存在

    if (condition == ExpireCondition::Always) {
        expire_dict_.insertOrAssign(std::string(key), toUnixMilliseconds(deadline));
        return true;
    }
    if (condition == ExpireCondition::NX) {
        if (expire_dict_.contains(key)) {
            return false;
        }
        expire_dict_.insert(std::string(key), toUnixMilliseconds(deadline));
        return true;
    }
    if (condition == ExpireCondition::XX) {
        if (!expire_dict_.contains(key)) {
            return false;
        }
        expire_dict_.insertOrAssign(std::string(key), toUnixMilliseconds(deadline));
        return true;
    }
    if (condition == ExpireCondition::GT || condition == ExpireCondition::LT) {
        UnixMilliseconds original = std::numeric_limits<UnixMilliseconds>::max();
        if (auto res = expire_dict_.get(key); res != nullptr) {
            original = *res;
        }
        UnixMilliseconds new_deadline = toUnixMilliseconds(deadline);
        if (condition == ExpireCondition::GT && original < new_deadline) {
            expire_dict_.insertOrAssign(std::string(key), new_deadline);
            return true;
        }
        if (condition == ExpireCondition::LT && original > new_deadline) {
            expire_dict_.insertOrAssign(std::string(key), new_deadline);
            return true;
        }
        return false;
    }
    assert(false);
    return false;
}

bool hyper::RedisDb::expireAfter(std::string_view key, Milliseconds ttl, ExpireTimePoint now,
                                 ExpireCondition condition) {
    return expireAt(key, now, now + ttl, condition);
}

hyper::UnixMilliseconds hyper::RedisDb::pttl(std::string_view key, ExpireTimePoint now) {
    expireIfNeeded_(key, now);
    if (!main_dict_.contains(key)) {
        return -2;
    }
    if (auto res = expire_dict_.get(key)) {
        return *res - toUnixMilliseconds(now);
    }
    return -1;
}

std::int64_t hyper::RedisDb::ttl(std::string_view key, ExpireTimePoint now) {
    auto res = pttl(key, now);
    if (res < 0) {
        return res;
    }
    return (res + 500) / 1000;
}

bool hyper::RedisDb::persist(std::string_view key, ExpireTimePoint now) {
    expireIfNeeded_(key, now);
    if (!main_dict_.contains(key)) {
        return false;
    }
    if (!expire_dict_.contains(key)) {
        return false;
    }

    return expire_dict_.erase(key);
}

std::size_t hyper::RedisDb::activeExpireCycle(ExpireTimePoint now, std::size_t max_checks) {
    std::size_t deleted{0};
    for (std::size_t i = 0; i < max_checks && !expire_dict_.empty(); ++i) {
        if (auto key = expire_dict_.getRandomKey()) {
            std::string key_copy = *key;
            if (expireIfNeeded_(key_copy, now)) {
                ++deleted;
            }
        } else {
            break;
        }
    }
    return deleted;
}

std::optional<hyper::ObjectType> hyper::RedisDb::type(std::string_view key, ExpireTimePoint now) {
    expireIfNeeded_(key, now);
    if (auto res = main_dict_.get(key)) {
        return (*res)->getType();
    }
    return std::nullopt;
}

bool hyper::RedisDb::rename(std::string_view old_key, std::string_view new_key, ExpireTimePoint now) {
    expireIfNeeded_(old_key, now);

    if (auto old = main_dict_.get(old_key)) {
        if (old_key == new_key) {
            return true;
        }
        auto value = std::move(*old);
        std::string key(new_key);

        if (auto exp = expire_dict_.get(old_key)) {
            expire_dict_.insertOrAssign(key, *exp);
            expire_dict_.erase(old_key);
        } else {
            expire_dict_.erase(key);
        }

        main_dict_.erase(old_key);
        main_dict_.insertOrAssign(std::move(key), value);
        return true;
    }
    return false;
}


hyper::RedisDb::~RedisDb() = default;

bool hyper::RedisDb::expireIfNeeded_(std::string_view key, ExpireTimePoint now) {
    const auto now_unix = toUnixMilliseconds(now);
    if (UnixMilliseconds* res = expire_dict_.get(key)) {
        if (now_unix >= *res) {
            del(key);
            return true;
        }
    }
    return false;
}
