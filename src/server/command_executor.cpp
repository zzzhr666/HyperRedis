#include "hyper/server/command_executor.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <charconv>
#include <cmath>
#include <limits>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>

#include "hyper/server/client_context.hpp"
#include "hyper/server/command_registry.hpp"
#include "hyper/storage/redis_manager.hpp"


namespace {

    std::optional<std::size_t> parseSize(std::string_view size_str) {
        if (size_str.empty() || size_str.front() == '-') {
            return std::nullopt;
        }
        std::size_t res{};
        const auto begin = size_str.data();
        const auto end = begin + size_str.size();
        if (auto [ptr , ec] = std::from_chars(begin, end, res); ptr == end && ec == std::errc()) {
            return res;
        }
        return std::nullopt;
    }

    std::optional<long> parseLong(std::string_view value) {
        long parsed{};
        const auto begin = value.data();
        const auto end = begin + value.size();
        if (value.empty()) {
            return std::nullopt;
        }
        auto [ptr, ec] = std::from_chars(begin, end, parsed);
        if (ptr == end && ec == std::errc()) {
            return parsed;
        }
        return std::nullopt;
    }

    std::optional<double> parseDouble(std::string_view value) {
        double parsed{};
        const auto begin = value.data();
        const auto end = begin + value.size();
        if (value.empty()) {
            return std::nullopt;
        }
        auto [ptr, ec] = std::from_chars(begin, end, parsed);
        if (ptr == end && ec == std::errc() && std::isfinite(parsed)) {
            return parsed;
        }
        return std::nullopt;
    }

    std::optional<hyper::Milliseconds> parseExpireDuration(std::string_view value,
                                                           std::int64_t milliseconds_per_input_unit) {
        std::int64_t parsed{};
        const auto begin = value.data();
        const auto end = begin + value.size();
        if (value.empty()) {
            return std::nullopt;
        }
        if (auto [ptr, ec] = std::from_chars(begin, end, parsed); ptr != end || ec != std::errc()) {
            return std::nullopt;
        }

        constexpr auto max = std::numeric_limits<hyper::Milliseconds::rep>::max();
        constexpr auto min = std::numeric_limits<hyper::Milliseconds::rep>::min();
        if (parsed > max / milliseconds_per_input_unit || parsed < min / milliseconds_per_input_unit) {
            return std::nullopt;
        }
        return hyper::Milliseconds{parsed * milliseconds_per_input_unit};
    }

    hyper::RespValue commandError(std::string_view message) {
        return hyper::respError(std::string(message));
    }

    std::string typeToString(hyper::ObjectType type) {
        switch (type) {
        case hyper::ObjectType::String:
            return "string";
        case hyper::ObjectType::List:
            return "list";
        case hyper::ObjectType::Set:
            return "set";
        case hyper::ObjectType::ZSet:
            return "zset";
        case hyper::ObjectType::Hash:
            return "hash";
        }
        assert(false);
        return {}; //NOLINT
    }

    std::optional<hyper::RedisDb::ExpireCondition> strToExpireCondition(std::string_view str) {
        std::string option{str};
        std::ranges::transform(option, option.begin(), [](unsigned char c) {
            return std::toupper(c);
        });
        if (option == "ALWAYS") {
            return hyper::RedisDb::ExpireCondition::Always;
        }
        if (option == "NX") {
            return hyper::RedisDb::ExpireCondition::NX;
        }
        if (option == "XX") {
            return hyper::RedisDb::ExpireCondition::XX;
        }
        if (option == "GT") {
            return hyper::RedisDb::ExpireCondition::GT;
        }
        if (option == "LT") {
            return hyper::RedisDb::ExpireCondition::LT;
        }
        return std::nullopt;
    }
}

hyper::RespValue hyper::CommandExecutor::execute(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    if (args.empty()) {
        return commandError(ErrEmptyCommand);
    }
    std::string command(args[0]);
    std::ranges::transform(command, command.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
    });
    auto res = findCommand(command);
    if (res == nullptr) {
        return commandError(ErrUnknownCommand);
    }
    std::size_t args_count = args.size();
    if (args_count < res->min_arity || args_count > res->max_arity) {
        return commandError(ErrWrongArity);
    }
    switch (res->command_name) {
    case CommandName::Append:
        return append_(manager, client, args, now);
    case CommandName::DbSize:
        return dbSize_(manager, client, now);
    case CommandName::Decr:
        return decr_(manager, client, args, now);
    case CommandName::Del:
        return del_(manager, client, args, now);
    case CommandName::Exists:
        return exists_(manager, client, args, now);
    case CommandName::FlushAll:
        return flushAll_(manager);
    case CommandName::FlushDb:
        return flushDb_(manager, client);
    case CommandName::Get:
        return get_(manager, client, args, now);
    case CommandName::GetRange:
        return getRange_(manager, client, args, now);
    case CommandName::HDel:
        return hDel_(manager, client, args, now);
    case CommandName::HGet:
        return hGet_(manager, client, args, now);
    case CommandName::HGetAll:
        return hGetAll_(manager, client, args, now);
    case CommandName::HLen:
        return hLen_(manager, client, args, now);
    case CommandName::HSet:
        return hSet_(manager, client, args, now);
    case CommandName::Incr:
        return incr_(manager, client, args, now);
    case CommandName::IncrBy:
        return incrBy_(manager, client, args, now);
    case CommandName::IncrByFloat:
        return incrByFloat_(manager, client, args, now);
    case CommandName::LLen:
        return lLen_(manager, client, args, now);
    case CommandName::LPop:
        return lPop_(manager, client, args, now);
    case CommandName::LPush:
        return lPush_(manager, client, args, now);
    case CommandName::LRange:
        return lRange_(manager, client, args, now);
    case CommandName::MGet:
        return mGet_(manager, client, args, now);
    case CommandName::MSet:
        return mSet_(manager, client, args);
    case CommandName::Ping:
        return ping_(args);
    case CommandName::RandomKey:
        return randomkey_(manager, client, now);
    case CommandName::Rename:
        return rename_(manager, client, args, now);
    case CommandName::RenameNx:
        return renameNx_(manager, client, args, now);
    case CommandName::RPop:
        return rPop_(manager, client, args, now);
    case CommandName::RPush:
        return rPush_(manager, client, args, now);
    case CommandName::SAdd:
        return sAdd_(manager, client, args, now);
    case CommandName::SCard:
        return sCard_(manager, client, args, now);
    case CommandName::Select:
        return select_(manager, client, args);
    case CommandName::Set:
        return set_(manager, client, args);
    case CommandName::SetRange:
        return setRange_(manager, client, args, now);
    case CommandName::SIsMember:
        return sIsMember_(manager, client, args, now);
    case CommandName::SMembers:
        return sMembers_(manager, client, args, now);
    case CommandName::SRem:
        return sRem_(manager, client, args, now);
    case CommandName::StrLen:
        return strLen_(manager, client, args, now);
    case CommandName::Type:
        return type_(manager, client, args, now);
    case CommandName::Ttl:
        return ttl_(manager, client, args, now);
    case CommandName::Pttl:
        return pTtl_(manager, client, args, now);
    case CommandName::Persist:
        return persist_(manager, client, args, now);
    case CommandName::Expire:
        return expire_(manager, client, args, now);
    case CommandName::PExpire:
        return pExpire_(manager, client, args, now);
    case CommandName::ZAdd:
        return zAdd_(manager, client, args, now);
    case CommandName::ZCard:
        return zCard_(manager, client, args, now);
    case CommandName::ZRange:
        return zRange_(manager, client, args, now);
    case CommandName::ZRem:
        return zRem_(manager, client, args, now);
    case CommandName::ZScore:
        return zScore_(manager, client, args, now);
    case CommandName::LIndex:
        return lIndex_(manager, client, args, now);
    case CommandName::LSet:
        return lSet_(manager, client, args, now);
    case CommandName::LInsert:
        return lInsert_(manager, client, args, now);
    case CommandName::LRem:
        return lRem_(manager, client, args, now);
    case CommandName::LTrim:
        return lTrim_(manager, client, args, now);
    case CommandName::HExists:
        return hExists_(manager, client, args, now);
    case CommandName::HKeys:
        return hKeys_(manager, client, args, now);
    case CommandName::HVals:
        return hVals_(manager, client, args, now);
    case CommandName::SPop:
        return sPop_(manager, client, args, now);
    case CommandName::SRandMember:
        return sRandMember_(manager, client, args, now);
    case CommandName::ZRank:
        return zRank_(manager, client, args, now);
    case CommandName::ZRevRank:
        return zRevRank_(manager, client, args, now);
    case CommandName::ZCount:
        return zCount_(manager, client, args, now);
    case CommandName::ZRevRange:
        return zRevRange_(manager, client, args, now);
    case CommandName::ZIncrBy:
        return zIncrBy_(manager, client, args, now);
    case CommandName::ZRemRangeByRank:
        return zRemRangeByRank_(manager, client, args, now);
    case CommandName::ZRemRangeByScore:
        return zRemRangeByScore_(manager, client, args, now);
    }
    return commandError(ErrUnknownCommand);
}

// =============================================================================
// --- Generic / Database / Connection ---
// =============================================================================

hyper::RespValue hyper::CommandExecutor::ping_(Args args) const {
    if (args.size() == 1) {
        return respPong();
    }
    return respBulk(std::string(args[1]));
}

hyper::RespValue hyper::CommandExecutor::select_(RedisManager& manager, RedisClientContext& client, Args args) const {
    auto res_op = parseSize(args[1]);
    if (!res_op.has_value()) {
        return commandError(ErrInvalidDbIndex);
    }
    if (client.selectDb(manager, res_op.value())) {
        return respOk();
    }
    return commandError(ErrInvalidDbIndex);
}

hyper::RespValue hyper::CommandExecutor::dbSize_(RedisManager& manager, RedisClientContext& client,
                                                 ExpireTimePoint now) const {
    auto current_db = client.currentDb(manager);
    assert(current_db != nullptr);
    return respInteger(static_cast<std::int64_t>(current_db->size()));
}

hyper::RespValue hyper::CommandExecutor::del_(RedisManager& manager, RedisClientContext& client, Args args,
                                              ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    std::size_t count{0};
    for (std::size_t i = 1; i < args.size(); ++i) {
        if (db->exists(args[i], now) && db->del(args[i])) {
            ++count;
        }
    }
    return respInteger(static_cast<std::int64_t>(count));
}

hyper::RespValue hyper::CommandExecutor::exists_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    std::size_t count{0};
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    for (std::size_t i = 1; i < args.size(); ++i) {
        if (db->exists(args[i], now)) {
            ++count;
        }
    }
    return respInteger(static_cast<std::int64_t>(count));
}

hyper::RespValue hyper::CommandExecutor::type_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    if (auto type_op = db->type(args[1], now); type_op.has_value()) {
        return RespSimpleString{typeToString(type_op.value())};
    }
    return RespSimpleString{"none"};
}

hyper::RespValue hyper::CommandExecutor::ttl_(RedisManager& manager, RedisClientContext& client, Args args,
                                              ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    return respInteger(db->ttl(args[1], now));
}

hyper::RespValue hyper::CommandExecutor::pTtl_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    return respInteger(db->pttl(args[1], now));
}

hyper::RespValue hyper::CommandExecutor::persist_(RedisManager& manager, RedisClientContext& client, Args args,
                                                  ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    return respInteger(db->persist(args[1], now) ? 1 : 0);
}

hyper::RespValue hyper::CommandExecutor::expire_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto condition = RedisDb::ExpireCondition::Always;
    if (args.size() == 4) {
        auto condition_opt = strToExpireCondition(args[3]);
        if (condition_opt.has_value()) {
            condition = condition_opt.value();
        } else {
            return commandError(ErrSyntaxError);
        }
    }
    auto milliseconds = parseExpireDuration(args[2], 1000);
    if (milliseconds.has_value()) {
        return respInteger(db->expireAfter(args[1], milliseconds.value(), now, condition) ? 1 : 0);
    }
    return commandError(ErrInvalidInteger);
}

hyper::RespValue hyper::CommandExecutor::pExpire_(RedisManager& manager, RedisClientContext& client, Args args,
                                                  ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto condition = RedisDb::ExpireCondition::Always;
    if (args.size() == 4) {
        auto condition_opt = strToExpireCondition(args[3]);
        if (condition_opt.has_value()) {
            condition = condition_opt.value();
        } else {
            return commandError(ErrSyntaxError);
        }
    }
    auto milliseconds = parseExpireDuration(args[2], 1);
    if (milliseconds.has_value()) {
        return respInteger(db->expireAfter(args[1], milliseconds.value(), now, condition) ? 1 : 0);
    }
    return commandError(ErrInvalidInteger);
}

hyper::RespValue hyper::CommandExecutor::flushAll_(RedisManager& manager) const {
    manager.clearAll();
    return respOk();
}

hyper::RespValue hyper::CommandExecutor::flushDb_(RedisManager& manager, RedisClientContext& client) const {
    if (manager.clearDb(client.dbIndex())) {
        return respOk();
    }
    return commandError(ErrInvalidDbIndex);
}

hyper::RespValue hyper::CommandExecutor::randomkey_(RedisManager& manager, RedisClientContext& client,
                                                    ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    if (auto key = db->randomKey(now); key.has_value()) {
        return respBulk(key.value());
    }
    return respNullBulk();
}

hyper::RespValue hyper::CommandExecutor::rename_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& old_key = args[1];
    auto& new_key = args[2];
    if (db->rename(old_key, new_key, now)) {
        return respOk();
    }
    return commandError(ErrNoSuchKey);
}

hyper::RespValue hyper::CommandExecutor::renameNx_(RedisManager& manager, RedisClientContext& client, Args args,
                                                   ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& old_key = args[1];
    auto& new_key = args[2];
    if (!db->exists(old_key, now)) {
        return commandError(ErrNoSuchKey);
    }
    if (db->exists(new_key, now)) {
        return respInteger(0);
    }

    db->rename(old_key, new_key, now);
    return respInteger(1);
}

// =============================================================================
// --- String ---
// =============================================================================

hyper::RespValue hyper::CommandExecutor::set_(RedisManager& manager, RedisClientContext& client, Args args) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    db->set(std::string(args[1]), RedisObject::createSharedStringObject(args[2]));
    return respOk();
}

hyper::RespValue hyper::CommandExecutor::get_(RedisManager& manager, RedisClientContext& client, Args args,
                                              ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto value = db->get(args[1], now);
    if (value == nullptr) {
        return respNullBulk();
    }
    if (value->getType() != ObjectType::String) {
        return commandError(ErrWrongType);
    }
    return respBulk(value->asString());
}

hyper::RespValue hyper::CommandExecutor::mGet_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto resp_array = std::make_shared<RespArray>();
    resp_array->values.reserve(args.size() - 1);
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    for (std::size_t i = 1; i < args.size(); ++i) {
        if (auto res = db->get(args[i], now); res != nullptr && res->getType() == ObjectType::String) {
            resp_array->values.emplace_back(respBulk(res->asString()));
        } else {
            resp_array->values.emplace_back(respNullBulk());
        }
    }
    return resp_array;
}

hyper::RespValue hyper::CommandExecutor::mSet_(RedisManager& manager, RedisClientContext& client, Args args) const {
    if (args.size() % 2 == 0) {
        return commandError(ErrWrongArity);
    }
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    for (std::size_t i = 1; i < args.size(); i += 2) {
        auto& key = args[i];
        auto& value = args[i + 1];
        db->set(std::string(key), RedisObject::createSharedStringObject(value));
    }
    return respOk();
}

hyper::RespValue hyper::CommandExecutor::strLen_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto res = db->get(std::string(args[1]), now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::String) {
        return commandError(ErrWrongType);
    }
    return respInteger(static_cast<std::int64_t>(res->asString().size()));
}

hyper::RespValue hyper::CommandExecutor::append_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto res = db->get(std::string(args[1]), now);
    if (res != nullptr) {
        res->append(args[2]);
        return respInteger(static_cast<std::int64_t>(res->asString().size()));
    }
    db->set(std::string(args[1]), RedisObject::createSharedStringObject(args[2]));
    return respInteger(static_cast<std::int64_t>(args[2].size()));
}

hyper::RespValue hyper::CommandExecutor::incr_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto& key = args[1];
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    long increment_v = 1;
    auto value = db->get(key, now);
    if (value == nullptr) {
        db->set(std::string(key), RedisObject::createSharedLongObject(increment_v));
        return respInteger(increment_v);
    }
    if (value->getType() != ObjectType::String) {
        return commandError(ErrWrongType);
    }
    auto updated = value->stringIncrBy(increment_v);
    if (updated.has_value()) {
        return respInteger(updated.value());
    }
    return commandError(ErrInvalidInteger);
}

hyper::RespValue hyper::CommandExecutor::decr_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto& key = args[1];
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    long increment_v = -1;
    auto value = db->get(key, now);
    if (value == nullptr) {
        db->set(std::string(key), RedisObject::createSharedLongObject(increment_v));
        return respInteger(increment_v);
    }
    if (value->getType() != ObjectType::String) {
        return commandError(ErrWrongType);
    }

    auto updated = value->stringIncrBy(increment_v);
    if (updated.has_value()) {
        return respInteger(updated.value());
    }
    return commandError(ErrInvalidInteger);
}

hyper::RespValue hyper::CommandExecutor::incrBy_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    auto& key = args[1];
    auto& increment_str = args[2];
    auto increment = parseLong(increment_str);
    if (!increment.has_value()) {
        return commandError(ErrInvalidInteger);
    }
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto increment_v = increment.value();
    auto value = db->get(key, now);
    if (value == nullptr) {
        db->set(std::string(key), RedisObject::createSharedLongObject(increment_v));
        return respInteger(increment_v);
    }
    if (value->getType() != ObjectType::String) {
        return commandError(ErrWrongType);
    }

    auto updated = value->stringIncrBy(increment_v);
    if (updated.has_value()) {
        return respInteger(updated.value());
    }
    return commandError(ErrInvalidInteger);
}

hyper::RespValue hyper::CommandExecutor::incrByFloat_(RedisManager& manager, RedisClientContext& client, Args args,
                                                      ExpireTimePoint now) const {
    auto& key = args[1];
    auto& increment_str = args[2];
    auto increment = parseDouble(increment_str);
    if (!increment.has_value()) {
        return commandError(ErrInvalidFloat);
    }
    auto increment_v = increment.value();
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto value = db->get(key, now);
    if (value == nullptr) {
        if (!std::isfinite(increment_v)) {
            return commandError(ErrFloatResultInvalid);
        }
        auto obj = RedisObject::createSharedStringObject("0");
        auto updated = obj->stringIncrByFloat(increment_v);
        if (updated.has_value()) {
            db->set(std::string(key), obj);
            return respBulk(obj->asString());
        }
        return commandError(ErrFloatResultInvalid);
    }
    if (value->getType() != ObjectType::String) {
        return commandError(ErrWrongType);
    }
    auto current = parseDouble(value->asString());
    if (!current.has_value()) {
        return commandError(ErrInvalidFloat);
    }
    if (!std::isfinite(current.value() + increment_v)) {
        return commandError(ErrFloatResultInvalid);
    }
    auto updated = value->stringIncrByFloat(increment_v);
    if (updated.has_value()) {
        return respBulk(value->asString());
    }
    return commandError(ErrFloatResultInvalid);
}

hyper::RespValue hyper::CommandExecutor::getRange_(RedisManager& manager, RedisClientContext& client, Args args,
                                                   ExpireTimePoint now) const {
    auto start = parseLong(args[2]);
    auto end = parseLong(args[3]);
    if (!(start.has_value() && end.has_value())) {
        return commandError(ErrInvalidInteger);
    }
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respBulk({});
    }
    if (res->getType() != ObjectType::String) {
        return commandError(ErrWrongType);
    }

    return respBulk(std::move(res->stringGetRange(static_cast<int>(start.value()), static_cast<int>(end.value()))));
}

hyper::RespValue hyper::CommandExecutor::setRange_(RedisManager& manager, RedisClientContext& client, Args args,
                                                   ExpireTimePoint now) const {
    auto offset_opt = parseSize(args[2]);
    if (!offset_opt.has_value()) {
        return commandError(ErrInvalidInteger);
    }
    std::size_t offset = offset_opt.value();
    auto& key = args[1];
    auto& value = args[3];
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto res = db->get(key, now);
    if (res == nullptr) {
        auto new_obj = RedisObject::createSharedStringObject({});
        db->set(std::string(key), new_obj);
        res = new_obj;
    } else if (res->getType() != ObjectType::String) {
        return commandError(ErrWrongType);
    }
    res->stringSetRange(offset, value);
    return respInteger(static_cast<std::int64_t>(res->stringLen()));
}

// =============================================================================
// --- List ---
// =============================================================================

hyper::RespValue hyper::CommandExecutor::lPush_(RedisManager& manager, RedisClientContext& client, Args args,
                                                ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        auto list_obj = RedisObject::createSharedListObject();
        db->set(std::string(key), list_obj);
        res = list_obj;
    } else if (res->getType() != ObjectType::List) {
        return commandError(ErrWrongType);
    }
    for (int i = 2; i < args.size(); ++i) {
        res->listLeftPush(RedisObject::createSharedStringObject(args[i]));
    }
    return respInteger(static_cast<std::int64_t>(res->listLen()));
}

hyper::RespValue hyper::CommandExecutor::rPush_(RedisManager& manager, RedisClientContext& client, Args args,
                                                ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        auto list_obj = RedisObject::createSharedListObject();
        db->set(std::string(key), list_obj);
        res = list_obj;
    } else if (res->getType() != ObjectType::List) {
        return commandError(ErrWrongType);
    }
    for (int i = 2; i < args.size(); ++i) {
        res->listRightPush(RedisObject::createSharedStringObject(args[i]));
    }
    return respInteger(static_cast<std::int64_t>(res->listLen()));
}

hyper::RespValue hyper::CommandExecutor::lPop_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respNullBulk();
    }
    if (res->getType() != ObjectType::List) {
        return commandError(ErrWrongType);
    }
    auto ret = res->listLeftPop();
    if (ret == nullptr) {
        return respNullBulk();
    }
    if (res->listLen() == 0) {
        db->del(key);
    }
    return respBulk(ret->asString());
}

hyper::RespValue hyper::CommandExecutor::rPop_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respNullBulk();
    }
    if (res->getType() != ObjectType::List) {
        return commandError(ErrWrongType);
    }
    auto ret = res->listRightPop();
    if (ret == nullptr) {
        return respNullBulk();
    }
    if (res->listLen() == 0) {
        db->del(key);
    }
    return respBulk(ret->asString());
}

hyper::RespValue hyper::CommandExecutor::lLen_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto res = db->get(args[1], now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::List) {
        return commandError(ErrWrongType);
    }
    return respInteger(static_cast<std::int64_t>(res->listLen()));
}

hyper::RespValue hyper::CommandExecutor::lRange_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    auto start = parseLong(args[2]);
    auto end = parseLong(args[3]);
    if (!start.has_value() || !end.has_value()) {
        return commandError(ErrInvalidInteger);
    }
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    auto ret = std::make_shared<RespArray>();
    if (res == nullptr) {
        return ret;
    }
    if (res->getType() != ObjectType::List) {
        return commandError(ErrWrongType);
    }
    auto arr = res->listRange(static_cast<int>(start.value()), static_cast<int>(end.value()));
    for (const auto& obj_ptr : arr) {
        ret->values.emplace_back(respBulk(obj_ptr->asString()));
    }
    return ret;
}

hyper::RespValue hyper::CommandExecutor::lIndex_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    auto index = parseLong(args[2]);
    if (!index.has_value() || index.value() > std::numeric_limits<int>::max() || index.value() < std::numeric_limits<
        int>::min()) {
        return commandError(ErrInvalidInteger);
    }
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto res = db->get(args[1], now);
    if (res == nullptr) {
        return respNullBulk();
    }
    if (res->getType() != ObjectType::List) {
        return commandError(ErrWrongType);
    }
    auto ret_opt = res->listIndexAsString(static_cast<int>(index.value()));
    if (!ret_opt.has_value()) {
        return respNullBulk();
    }
    return respBulk(ret_opt.value());
}

hyper::RespValue hyper::CommandExecutor::lSet_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto index = parseLong(args[2]);
    if (!index.has_value() || index.value() > std::numeric_limits<int>::max() || index.value() < std::numeric_limits<
        int>::min()) {
        return commandError(ErrInvalidInteger);
    }
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return commandError(ErrNoSuchKey);
    }
    if (res->getType() != ObjectType::List) {
        return commandError(ErrWrongType);
    }
    if (res->listSet(static_cast<int>(index.value()), RedisObject::createSharedStringObject(args[3]))) {
        return respOk();
    }
    return commandError(ErrIndexOutOfRange);
}

hyper::RespValue hyper::CommandExecutor::lInsert_(RedisManager& manager, RedisClientContext& client, Args args,
                                                  ExpireTimePoint now) const {
    std::string option(args[2]);
    std::ranges::transform(option, option.begin(), ::toupper);

    bool before = true;
    if (option != "BEFORE" && option != "AFTER") {
        return commandError(ErrSyntaxError);
    }
    if (option == "AFTER") {
        before = false;
    }
    auto& key = args[1];
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::List) {
        return commandError(ErrWrongType);
    }

    return res->listInsert(args[3], RedisObject::createSharedStringObject(args[4]), before).has_value()
               ? respInteger(static_cast<std::int64_t>(res->listLen()))
               : respInteger(-1);
}

hyper::RespValue hyper::CommandExecutor::lRem_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto count = parseLong(args[2]);
    if (!count.has_value() || count.value() > std::numeric_limits<int>::max() || count.value() < std::numeric_limits<
        int>::min()) {
        return commandError(ErrInvalidInteger);
    }
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto res = db->get(args[1], now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::List) {
        return commandError(ErrWrongType);
    }
    auto ret = res->listRemove(static_cast<int>(count.value()), args[3]);
    if (res->listLen() == 0) {
        db->del(args[1]);
    }
    return respInteger(static_cast<std::int64_t>(ret));
}

hyper::RespValue hyper::CommandExecutor::lTrim_(RedisManager& manager, RedisClientContext& client, Args args,
                                                ExpireTimePoint now) const {
    auto start = parseLong(args[2]);
    auto end = parseLong(args[3]);
    if (!start.has_value() || !end.has_value() ||
        start.value() > std::numeric_limits<int>::max() || start.value() < std::numeric_limits<int>::min() ||
        end.value() > std::numeric_limits<int>::max() || end.value() < std::numeric_limits<int>::min()) {
        return commandError(ErrInvalidInteger);
    }
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto res = db->get(args[1], now);
    if (res == nullptr) {
        return respOk();
    }
    if (res->getType() != ObjectType::List) {
        return commandError(ErrWrongType);
    }
    res->listTrim(static_cast<int>(start.value()), static_cast<int>(end.value()));
    if (res->listLen() == 0) {
        db->del(args[1]);
    }
    return respOk();
}

// =============================================================================
// --- Hash ---
// =============================================================================

hyper::RespValue hyper::CommandExecutor::hSet_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto& field = args[2];
    auto& value = args[3];
    auto res = db->get(key, now);
    if (res == nullptr) {
        auto hash_obj = RedisObject::createSharedHashObject();
        db->set(std::string(key), hash_obj);
        res = hash_obj;
    } else if (res->getType() != ObjectType::Hash) {
        return commandError(ErrWrongType);
    }
    std::int64_t ret = res->hashSet(std::string(field), RedisObject::createSharedStringObject(value)) ? 1 : 0;
    return respInteger(ret);
}

hyper::RespValue hyper::CommandExecutor::hGet_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto& field = args[2];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respNullBulk();
    }
    if (res->getType() != ObjectType::Hash) {
        return commandError(ErrWrongType);
    }
    auto value = res->hashGet(field);
    if (value == nullptr) {
        return respNullBulk();
    }
    return respBulk(value->asString());
}

hyper::RespValue hyper::CommandExecutor::hDel_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::Hash) {
        return commandError(ErrWrongType);
    }
    std::int64_t count{0};
    for (std::size_t i = 2; i < args.size(); ++i) {
        if (res->hashRemove(args[i])) {
            ++count;
        }
    }
    if (res->hashSize() == 0) {
        db->del(key);
    }
    return respInteger(count);
}

hyper::RespValue hyper::CommandExecutor::hLen_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::Hash) {
        return commandError(ErrWrongType);
    }
    return respInteger(static_cast<std::int64_t>(res->hashSize()));
}

hyper::RespValue hyper::CommandExecutor::hGetAll_(RedisManager& manager, RedisClientContext& client, Args args,
                                                  ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto key = args[1];
    auto res = db->get(key, now);
    auto arr = std::make_shared<RespArray>();
    if (res == nullptr) {
        return arr;
    }
    if (res->getType() != ObjectType::Hash) {
        return commandError(ErrWrongType);
    }
    auto all_data = res->hashGetAllAsStrings();
    for (auto& [field, value] : all_data) {
        arr->values.emplace_back(respBulk(std::move(field)));
        arr->values.emplace_back(respBulk(std::move(value)));
    }
    return arr;
}

hyper::RespValue hyper::CommandExecutor::hExists_(RedisManager& manager, RedisClientContext& client, Args args,
                                                  ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto res = db->get(args[1], now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::Hash) {
        return commandError(ErrWrongType);
    }
    return res->hashContains(args[2]) ? respInteger(1) : respInteger(0);
}

hyper::RespValue hyper::CommandExecutor::hKeys_(RedisManager& manager, RedisClientContext& client, Args args,
                                                ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto arr = std::make_shared<RespArray>();
    auto res = db->get(args[1], now);
    if (res == nullptr) {
        return arr;
    }
    if (res->getType() != ObjectType::Hash) {
        return commandError(ErrWrongType);
    }
    res->hashForEach([&arr](std::string_view k, const RedisObjectPtr&) {
        arr->values.emplace_back(respBulk(std::string(k)));
    });
    return arr;
}

hyper::RespValue hyper::CommandExecutor::hVals_(RedisManager& manager, RedisClientContext& client, Args args,
                                                ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto arr = std::make_shared<RespArray>();
    auto res = db->get(args[1], now);
    if (res == nullptr) {
        return arr;
    }
    if (res->getType() != ObjectType::Hash) {
        return commandError(ErrWrongType);
    }
    res->hashForEach([&arr](std::string_view, const RedisObjectPtr& v) {
        arr->values.emplace_back(respBulk(v->asString()));
    });
    return arr;
}

// =============================================================================
// --- Set ---
// =============================================================================

hyper::RespValue hyper::CommandExecutor::sAdd_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        auto new_set = RedisObject::createSharedSetObject();
        db->set(std::string(key), new_set);
        res = new_set;
    } else if (res->getType() != ObjectType::Set) {
        return commandError(ErrWrongType);
    }
    std::int64_t count{0};
    for (std::size_t i = 2; i < args.size(); ++i) {
        if (res->setAdd(args[i])) {
            ++count;
        }
    }
    return respInteger(count);
}

hyper::RespValue hyper::CommandExecutor::sRem_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::Set) {
        return commandError(ErrWrongType);
    }
    std::int64_t count{0};
    for (std::size_t i = 2; i < args.size(); ++i) {
        if (res->setRemove(args[i])) {
            ++count;
        }
    }
    if (res->setSize() == 0) {
        db->del(key);
    }
    return respInteger(count);
}

hyper::RespValue hyper::CommandExecutor::sIsMember_(RedisManager& manager, RedisClientContext& client, Args args,
                                                    ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::Set) {
        return commandError(ErrWrongType);
    }
    auto& member = args[2];
    return res->setContains(member) ? respInteger(1) : respInteger(0);
}

hyper::RespValue hyper::CommandExecutor::sCard_(RedisManager& manager, RedisClientContext& client, Args args,
                                                ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::Set) {
        return commandError(ErrWrongType);
    }
    return respInteger(static_cast<std::int64_t>(res->setSize()));
}

hyper::RespValue hyper::CommandExecutor::sMembers_(RedisManager& manager, RedisClientContext& client, Args args,
                                                   ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto key = args[1];
    auto arr = std::make_shared<RespArray>();
    auto res = db->get(key, now);
    if (res == nullptr) {
        return arr;
    }
    if (res->getType() != ObjectType::Set) {
        return commandError(ErrWrongType);
    }
    res->setForEach([&arr](std::string_view member) {
        arr->values.emplace_back(respBulk(std::string(member)));
    });
    return arr;
}

hyper::RespValue hyper::CommandExecutor::sPop_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto res = db->get(args[1], now);

    if (args.size() == 2) {
        if (res == nullptr) {
            return respNullBulk();
        }
        if (res->getType() != ObjectType::Set) {
            return commandError(ErrWrongType);
        }

        auto member = res->setPopString();
        if (!member.has_value()) {
            return respNullBulk();
        }
        if (res->setSize() == 0) {
            db->del(args[1]);
        }
        return respBulk(member.value());
    }

    auto count_opt = parseLong(args[2]);
    if (!count_opt.has_value()) {
        return commandError(ErrInvalidInteger);
    }
    if (count_opt.value() < 0) {
        return commandError(ErrUnpositiveValue);
    }

    auto arr = std::make_shared<RespArray>();
    auto count = count_opt.value();
    if (count == 0) {
        return arr;
    }
    if (res == nullptr) {
        return arr;
    }
    if (res->getType() != ObjectType::Set) {
        return commandError(ErrWrongType);
    }

    while (count-- > 0 && res->setSize() > 0) {
        auto member = res->setPopString();
        if (!member.has_value()) {
            break;
        }
        arr->values.emplace_back(respBulk(member.value()));
    }

    if (res->setSize() == 0) {
        db->del(args[1]);
    }
    return arr;
}

hyper::RespValue hyper::CommandExecutor::sRandMember_(RedisManager& manager, RedisClientContext& client, Args args,
                                                      ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto res = db->get(args[1], now);
    if (args.size() == 2) {
        if (res == nullptr) {
            return respNullBulk();
        }
        if (res->getType() != ObjectType::Set) {
            return commandError(ErrWrongType);
        }
        auto member = res->setRandomMemberString();
        if (!member.has_value()) {
            return respNullBulk();
        }
        return respBulk(member.value());
    }
    auto count_opt = parseLong(args[2]);
    if (!count_opt.has_value()) {
        return commandError(ErrInvalidInteger);
    }
    auto arr = std::make_shared<RespArray>();
    long count = count_opt.value();
    if (res == nullptr) {
        return arr;
    }
    if (res->getType() != ObjectType::Set) {
        return commandError(ErrWrongType);
    }
    if (count == 0) {
        return arr;
    }
    if (count > 0) {
        std::vector<std::string> members;
        members.reserve(res->setSize());
        res->setForEach([&members](std::string_view member) {
            members.emplace_back(member);
        });
        auto limit = std::min(static_cast<std::size_t>(count), members.size());
        for (std::size_t i = 0; i < limit; ++i) {
            arr->values.emplace_back(respBulk(std::move(members[i])));
        }
        return arr;
    }
    if (count == std::numeric_limits<long>::min()) {
        return commandError(ErrInvalidInteger);
    }
    const auto repeat_count = static_cast<std::size_t>(-count);
    for (std::size_t i = 0; i < repeat_count; ++i) {
        auto member = res->setRandomMemberString();
        if (!member.has_value()) {
            break;
        }
        arr->values.emplace_back(respBulk(member.value()));
    }
    return arr;
}

// =============================================================================
// --- ZSet ---
// =============================================================================

hyper::RespValue hyper::CommandExecutor::zAdd_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    if (args.size() % 2 != 0) {
        return commandError(ErrWrongArity);
    }
    std::vector<double> scores{};
    for (std::size_t i = 2; i < args.size(); i += 2) {
        auto score_opt = parseDouble(args[i]);
        if (!score_opt.has_value()) {
            return commandError(ErrInvalidFloat);
        }
        scores.emplace_back(score_opt.value());
    }

    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        auto new_zset = RedisObject::createSharedZSetObject();
        db->set(std::string(key), new_zset);
        res = new_zset;
    } else if (res->getType() != ObjectType::ZSet) {
        return commandError(ErrWrongType);
    }
    std::int64_t count{0};

    for (std::size_t i = 3; i < args.size(); i += 2) {
        if (res->zSetAdd(std::string(args[i]), scores[i / 2 - 1])) {
            ++count;
        }
    }
    return respInteger(count);
}

hyper::RespValue hyper::CommandExecutor::zRem_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::ZSet) {
        return commandError(ErrWrongType);
    }
    std::int64_t count{0};
    for (std::size_t i = 2; i < args.size(); ++i) {
        if (res->zSetRemove(args[i])) {
            ++count;
        }
    }
    if (res->zSetSize() == 0) {
        db->del(key);
    }
    return respInteger(count);
}

hyper::RespValue hyper::CommandExecutor::zScore_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respNullBulk();
    }
    if (res->getType() != ObjectType::ZSet) {
        return commandError(ErrWrongType);
    }
    auto& member = args[2];
    auto score_opt = res->zSetScore(member);
    if (score_opt.has_value()) {
        return respBulk(std::to_string(score_opt.value()));
    }
    return respNullBulk();
}

hyper::RespValue hyper::CommandExecutor::zCard_(RedisManager& manager, RedisClientContext& client, Args args,
                                                ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::ZSet) {
        return commandError(ErrWrongType);
    }
    return respInteger(static_cast<std::int64_t>(res->zSetSize()));
}

hyper::RespValue hyper::CommandExecutor::zRange_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    auto start = parseLong(args[2]);
    auto end = parseLong(args[3]);
    if (!start.has_value() || !end.has_value()) {
        return commandError(ErrInvalidInteger);
    }
    bool need_score = false;
    if (args.size() == 5) {
        std::string with_scores(args[4]);
        std::ranges::transform(with_scores, with_scores.begin(), ::toupper);
        if (with_scores != "WITHSCORES") {
            return commandError(ErrSyntaxError);
        }
        need_score = true;
    }
    auto& key = args[1];
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto arr = std::make_shared<RespArray>();
    auto res = db->get(key, now);
    if (res == nullptr) {
        return arr;
    }
    if (res->getType() != ObjectType::ZSet) {
        return commandError(ErrWrongType);
    }
    auto range_data = res->zSetRange(static_cast<int>(start.value()), static_cast<int>(end.value()));
    for (auto& [member, score] : range_data) {
        arr->values.emplace_back(respBulk(member));
        if (need_score) {
            arr->values.emplace_back(respBulk(std::to_string(score)));
        }
    }
    return arr;
}

hyper::RespValue hyper::CommandExecutor::zRank_(RedisManager& manager, RedisClientContext& client, Args args,
                                                ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respNullBulk();
    }
    if (res->getType() != ObjectType::ZSet) {
        return commandError(ErrWrongType);
    }
    auto& member = args[2];
    auto rank_opt = res->zSetRank(member);
    if (!rank_opt.has_value()) {
        return respNullBulk();
    }
    return respInteger(static_cast<std::int64_t>(rank_opt.value()));
}

hyper::RespValue hyper::CommandExecutor::zRevRank_(RedisManager& manager, RedisClientContext& client, Args args,
                                                   ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respNullBulk();
    }
    if (res->getType() != ObjectType::ZSet) {
        return commandError(ErrWrongType);
    }
    auto& member = args[2];
    auto rank_opt = res->zSetRevRank(member);
    if (!rank_opt.has_value()) {
        return respNullBulk();
    }
    return respInteger(static_cast<std::int64_t>(rank_opt.value()));
}

hyper::RespValue hyper::CommandExecutor::zCount_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    auto min = parseDouble(args[2]);
    auto max = parseDouble(args[3]);
    if (!min.has_value() || !max.has_value()) {
        return commandError(ErrInvalidFloat);
    }
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::ZSet) {
        return commandError(ErrWrongType);
    }
    auto count = res->zSetCount(min.value(), max.value());
    return respInteger(static_cast<std::int64_t>(count));
}

hyper::RespValue hyper::CommandExecutor::zRevRange_(RedisManager& manager, RedisClientContext& client, Args args,
                                                    ExpireTimePoint now) const {
    auto start = parseLong(args[2]);
    auto end = parseLong(args[3]);
    if (!start.has_value() || !end.has_value()) {
        return commandError(ErrInvalidInteger);
    }
    bool need_score = false;
    if (args.size() == 5) {
        std::string with_scores(args[4]);
        std::ranges::transform(with_scores, with_scores.begin(), ::toupper);
        if (with_scores != "WITHSCORES") {
            return commandError(ErrSyntaxError);
        }
        need_score = true;
    }
    auto& key = args[1];
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto arr = std::make_shared<RespArray>();
    auto res = db->get(key, now);
    if (res == nullptr) {
        return arr;
    }
    if (res->getType() != ObjectType::ZSet) {
        return commandError(ErrWrongType);
    }
    auto range_data = res->zSetRevRange(static_cast<int>(start.value()), static_cast<int>(end.value()));
    for (auto& [member, score] : range_data) {
        arr->values.emplace_back(respBulk(member));
        if (need_score) {
            arr->values.emplace_back(respBulk(std::to_string(score)));
        }
    }
    return arr;
}

hyper::RespValue hyper::CommandExecutor::zIncrBy_(RedisManager& manager, RedisClientContext& client, Args args,
                                                  ExpireTimePoint now) const {
    auto increment = parseDouble(args[2]);
    if (!increment.has_value()) {
        return commandError(ErrInvalidFloat);
    }
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto& key = args[1];
    auto res = db->get(key, now);
    if (res == nullptr) {
        auto new_zset = RedisObject::createSharedZSetObject();
        db->set(std::string(key), new_zset);
        res = new_zset;
    }
    if (res->getType() != ObjectType::ZSet) {
        return commandError(ErrWrongType);
    }
    auto& member = args[3];
    auto r = res->zSetIncrByChecked(std::string(member), increment.value());
    if (r.has_value()) {
        return respBulk(std::to_string(r.value()));
    }
    return commandError(ErrFloatResultInvalid);
}

hyper::RespValue hyper::CommandExecutor::zRemRangeByRank_(RedisManager& manager, RedisClientContext& client, Args args,
                                                          ExpireTimePoint now) const {
    auto start = parseLong(args[2]);
    auto end = parseLong(args[3]);
    if (!start.has_value() || !end.has_value() ||
        end.value() < std::numeric_limits<int>::min() || end.value() > std::numeric_limits<int>::max() ||
        start.value() < std::numeric_limits<int>::min() || start.value() > std::numeric_limits<int>::max()) {
        return commandError(ErrInvalidInteger);
    }
    auto& key = args[1];
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::ZSet) {
        return commandError(ErrWrongType);
    }

    auto count = res->zSetRemoveRangeByRank(static_cast<int>(start.value()), static_cast<int>(end.value()));
    if (res->zSetSize() == 0) {
        db->del(key);
    }
    return respInteger(static_cast<std::int64_t>(count));
}

hyper::RespValue hyper::CommandExecutor::zRemRangeByScore_(RedisManager& manager, RedisClientContext& client, Args args,
                                                           ExpireTimePoint now) const {
    auto min = parseDouble(args[2]);
    auto max = parseDouble(args[3]);
    if (!min.has_value() || !max.has_value()) {
        return commandError(ErrInvalidFloat);
    }
    auto& key = args[1];
    auto db = client.currentDb(manager);
    assert(db != nullptr);
    auto res = db->get(key, now);
    if (res == nullptr) {
        return respInteger(0);
    }
    if (res->getType() != ObjectType::ZSet) {
        return commandError(ErrWrongType);
    }

    auto count = res->zSetRemoveRangeByScore(min.value(), max.value());
    if (res->zSetSize() == 0) {
        db->del(key);
    }
    return respInteger(static_cast<std::int64_t>(count));
}
