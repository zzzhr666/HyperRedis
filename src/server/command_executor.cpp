#include "hyper/server/command_executor.hpp"

#include <algorithm>
#include <array>
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
#include "hyper/storage/redis_manager.hpp"


namespace {
    constexpr std::size_t CommandNumber = 49;
    constexpr std::string_view ErrEmptyCommand = "ERR empty command";
    constexpr std::string_view ErrUnknownCommand = "ERR unknown command";
    constexpr std::string_view ErrSyntaxError = "ERR syntax error";
    constexpr std::string_view ErrWrongArity = "ERR wrong number of arguments";
    constexpr std::string_view ErrInvalidDbIndex = "ERR invalid DB index";
    constexpr std::string_view ErrInvalidInteger = "ERR value is not an integer or out of range";
    constexpr std::string_view ErrInvalidFloat = "ERR value is not a valid float";
    constexpr std::string_view ErrFloatResultInvalid = "ERR increment would produce NaN or Infinity";
    constexpr std::string_view ErrNoSuchKey = "ERR no such key";
    constexpr std::string_view ErrWrongType = "WRONGTYPE Operation against a key holding the wrong kind of value";
    constexpr std::string_view ErrCommandNotImplemented = "ERR command not implemented";

    enum class CommandName : std::uint8_t {
        Append = 0,
        DbSize,
        Decr,
        Del,
        Exists,
        FlushAll,
        FlushDb,
        Get,
        GetRange,
        HDel,
        HGet,
        HGetAll,
        HLen,
        HSet,
        Incr,
        IncrBy,
        IncrByFloat,
        LLen,
        LPop,
        LPush,
        LRange,
        MGet,
        MSet,
        Ping,
        RandomKey,
        Rename,
        RenameNx,
        RPop,
        RPush,
        SAdd,
        SCard,
        Select,
        Set,
        SetRange,
        SIsMember,
        SMembers,
        SRem,
        StrLen,
        Type,
        Ttl,
        Pttl,
        Persist,
        Expire,
        PExpire,
        ZAdd,
        ZCard,
        ZRange,
        ZRem,
        ZScore
    };

    struct CommandSpec {
        std::string_view name;
        std::size_t min_arity;
        std::size_t max_arity;
        CommandName command_name;
        bool write; //NOLINT
    };

    constexpr std::size_t UnlimitedArity = std::numeric_limits<std::size_t>::max();

    constexpr auto makeRegistry() {
        std::array<CommandSpec, CommandNumber> registry{
            {
                {"APPEND", 3, 3, CommandName::Append, true},
                {"DBSIZE", 1, 1, CommandName::DbSize, false},
                {"DECR", 2, 2, CommandName::Decr, true},
                {"DEL", 2, UnlimitedArity, CommandName::Del, true},
                {"EXISTS", 2, UnlimitedArity, CommandName::Exists, false},
                {"EXPIRE", 3, 4, CommandName::Expire, true},
                {"FLUSHALL", 1, 1, CommandName::FlushAll, true},
                {"FLUSHDB", 1, 1, CommandName::FlushDb, true},
                {"GET", 2, 2, CommandName::Get, false},
                {"GETRANGE", 4, 4, CommandName::GetRange, false},
                {"HDEL", 3, UnlimitedArity, CommandName::HDel, true},
                {"HGET", 3, 3, CommandName::HGet, false},
                {"HGETALL", 2, 2, CommandName::HGetAll, false},
                {"HLEN", 2, 2, CommandName::HLen, false},
                {"HSET", 4, 4, CommandName::HSet, true},
                {"INCR", 2, 2, CommandName::Incr, true},
                {"INCRBY", 3, 3, CommandName::IncrBy, true},
                {"INCRBYFLOAT", 3, 3, CommandName::IncrByFloat, true},
                {"LLEN", 2, 2, CommandName::LLen, false},
                {"LPOP", 2, 2, CommandName::LPop, true},
                {"LPUSH", 3, UnlimitedArity, CommandName::LPush, true},
                {"LRANGE", 4, 4, CommandName::LRange, false},
                {"MGET", 2, UnlimitedArity, CommandName::MGet, false},
                {"MSET", 3, UnlimitedArity, CommandName::MSet, true},
                {"PEXPIRE", 3, 4, CommandName::PExpire, true},
                {"PERSIST", 2, 2, CommandName::Persist, true},
                {"PING", 1, 2, CommandName::Ping, false},
                {"PTTL", 2, 2, CommandName::Pttl, false},
                {"RANDOMKEY", 1, 1, CommandName::RandomKey, false},
                {"RENAME", 3, 3, CommandName::Rename, true},
                {"RENAMENX", 3, 3, CommandName::RenameNx, true},
                {"RPOP", 2, 2, CommandName::RPop, true},
                {"RPUSH", 3, UnlimitedArity, CommandName::RPush, true},
                {"SADD", 3, UnlimitedArity, CommandName::SAdd, true},
                {"SCARD", 2, 2, CommandName::SCard, false},
                {"SELECT", 2, 2, CommandName::Select, false},
                {"SET", 3, 3, CommandName::Set, true},
                {"SETRANGE", 4, 4, CommandName::SetRange, true},
                {"SISMEMBER", 3, 3, CommandName::SIsMember, false},
                {"SMEMBERS", 2, 2, CommandName::SMembers, false},
                {"SREM", 3, UnlimitedArity, CommandName::SRem, true},
                {"STRLEN", 2, 2, CommandName::StrLen, false},
                {"TTL", 2, 2, CommandName::Ttl, false},
                {"TYPE", 2, 2, CommandName::Type, false},
                {"ZADD", 4, UnlimitedArity, CommandName::ZAdd, true},
                {"ZCARD", 2, 2, CommandName::ZCard, false},
                {"ZRANGE", 4, 4, CommandName::ZRange, false},
                {"ZREM", 3, UnlimitedArity, CommandName::ZRem, true},
                {"ZSCORE", 3, 3, CommandName::ZScore, false}
            }
        };
        std::ranges::sort(registry, {}, &CommandSpec::name);

        return registry;
    }

    constexpr auto registry = makeRegistry();

    constexpr bool hasNoDuplicateCommand() {
        for (std::size_t i = 1; i < registry.size(); ++i) {
            if (registry[i - 1].name == registry[i].name) {
                return false;
            }
        }
        return true;
    }


    static_assert(hasNoDuplicateCommand());

    const CommandSpec* findCommand(std::string_view upper_case_command) {
        auto it = std::ranges::lower_bound(registry, upper_case_command, {}, &CommandSpec::name);
        if (it == registry.end() || it->name != upper_case_command) {
            return nullptr;
        }
        return &*it;
    }

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
    case CommandName::Decr:
        return decr_(manager, client, args, now);
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
    case CommandName::SIsMember:
        return sIsMember_(manager, client, args, now);
    case CommandName::SMembers:
        return sMembers_(manager, client, args, now);
    case CommandName::SRem:
        return sRem_(manager, client, args, now);
    case CommandName::StrLen:
        return strLen_(manager, client, args, now);
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
    case CommandName::DbSize:
        return dbSize_(manager, client, now);
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
    case CommandName::Ping:
        return ping_(args);
    case CommandName::RandomKey:
        return randomkey_(manager, client, now);
    case CommandName::Rename:
        return rename_(manager, client, args, now);
    case CommandName::Select:
        return select_(manager, client, args);
    case CommandName::Set:
        return set_(manager, client, args);
    case CommandName::SetRange:
        return setRange_(manager, client, args, now);
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
    }
    return commandError(ErrUnknownCommand);
}

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

hyper::RespValue hyper::CommandExecutor::dbSize_(RedisManager& manager, RedisClientContext& client,
                                                 ExpireTimePoint now) const {
    auto current_db = client.currentDb(manager);
    assert(current_db!=nullptr);
    return respInteger(static_cast<std::int64_t>(current_db->size()));
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
        db->set(std::string(key),new_obj);
        res = new_obj;
    } else if (res->getType() != ObjectType::String) {
        return commandError(ErrWrongType);
    }
    res->stringSetRange(offset,value);
    return respInteger(static_cast<std::int64_t>(res->stringLen()));
}

hyper::RespValue hyper::CommandExecutor::lPush_(RedisManager& manager, RedisClientContext& client, Args args,
                                                ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::rPush_(RedisManager& manager, RedisClientContext& client, Args args,
                                                ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::lPop_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::rPop_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::lLen_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::lRange_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::hSet_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::hGet_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::hDel_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::hLen_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::hGetAll_(RedisManager& manager, RedisClientContext& client, Args args,
                                                  ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::sAdd_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::sRem_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::sIsMember_(RedisManager& manager, RedisClientContext& client, Args args,
                                                    ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::sCard_(RedisManager& manager, RedisClientContext& client, Args args,
                                                ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::sMembers_(RedisManager& manager, RedisClientContext& client, Args args,
                                                   ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::zAdd_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::zRem_(RedisManager& manager, RedisClientContext& client, Args args,
                                               ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::zScore_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::zCard_(RedisManager& manager, RedisClientContext& client, Args args,
                                                ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}

hyper::RespValue hyper::CommandExecutor::zRange_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const {
    return commandError(ErrCommandNotImplemented);
}
