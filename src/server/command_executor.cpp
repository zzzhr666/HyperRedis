#include "hyper/server/command_executor.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <ranges>
#include <string_view>
#include <cassert>
#include <cctype>
#include <string>
#include <charconv>
#include <optional>

#include "hyper/server/client_context.hpp"
#include "hyper/storage/redis_manager.hpp"


namespace {
    constexpr std::size_t CommandNumber = 12;
    constexpr std::string_view ErrEmptyCommand = "ERR empty command";
    constexpr std::string_view ErrUnknownCommand = "ERR unknown command";
    constexpr std::string_view ErrWrongArity = "ERR wrong number of arguments";
    constexpr std::string_view ErrInvalidDbIndex = "ERR invalid DB index";
    constexpr std::string_view ErrInvalidInteger = "ERR value is not an integer or out of range";
    constexpr std::string_view ErrWrongType = "WRONGTYPE Operation against a key holding the wrong kind of value";

    enum class CommandName : std::uint8_t {
        Del = 0,
        Exists,
        Get,
        Ping,
        Select,
        Set,
        Type,
        Ttl,
        Pttl,
        Persist,
        Expire,
        PExpire
    };

    struct CommandSpec {
        std::string_view name;
        std::size_t min_arity;
        std::size_t max_arity;
        CommandName command_name;
        bool write;
    };

    constexpr std::size_t UnlimitedArity = std::numeric_limits<std::size_t>::max();

    constexpr auto makeRegistry() {
        std::array<CommandSpec, CommandNumber> registry{
            {
                {"DEL", 2, UnlimitedArity, CommandName::Del, true},
                {"EXISTS", 2, UnlimitedArity, CommandName::Exists, false},
                {"GET", 2, 2, CommandName::Get, false},
                {"PING", 1, 2, CommandName::Ping, false},
                {"SELECT", 2, 2, CommandName::Select, false},
                {"SET", 3, 3, CommandName::Set, true},
                {"TYPE", 2, 2, CommandName::Type, false},
                {"TTL", 2, 2, CommandName::Ttl, false},
                {"PTTL", 2, 2, CommandName::Pttl, false},
                {"PERSIST", 2, 2, CommandName::Persist, true},
                {"EXPIRE", 3, 3, CommandName::Expire, true},
                {"PEXPIRE", 3, 3, CommandName::PExpire, true}
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

        const auto max = std::numeric_limits<hyper::Milliseconds::rep>::max();
        const auto min = std::numeric_limits<hyper::Milliseconds::rep>::min();
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
        return {};
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
    case CommandName::Del:
        return del_(manager, client, args, now);
    case CommandName::Exists:
        return exists_(manager, client, args, now);
    case CommandName::Get:
        return get_(manager, client, args, now);
    case CommandName::Ping:
        return ping_(args);
    case CommandName::Select:
        return select_(manager, client, args);
    case CommandName::Set:
        return set_(manager, client, args, now);
    case CommandName::Type:
        return type_(manager, client, args, now);
    case CommandName::Ttl:
        return ttl_(manager, client, args, now);
    case CommandName::Pttl:
        return pttl_(manager, client, args, now);
    case CommandName::Persist:
        return persist_(manager, client, args, now);
    case CommandName::Expire:
        return expire_(manager, client, args, now);
    case CommandName::PExpire:
        return pexpire_(manager, client, args, now);
    }
    assert(false);
    return {};
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

hyper::RespValue hyper::CommandExecutor::set_(RedisManager& manager, RedisClientContext& client, Args args,
                                              ExpireTimePoint now) const {
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


hyper::RespValue hyper::CommandExecutor::pttl_(RedisManager& manager, RedisClientContext& client, Args args,
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

    auto milliseconds = parseExpireDuration(args[2], 1000);
    if (milliseconds.has_value()) {
        return respInteger(db->expireAfter(args[1], milliseconds.value(), now) ? 1 : 0);
    }
    return commandError(ErrInvalidInteger);
}

hyper::RespValue hyper::CommandExecutor::pexpire_(RedisManager& manager, RedisClientContext& client, Args args,
                                                  ExpireTimePoint now) const {
    auto db = client.currentDb(manager);
    assert(db != nullptr);

    auto milliseconds = parseExpireDuration(args[2], 1);
    if (milliseconds.has_value()) {
        return respInteger(db->expireAfter(args[1], milliseconds.value(), now) ? 1 : 0);
    }
    return commandError(ErrInvalidInteger);
}
