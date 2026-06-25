#include "hyper/server/command_registry.hpp"


#include <algorithm>
#include <array>

namespace {
    constexpr auto makeRegistry() {
        constexpr std::size_t CommandNumber = 81;

        std::array<hyper::CommandSpec, CommandNumber> registry{
            {
                {"APPEND", 3, 3, hyper::CommandName::Append, true},
                {"DBSIZE", 1, 1, hyper::CommandName::DbSize, false},
                {"DECR", 2, 2, hyper::CommandName::Decr, true},
                {"DEL", 2, hyper::UnlimitedArity, hyper::CommandName::Del, true},
                {"CONFIG", 3, 4, hyper::CommandName::Config, false},
                {"EXISTS", 2, hyper::UnlimitedArity, hyper::CommandName::Exists, false},
                {"EXPIRE", 3, 4, hyper::CommandName::Expire, true},
                {"FLUSHALL", 1, 1, hyper::CommandName::FlushAll, true},
                {"FLUSHDB", 1, 1, hyper::CommandName::FlushDb, true},
                {"GET", 2, 2, hyper::CommandName::Get, false},
                {"GETRANGE", 4, 4, hyper::CommandName::GetRange, false},
                {"HDEL", 3, hyper::UnlimitedArity, hyper::CommandName::HDel, true},
                {"HGET", 3, 3, hyper::CommandName::HGet, false},
                {"HGETALL", 2, 2, hyper::CommandName::HGetAll, false},
                {"HLEN", 2, 2, hyper::CommandName::HLen, false},
                {"HSET", 4, 4, hyper::CommandName::HSet, true},
                {"INCR", 2, 2, hyper::CommandName::Incr, true},
                {"INCRBY", 3, 3, hyper::CommandName::IncrBy, true},
                {"INCRBYFLOAT", 3, 3, hyper::CommandName::IncrByFloat, true},
                {"LLEN", 2, 2, hyper::CommandName::LLen, false},
                {"LPOP", 2, 2, hyper::CommandName::LPop, true},
                {"LPUSH", 3, hyper::UnlimitedArity, hyper::CommandName::LPush, true},
                {"LRANGE", 4, 4, hyper::CommandName::LRange, false},
                {"MGET", 2, hyper::UnlimitedArity, hyper::CommandName::MGet, false},
                {"MSET", 3, hyper::UnlimitedArity, hyper::CommandName::MSet, true},
                {"PEXPIRE", 3, 4, hyper::CommandName::PExpire, true},
                {"PEXPIREAT", 3, 4, hyper::CommandName::PExpireAt, true},
                {"PERSIST", 2, 2, hyper::CommandName::Persist, true},
                {"PING", 1, 2, hyper::CommandName::Ping, false},
                {"PTTL", 2, 2, hyper::CommandName::Pttl, false},
                {"RANDOMKEY", 1, 1, hyper::CommandName::RandomKey, false},
                {"RENAME", 3, 3, hyper::CommandName::Rename, true},
                {"RENAMENX", 3, 3, hyper::CommandName::RenameNx, true},
                {"RPOP", 2, 2, hyper::CommandName::RPop, true},
                {"RPUSH", 3, hyper::UnlimitedArity, hyper::CommandName::RPush, true},
                {"SADD", 3, hyper::UnlimitedArity, hyper::CommandName::SAdd, true},
                {"SCARD", 2, 2, hyper::CommandName::SCard, false},
                {"SELECT", 2, 2, hyper::CommandName::Select, false},
                {"SET", 3, 3, hyper::CommandName::Set, true},
                {"SETRANGE", 4, 4, hyper::CommandName::SetRange, true},
                {"SISMEMBER", 3, 3, hyper::CommandName::SIsMember, false},
                {"SMEMBERS", 2, 2, hyper::CommandName::SMembers, false},
                {"SREM", 3, hyper::UnlimitedArity, hyper::CommandName::SRem, true},
                {"SPOP", 2, 3, hyper::CommandName::SPop, true},
                {"SRANDMEMBER", 2, 3, hyper::CommandName::SRandMember, false},
                {"STRLEN", 2, 2, hyper::CommandName::StrLen, false},
                {"TTL", 2, 2, hyper::CommandName::Ttl, false},
                {"TYPE", 2, 2, hyper::CommandName::Type, false},
                {"ZADD", 4, hyper::UnlimitedArity, hyper::CommandName::ZAdd, true},
                {"ZCARD", 2, 2, hyper::CommandName::ZCard, false},
                {"ZRANGE", 4, 5, hyper::CommandName::ZRange, false},
                {"ZREM", 3, hyper::UnlimitedArity, hyper::CommandName::ZRem, true},
                {"ZSCORE", 3, 3, hyper::CommandName::ZScore, false},
                {"HEXISTS", 3, 3, hyper::CommandName::HExists, false},
                {"HKEYS", 2, 2, hyper::CommandName::HKeys, false},
                {"HVALS", 2, 2, hyper::CommandName::HVals, false},
                {"LINDEX", 3, 3, hyper::CommandName::LIndex, false},
                {"LINSERT", 5, 5, hyper::CommandName::LInsert, true},
                {"LREM", 4, 4, hyper::CommandName::LRem, true},
                {"LSET", 4, 4, hyper::CommandName::LSet, true},
                {"LTRIM", 4, 4, hyper::CommandName::LTrim, true},
                {"ZCOUNT", 4, 4, hyper::CommandName::ZCount, false},
                {"ZINCRBY", 4, 4, hyper::CommandName::ZIncrBy, true},
                {"ZRANK", 3, 3, hyper::CommandName::ZRank, false},
                {"ZREMRANGEBYRANK", 4, 4, hyper::CommandName::ZRemRangeByRank, true},
                {"ZREMRANGEBYSCORE", 4, 4, hyper::CommandName::ZRemRangeByScore, true},
                {"ZREVRANGE", 4, 5, hyper::CommandName::ZRevRange, false},
                {"ZREVRANK", 3, 3, hyper::CommandName::ZRevRank, false},
                {"SAVE", 1, 1, hyper::CommandName::Save, false},
                {"BGSAVE", 1, 1, hyper::CommandName::BgSave, false},
                {"LASTSAVE", 1, 1, hyper::CommandName::LastSave, false},
                {"INFO", 1, 2, hyper::CommandName::Info, false},
                {"OBJECT", 3, 3, hyper::CommandName::Object, false},
                {"TIME", 1, 1, hyper::CommandName::Time, false},
                {"REWRITEAOF", 1, 1, hyper::CommandName::RewriteAof, false},
                {"BGREWRITEAOF", 1, 1, hyper::CommandName::BgRewriteAof, false},
                {"COMMAND",1,hyper::UnlimitedArity,hyper::CommandName::Command,false},
                {"HRZHANG",1,1,hyper::CommandName::Ping,false},
                {"PUBLISH", 3, 3, hyper::CommandName::Publish, false},
                {"SUBSCRIBE", 2, hyper::UnlimitedArity, hyper::CommandName::Subscribe, false},
                {"UNSUBSCRIBE", 1, hyper::UnlimitedArity, hyper::CommandName::Unsubscribe, false}

            }
        };
        std::ranges::sort(registry, {}, &hyper::CommandSpec::name);

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
}

const hyper::CommandSpec* hyper::findCommand(std::string_view upper_case_command) {
    auto it = std::ranges::lower_bound(registry, upper_case_command, {}, &CommandSpec::name);
    if (it == registry.end() || it->name != upper_case_command) {
        return nullptr;
    }
    return &*it;
}

std::span<const hyper::CommandSpec> hyper::getAllCommands() {
    return {registry.begin(),registry.end()};
}
