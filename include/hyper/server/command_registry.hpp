#pragma once

#include <cstdint>
#include <limits>
#include <string_view>
#include <span>

namespace hyper {
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
        PExpireAt,
        ZAdd,
        ZCard,
        ZRange,
        ZRem,
        ZScore,
        LIndex,
        LSet,
        LInsert,
        LRem,
        LTrim,
        HExists,
        HKeys,
        HVals,
        SPop,
        SRandMember,
        ZRank,
        ZRevRank,
        ZCount,
        ZRevRange,
        ZIncrBy,
        ZRemRangeByRank,
        ZRemRangeByScore,
        Publish,
        Subscribe,
        Unsubscribe,
        Save,
        BgSave,
        LastSave,
        Info,
        Object,
        Time,
        Config,
        RewriteAof,
        BgRewriteAof,
        Command
    };

    struct CommandSpec {
        std::string_view name;
        std::size_t min_arity;
        std::size_t max_arity;
        CommandName command_name;
        bool write;
    };


    constexpr std::size_t UnlimitedArity = std::numeric_limits<std::size_t>::max();

    [[nodiscard]] const CommandSpec* findCommand(std::string_view upper_case_command);
    [[nodiscard]] std::span<const CommandSpec>getAllCommands();
}
