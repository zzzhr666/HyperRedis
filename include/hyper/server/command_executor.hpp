#pragma once

#include <span>

#include "hyper/server/resp_value.hpp"
#include "hyper/storage/database.hpp"


namespace hyper {
    class RedisManager;
    class RedisClientContext;
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
    constexpr std::string_view ErrIndexOutOfRange = "ERR index out of range";
    constexpr std::string_view ErrUnpositiveValue = "ERR value is out of range, must be positive";
    constexpr std::string_view ErrCommandNotImplemented = "ERR command not implemented";
    constexpr std::string_view ErrAofWriteFailed = "ERR append only file write failed";

    class CommandExecutor {
    public:
        using Args = std::span<const std::string_view>;

        [[nodiscard]] RespValue execute(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

    private:
        // --- Generic / Database / Connection ---
        [[nodiscard]] RespValue ping_(Args args) const;

        [[nodiscard]] RespValue select_(RedisManager& manager, RedisClientContext& client, Args args) const;

        [[nodiscard]] RespValue dbSize_(RedisManager& manager, RedisClientContext& client, ExpireTimePoint now) const;

        [[nodiscard]] RespValue del_(RedisManager& manager, RedisClientContext& client, Args args,
                                     ExpireTimePoint now) const;

        [[nodiscard]] RespValue exists_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

        [[nodiscard]] RespValue type_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue ttl_(RedisManager& manager, RedisClientContext& client, Args args,
                                     ExpireTimePoint now) const;

        [[nodiscard]] RespValue pTtl_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue persist_(RedisManager& manager, RedisClientContext& client, Args args,
                                         ExpireTimePoint now) const;

        [[nodiscard]] RespValue expire_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

        [[nodiscard]] RespValue pExpire_(RedisManager& manager, RedisClientContext& client, Args args,
                                         ExpireTimePoint now) const;

        [[nodiscard]] RespValue pExpireAt_(RedisManager& manager, RedisClientContext& client, Args args,
                                           ExpireTimePoint now) const;

        [[nodiscard]] RespValue flushAll_(RedisManager& manager) const;

        [[nodiscard]] RespValue flushDb_(RedisManager& manager, RedisClientContext& client) const;

        [[nodiscard]] RespValue randomkey_(RedisManager& manager, RedisClientContext& client,
                                           ExpireTimePoint now) const;

        [[nodiscard]] RespValue rename_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

        [[nodiscard]] RespValue renameNx_(RedisManager& manager, RedisClientContext& client, Args args,
                                          ExpireTimePoint now) const;

        [[nodiscard]] RespValue save_() const;

        [[nodiscard]] RespValue lastSave_() const;

        [[nodiscard]] RespValue info_() const;

        [[nodiscard]] RespValue object_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

        [[nodiscard]] RespValue time_() const;

        // --- String ---
        [[nodiscard]] RespValue set_(RedisManager& manager, RedisClientContext& client, Args args) const;

        [[nodiscard]] RespValue get_(RedisManager& manager, RedisClientContext& client, Args args,
                                     ExpireTimePoint now) const;

        [[nodiscard]] RespValue mGet_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue mSet_(RedisManager& manager, RedisClientContext& client, Args args) const;

        [[nodiscard]] RespValue strLen_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

        [[nodiscard]] RespValue append_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

        [[nodiscard]] RespValue incr_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue decr_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue incrBy_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

        [[nodiscard]] RespValue incrByFloat_(RedisManager& manager, RedisClientContext& client, Args args,
                                             ExpireTimePoint now) const;

        [[nodiscard]] RespValue getRange_(RedisManager& manager, RedisClientContext& client, Args args,
                                          ExpireTimePoint now) const;

        [[nodiscard]] RespValue setRange_(RedisManager& manager, RedisClientContext& client, Args args,
                                          ExpireTimePoint now) const;

        // --- List ---
        [[nodiscard]] RespValue lPush_(RedisManager& manager, RedisClientContext& client, Args args,
                                       ExpireTimePoint now) const;

        [[nodiscard]] RespValue rPush_(RedisManager& manager, RedisClientContext& client, Args args,
                                       ExpireTimePoint now) const;

        [[nodiscard]] RespValue lPop_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue rPop_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue lLen_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue lRange_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

        [[nodiscard]] RespValue lIndex_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

        [[nodiscard]] RespValue lSet_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue lInsert_(RedisManager& manager, RedisClientContext& client, Args args,
                                         ExpireTimePoint now) const;

        [[nodiscard]] RespValue lRem_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue lTrim_(RedisManager& manager, RedisClientContext& client, Args args,
                                       ExpireTimePoint now) const;

        // --- Hash ---
        [[nodiscard]] RespValue hSet_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue hGet_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue hDel_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue hLen_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue hGetAll_(RedisManager& manager, RedisClientContext& client, Args args,
                                         ExpireTimePoint now) const;

        [[nodiscard]] RespValue hExists_(RedisManager& manager, RedisClientContext& client, Args args,
                                         ExpireTimePoint now) const;

        [[nodiscard]] RespValue hKeys_(RedisManager& manager, RedisClientContext& client, Args args,
                                       ExpireTimePoint now) const;

        [[nodiscard]] RespValue hVals_(RedisManager& manager, RedisClientContext& client, Args args,
                                       ExpireTimePoint now) const;

        // --- Set ---
        [[nodiscard]] RespValue sAdd_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue sRem_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue sIsMember_(RedisManager& manager, RedisClientContext& client, Args args,
                                           ExpireTimePoint now) const;

        [[nodiscard]] RespValue sCard_(RedisManager& manager, RedisClientContext& client, Args args,
                                       ExpireTimePoint now) const;

        [[nodiscard]] RespValue sMembers_(RedisManager& manager, RedisClientContext& client, Args args,
                                          ExpireTimePoint now) const;

        [[nodiscard]] RespValue sPop_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue sRandMember_(RedisManager& manager, RedisClientContext& client, Args args,
                                             ExpireTimePoint now) const;

        // --- ZSet ---
        [[nodiscard]] RespValue zAdd_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue zRem_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;

        [[nodiscard]] RespValue zScore_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

        [[nodiscard]] RespValue zCard_(RedisManager& manager, RedisClientContext& client, Args args,
                                       ExpireTimePoint now) const;

        [[nodiscard]] RespValue zRange_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

        [[nodiscard]] RespValue zRank_(RedisManager& manager, RedisClientContext& client, Args args,
                                       ExpireTimePoint now) const;

        [[nodiscard]] RespValue zRevRank_(RedisManager& manager, RedisClientContext& client, Args args,
                                          ExpireTimePoint now) const;

        [[nodiscard]] RespValue zCount_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

        [[nodiscard]] RespValue zRevRange_(RedisManager& manager, RedisClientContext& client, Args args,
                                           ExpireTimePoint now) const;

        [[nodiscard]] RespValue zIncrBy_(RedisManager& manager, RedisClientContext& client, Args args,
                                         ExpireTimePoint now) const;

        [[nodiscard]] RespValue zRemRangeByRank_(RedisManager& manager, RedisClientContext& client, Args args,
                                                 ExpireTimePoint now) const;

        [[nodiscard]] RespValue zRemRangeByScore_(RedisManager& manager, RedisClientContext& client, Args args,
                                                  ExpireTimePoint now) const;
    };
}
