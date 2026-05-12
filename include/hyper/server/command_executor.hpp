#pragma once

#include <span>

#include "hyper/server/resp_value.hpp"
#include "hyper/storage/database.hpp"


namespace hyper {
    class RedisManager;
    class RedisClientContext;

    class CommandExecutor {
    public:
        using Args = std::span<const std::string_view>;

        [[nodiscard]] RespValue execute(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

    private:
        [[nodiscard]] RespValue dbSize_(RedisManager& manager, RedisClientContext& client,
                                        ExpireTimePoint now) const;

        [[nodiscard]] RespValue ping_(Args args) const;

        [[nodiscard]] RespValue select_(RedisManager& manager, RedisClientContext& client, Args args) const;

        [[nodiscard]] RespValue set_(RedisManager& manager, RedisClientContext& client, Args args) const;

        [[nodiscard]] RespValue get_(RedisManager& manager, RedisClientContext& client, Args args,
                                     ExpireTimePoint now) const;

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

        [[nodiscard]] RespValue flushAll_(RedisManager& manager) const;

        [[nodiscard]] RespValue flushDb_(RedisManager& manager, RedisClientContext& client) const;

        [[nodiscard]] RespValue randomkey_(RedisManager& manager, RedisClientContext& client,
                                           ExpireTimePoint now) const;

        [[nodiscard]] RespValue rename_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;

        [[nodiscard]] RespValue renameNx_(RedisManager& manager, RedisClientContext& client, Args args,
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

    };
}
