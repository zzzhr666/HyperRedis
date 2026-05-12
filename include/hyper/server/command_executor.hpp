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
        [[nodiscard]] RespValue ping_(Args args) const;
        [[nodiscard]] RespValue select_(RedisManager& manager, RedisClientContext& client, Args args) const;
        [[nodiscard]] RespValue set_(RedisManager& manager, RedisClientContext& client, Args args,
                                     ExpireTimePoint now) const;
        [[nodiscard]] RespValue get_(RedisManager& manager, RedisClientContext& client, Args args,
                                     ExpireTimePoint now) const;
        [[nodiscard]] RespValue del_(RedisManager& manager, RedisClientContext& client, Args args, ExpireTimePoint now) const;
        [[nodiscard]] RespValue exists_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;
        [[nodiscard]] RespValue type_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;
        [[nodiscard]] RespValue ttl_(RedisManager& manager, RedisClientContext& client, Args args,
                                     ExpireTimePoint now) const;
        [[nodiscard]] RespValue pttl_(RedisManager& manager, RedisClientContext& client, Args args,
                                      ExpireTimePoint now) const;
        [[nodiscard]] RespValue persist_(RedisManager& manager, RedisClientContext& client, Args args,
                                         ExpireTimePoint now) const;
        [[nodiscard]] RespValue expire_(RedisManager& manager, RedisClientContext& client, Args args,
                                        ExpireTimePoint now) const;
        [[nodiscard]] RespValue pexpire_(RedisManager& manager, RedisClientContext& client, Args args,
                                         ExpireTimePoint now) const;

    };
}
