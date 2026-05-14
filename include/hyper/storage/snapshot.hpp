#pragma once

#include <cstdint>
#include <vector>
#include <string_view>
#include "database.hpp"
#include "redis_manager.hpp"


namespace hyper {
    static constexpr std::string_view Magic = "REDIS";
    static constexpr std::string_view Version = "0009";

    static constexpr std::uint8_t OpCode_Idle = 248;
    static constexpr std::uint8_t OpCode_Freq = 249;
    static constexpr std::uint8_t OpCode_Aux = 250;
    static constexpr std::uint8_t OpCode_ResizeDb = 251;
    static constexpr std::uint8_t OpCode_ExpireTimeMs = 252;
    static constexpr std::uint8_t OpCode_ExpireTime = 253;
    static constexpr std::uint8_t OpCode_SelectDb = 254;
    static constexpr std::uint8_t OpCode_EOF = 255;

    class Snapshot {
    public:
        static std::vector<std::uint8_t> save(RedisManager& manager, ExpireTimePoint now);

        static bool load(const std::vector<std::uint8_t>& data, RedisManager& manager, ExpireTimePoint now);

    };
}
