#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "hyper/server/resp_value.hpp"

namespace hyper {
    [[nodiscard]] std::string serializeRespValue(const RespValue& value);


    enum class RespParseStatus : std::uint8_t {
        Complete,
        Incomplete,
        Error
    };

    struct RespCommand {
        std::vector<std::string> args;
    };

    struct RespParseResult {
        RespParseStatus status{};
        RespCommand command{};
        std::size_t consumed{};
    };

    [[nodiscard]] RespParseResult parseRespCommand(std::string_view input); //todo
}
