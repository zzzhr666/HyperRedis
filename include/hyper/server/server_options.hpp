#pragma once

#include <string>
#include <string_view>

#include "redis_server_runner.hpp"

namespace hyper {
    struct ServerOptionsParseResult {
        bool ok{false};
        bool help_requested{false};
        std::string error;
    };

    [[nodiscard]] std::string serverUsage(std::string_view program);

    [[nodiscard]] bool parseAofFsyncPolicy(std::string_view text, AofFsyncPolicy& policy);

    [[nodiscard]] bool parseServerPort(std::string_view text, std::uint16_t& port);

    [[nodiscard]] ServerOptionsParseResult parseServerOptions(int argc,
                                                              char* argv[],
                                                              RedisServerRunnerConfig& config);
}
