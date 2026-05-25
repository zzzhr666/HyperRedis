#include "hyper/server/server_options.hpp"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <string>

std::string hyper::serverUsage(std::string_view program) {
    std::string usage = "Usage: ";
    usage.append(program);
    usage.append(" [--host HOST] [--port PORT] [--aof PATH] "
        "[--appendfsync no|always|everysec] [--rdb PATH] "
        "[--load-rdb] [--save-rdb-on-stop] [--load-aof]");
    return usage;
}

bool hyper::parseAofFsyncPolicy(std::string_view text, AofFsyncPolicy& policy) {
    std::string lower(text);
    std::ranges::transform(lower, lower.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });

    if (lower == "no") {
        policy = AofFsyncPolicy::No;
        return true;
    }
    if (lower == "always") {
        policy = AofFsyncPolicy::Always;
        return true;
    }
    if (lower == "everysec") {
        policy = AofFsyncPolicy::EverySecond;
        return true;
    }
    return false;
}

bool hyper::parseServerPort(std::string_view text, std::uint16_t& port) {
    int value{};
    const auto first = text.data();
    const auto last = first + text.size();
    const auto [ptr,ec] = std::from_chars(first, last, value);
    if (ec != std::errc() || ptr != last || value < 0 || value > 65535) {
        return false;
    }
    port = static_cast<std::uint16_t>(value);
    return true;
}

hyper::ServerOptionsParseResult hyper::parseServerOptions(int argc, char* argv[],
                                                          RedisServerRunnerConfig& config) {
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg{argv[i]};
        if (arg == "--help" || arg == "-h") {
            return ServerOptionsParseResult{false, true, {}};
        }
        if (arg == "--host") {
            if (i + 1 >= argc) {
                return ServerOptionsParseResult{false, false, "missing value for --host"};
            }
            config.listen_options.host = argv[++i];
            continue;
        }
        if (arg == "--port") {
            if (i + 1 >= argc) {
                return ServerOptionsParseResult{false, false, "missing value for --port"};
            }
            if (!parseServerPort(argv[i + 1], config.listen_options.port)) {
                std::string error = "invalid port: ";
                error.append(argv[i + 1]);
                return ServerOptionsParseResult{false, false, std::move(error)};
            }
            ++i;
            continue;
        }
        if (arg == "--aof") {
            if (i + 1 >= argc) {
                return ServerOptionsParseResult{false, false, "missing value for --aof"};
            }
            config.persistence.aof_path = argv[++i];
            continue;
        }
        if (arg == "--appendfsync") {
            if (i + 1 >= argc) {
                return ServerOptionsParseResult{false, false, "missing value for --appendfsync"};
            }
            if (!parseAofFsyncPolicy(argv[i + 1], config.persistence.append_fsync_policy)) {
                std::string error = "invalid appendfsync policy: ";
                error.append(argv[i + 1]);
                return ServerOptionsParseResult{false, false, std::move(error)};
            }
            ++i;
            continue;
        }
        if (arg == "--rdb") {
            if (i + 1 >= argc) {
                return ServerOptionsParseResult{false, false, "missing value for --rdb"};
            }
            config.persistence.rdb_path = argv[++i];
            continue;
        }
        if (arg == "--load-rdb") {
            config.persistence.load_rdb_on_start = true;
            continue;
        }
        if (arg == "--save-rdb-on-stop") {
            config.persistence.save_rdb_on_stop = true;
            continue;
        }
        if (arg == "--load-aof") {
            config.persistence.load_aof_on_start = true;
            continue;
        }

        std::string error = "unknown argument: ";
        error.append(arg);
        return ServerOptionsParseResult{false, false, std::move(error)};
    }
    return ServerOptionsParseResult{true, false, {}};
}
