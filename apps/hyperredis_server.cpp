#include <charconv>
#include <chrono>
#include <cstdint>
#include <string_view>

#include <spdlog/spdlog.h>

#include "hyper/server/redis_server_runner.hpp"
#include "hyper/server/tcp_listener.hpp"

namespace {
    void printUsage(std::string_view program) {
        SPDLOG_ERROR("Usage: {} [--host HOST] [--port PORT]\n", program);
    }

    bool parsePort(std::string_view text, std::uint16_t& port) {
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

    bool parseArgs(int argc, char* argv[], hyper::TcpListenOptions& options) {
        for (int i = 1; i < argc; ++i) {
            const std::string_view arg{argv[i]};
            if (arg == "--help" || arg == "-h") {
                printUsage(argv[0]);
                return false;
            }
            if (arg == "--host") {
                if (i + 1 >= argc) {
                    SPDLOG_ERROR("missing value for --host");
                    printUsage(argv[0]);
                    return false;
                }
                options.host = argv[++i];
                continue;
            }
            if (arg == "--port") {
                if (i + 1 >= argc) {
                    SPDLOG_ERROR("missing value for --port");
                    printUsage(argv[0]);
                    return false;
                }
                if (!parsePort(argv[i + 1],options.port)) {
                    SPDLOG_ERROR("invalid port: {}",argv[i + 1]);
                    printUsage(argv[0]);
                    return false;
                }

                ++i;
                continue;
            }
            SPDLOG_ERROR("unknown argument: {}",arg);
            printUsage(argv[0]);
            return false;
        }
        return true;
    }
}

int main(int argc,char* argv[]) {
    hyper::TcpListenOptions options{"127.0.0.1", 8080};
    if (!parseArgs(argc,argv,options)) {
        return 1;
    }
    hyper::RedisServerRunner runner;
    if (!runner.start(options)) {
        SPDLOG_ERROR("Unable to start hyper redis runner.");
        return 1;
    }
    SPDLOG_INFO("HyperRedis listening on {}:{}", options.host, runner.port());
    while (runner.running()) {
        runner.runOnce(std::chrono::milliseconds{100});
    }
    return 0;
}
