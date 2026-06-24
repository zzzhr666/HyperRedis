#include <chrono>
#include <csignal>

#include <spdlog/spdlog.h>

#include "hyper/server/redis_server_runner.hpp"
#include "hyper/server/server_options.hpp"

namespace {
    volatile std::sig_atomic_t stop_request = 0;

    void signalHandler(int) {
        stop_request = 1;
    }

    void signalSigIntAndTerm() {
        std::signal(SIGINT, signalHandler);
        std::signal(SIGTERM, signalHandler);
    }
}

int main(int argc, char* argv[]) {
    signalSigIntAndTerm();
    hyper::RedisServerRunnerConfig config;
    config.listen_options = hyper::TcpListenOptions{"127.0.0.1", 8080};
    const auto parse_result = hyper::parseServerOptions(argc, argv, config);
    if (!parse_result.ok) {
        if (!parse_result.error.empty()) {
            SPDLOG_ERROR("{}", parse_result.error);
        } else if (parse_result.error.empty() && parse_result.help_requested) {
            SPDLOG_ERROR("{}", hyper::serverUsage(argv[0]));
            return 0;
        }
        SPDLOG_ERROR("{}", hyper::serverUsage(argv[0]));
        return 1;
    }
    hyper::RedisServerRunner runner;
    if (auto [success, error] = runner.start(config); !success) {
        SPDLOG_ERROR("Unable to start hyper redis runner:{}", error);
        return 1;
    }
    SPDLOG_INFO("HyperRedis listening on {}:{}", config.listen_options.host, runner.port());
    while (runner.running() && stop_request == 0) {
        runner.runOnce(std::chrono::milliseconds{100});
    }
    runner.stop();
    SPDLOG_INFO("HyperRedis stopped.");
    return 0;
}
