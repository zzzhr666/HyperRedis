#pragma once
#include "event_loop.hpp"
#include "redis_server.hpp"
#include "tcp_listener.hpp"

namespace hyper {
    class RedisServerRunner {
    public:
        RedisServerRunner() : running_(false) {}
        bool start(const TcpListenOptions& option);
        void runOnce(std::chrono::milliseconds timeout);

        void stop();

        [[nodiscard]] std::uint16_t port() const noexcept;

        [[nodiscard]] bool running() const noexcept {
            return running_;
        }

    private:
        RedisServer server_;
        EventLoop loop_;
        std::optional<TcpListener> listener_;
        bool running_;
    };
}
