#pragma once

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <optional>

#include "event_loop.hpp"
#include "tcp_listener.hpp"
#include "hyper/storage/aof_appender.hpp"
#include "hyper/storage/redis_manager.hpp"

namespace hyper {
    class RedisServer;

    struct RedisServerPersistenceConfig {
        std::optional<std::filesystem::path> aof_path;
        AofFsyncPolicy append_fsync_policy{AofFsyncPolicy::No};

        std::optional<std::filesystem::path> rdb_path;
        bool load_rdb_on_start{false};
        bool save_rdb_on_stop{false};
        bool load_aof_on_start{false};
    };

    struct RedisServerRunnerConfig {
        TcpListenOptions listen_options;
        std::size_t db_count{RedisManager::DefaultDbCount};
        RedisServerPersistenceConfig persistence;
    };

    class RedisServerRunner {
    public:
        RedisServerRunner();
        ~RedisServerRunner();

        bool start(const RedisServerRunnerConfig& config);
        void runOnce(std::chrono::milliseconds timeout);

        void stop();

        [[nodiscard]] std::uint16_t port() const noexcept;

        [[nodiscard]] bool running() const noexcept {
            return running_;
        }

    private:
        std::unique_ptr<RedisServer> server_;
        EventLoop loop_;
        std::optional<TcpListener> listener_;
        bool running_;
        std::optional<EventLoop::TimeEventId> server_cron_event_id_;
    };
}
