#pragma once
#include <cstddef>
#include <filesystem>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "client_session.hpp"
#include "command_processor.hpp"
#include "hyper/storage/redis_manager.hpp"

namespace hyper {
    class EventLoop;
    class RdbSaver;
    class AofAppender;

    class RedisServer {
    public:

        RedisServer(std::size_t db_count, std::unique_ptr<AofAppender> aof_appender,
                    std::unique_ptr<RdbSaver> rdb_saver);

        explicit RedisServer(std::size_t db_count = RedisManager::DefaultDbCount);

        ~RedisServer();

        RedisServer(const RedisServer&) = delete;

        RedisServer& operator=(const RedisServer&) = delete;

        RedisManager& manager() noexcept {
            return manager_;
        }

        [[nodiscard]] const RedisManager& manager() const noexcept {
            return manager_;
        }

        [[nodiscard]] std::size_t dirtyCount() const noexcept {
            return dirty_count_;
        }

        [[nodiscard]] std::size_t activeExpireCycle(ExpireTimePoint now, std::size_t max_checks_per_db);

        RespValue execute(RedisClientContext& client, CommandExecutor::Args args, ExpireTimePoint now);


        [[nodiscard]] bool addClient(int fd);
        [[nodiscard]] bool removeClient(int fd);
        [[nodiscard]] ClientSession* clientSession(int fd) noexcept;
        [[nodiscard]] const ClientSession* clientSession(int fd) const noexcept;

        [[nodiscard]] std::size_t clientCount() const noexcept {
            return client_sessions_.size();
        }

        [[nodiscard]] bool attachClient(EventLoop& loop, int fd);

        void detachClient(EventLoop& loop, int fd);

        [[nodiscard]] bool attachListener(EventLoop& loop, int listen_fd);
        void detachListener(EventLoop& loop, int listen_fd);

        //RDB
        [[nodiscard]] bool hasRdbSaver() const noexcept {
            return rdb_saver_ != nullptr;
        }
        [[nodiscard]] bool loadRdb(ExpireTimePoint now);
        [[nodiscard]] bool saveRdb(ExpireTimePoint now);

        //AOF
        [[nodiscard]] bool hasAofAppender() const noexcept {
            return aof_appender_ != nullptr;
        }
        [[nodiscard]] bool loadAof(ExpireTimePoint now);

        [[nodiscard]] ExpireTimePoint lastSaveTime() const noexcept {
            return last_save_time_;
        }

        [[nodiscard]] bool saveRdbOnStop() const noexcept {
            return save_rdb_on_stop_;
        }

        void setSaveRdbOnStop(bool enable) noexcept {
            save_rdb_on_stop_ = enable;
        }

        [[nodiscard]] std::size_t maxClients() const noexcept {
            return max_clients_;
        }

        void setMaxClients(std::size_t max) noexcept {
            max_clients_ = max;
        }

        [[nodiscard]] std::uint32_t timeout() const noexcept {
            return static_cast<std::uint32_t>(timeout_seconds_.count());
        }

        void setTimeout(std::uint32_t seconds) noexcept {
            timeout_seconds_ = std::chrono::seconds{seconds};
        }

        std::size_t serverCron(EventLoop& loop, ExpireTimePoint now);

    private:
        void enableClientWritable_(EventLoop& loop, int fd);

        bool adoptClient_(EventLoop& loop, int fd);

        RespValue handleConfig_(CommandExecutor::Args args);

        std::string generateInfoString_(CommandExecutor::Args args);

        RespValue rewriteAof_(ExpireTimePoint now);

    private:
        RedisManager manager_;
        std::unique_ptr<AofAppender> aof_appender_;
        std::unique_ptr<RdbSaver> rdb_saver_;
        CommandProcessor processor_;
        std::unordered_map<int, ClientSession> client_sessions_;
        std::unordered_set<int> owned_client_fds_;
        ExpireTimePoint last_save_time_;
        bool save_rdb_on_stop_;
        std::size_t dirty_count_;
        std::size_t total_commands_;
        std::size_t max_clients_;
        ExpireTimePoint start_time_;
        std::chrono::seconds timeout_seconds_{0};
        };
        }
