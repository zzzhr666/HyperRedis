#pragma once
#include <cstddef>
#include <memory>

#include "command_processor.hpp"
#include "hyper/storage/redis_manager.hpp"

namespace hyper {
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

        [[nodiscard]] std::size_t activeExpireCycle(ExpireTimePoint now,std::size_t max_checks_per_db);

        RespValue execute(RedisClientContext& client, CommandExecutor::Args args, ExpireTimePoint now);

    private:
        RedisManager manager_;
        std::unique_ptr<AofAppender> aof_appender_;
        std::unique_ptr<RdbSaver> rdb_saver_;
        CommandProcessor processor_;
        std::size_t dirty_count_;
    };
}
