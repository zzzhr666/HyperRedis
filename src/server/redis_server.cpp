#include "hyper/server/redis_server.hpp"

#include <utility>

#include "hyper/server/command_registry.hpp"
#include "hyper/storage/aof_appender.hpp"
#include "hyper/storage/rdb_saver.hpp"

hyper::RedisServer::RedisServer(std::size_t db_count, std::unique_ptr<AofAppender> aof_appender,
                                std::unique_ptr<RdbSaver> rdb_saver)
    : manager_(db_count), aof_appender_(std::move(aof_appender)), rdb_saver_(std::move(rdb_saver)),
      processor_(aof_appender_.get()), dirty_count_(0) {}

hyper::RedisServer::RedisServer(std::size_t db_count)
    : RedisServer(db_count, nullptr, nullptr) {}

hyper::RedisServer::~RedisServer() = default;

std::size_t hyper::RedisServer::activeExpireCycle(ExpireTimePoint now, std::size_t max_checks_per_db) {
    std::size_t deleted{};
    for (std::size_t i = 0; i < manager_.dbCount(); ++i) {
        auto db = manager_.db(i);
        if (db) {
            deleted += db->activeExpireCycle(now,max_checks_per_db);
        }
    }
    dirty_count_ += deleted;
    return deleted;
}

hyper::RespValue hyper::RedisServer::execute(RedisClientContext& client, CommandExecutor::Args args,
                                             ExpireTimePoint now) {
    const CommandSpec* res = nullptr;
    if (!args.empty()) {
        std::string cmd(args[0]);
        std::ranges::transform(cmd, cmd.begin(), [](unsigned char c) {
            return std::toupper(c);
        });
        res = findCommand(cmd);
    }
    auto execute_res = processor_.execute(manager_, client, args, now);
    if (res && res->write && !std::holds_alternative<RespError>(execute_res)) {
        ++dirty_count_;
    }
    return execute_res;
}
