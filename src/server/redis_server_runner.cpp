#include "hyper/server/redis_server_runner.hpp"
#include "hyper/server/redis_server.hpp"
#include "hyper/storage/aof_appender.hpp"
#include "hyper/storage/rdb_saver.hpp"

namespace {
    constexpr auto ServerCronInterval = std::chrono::milliseconds{100};
}

hyper::RedisServerRunner::RedisServerRunner()
    : server_(nullptr), running_(false), server_cron_event_id_(std::nullopt) {}

hyper::RedisServerRunner::~RedisServerRunner() = default;

hyper::StartResult hyper::RedisServerRunner::start(const RedisServerRunnerConfig& config) {
    if (running_) {
        return {false,"the server is already running"};
    }
    auto& [listen_option , db_count,persistence_config] = config;
    if ((persistence_config.load_rdb_on_start || persistence_config.save_rdb_on_stop) &&
        !persistence_config.rdb_path.has_value()) {
        return {false,"Rdb load on start or save on stop is enabled but RDB file path is missing"};
    }
    if (persistence_config.load_aof_on_start && !persistence_config.aof_path.has_value()) {
        return {false,"AOF load on start is enabled but AOF file path is missing"};
    }
    std::unique_ptr<AofAppender> aof_appender{nullptr};
    std::unique_ptr<RdbSaver> rdb_saver{nullptr};
    if (persistence_config.aof_path.has_value()) {
        aof_appender = std::make_unique<AofAppender>(persistence_config.aof_path.value(),
                                                     persistence_config.append_fsync_policy);
    }
    if (persistence_config.rdb_path.has_value()) {
        rdb_saver = std::make_unique<RdbSaver>(persistence_config.rdb_path.value());
    }


    auto server = std::make_unique<RedisServer>(db_count, std::move(aof_appender), std::move(rdb_saver));
    if (persistence_config.load_aof_on_start) {
        if (!server->loadAof(ExpireClock::now())) {
            return {false,"Failed to load AOF file during startup"};
        }
    } else if (persistence_config.load_rdb_on_start) {
        if (!server->loadRdb(ExpireClock::now())) {
            return {false,"Failed to load RDB file during startup"};
        }
    }
    auto listener_res = TcpListener::create(listen_option);
    if (std::holds_alternative<std::string>(listener_res)) {
        return {false,std::get<std::string>(listener_res)};
    }
    listener_ = std::move(std::get<TcpListener>(listener_res));
    if (!listener_.has_value()) {
        return {false,"unable to create listener"};
    }

    if (!server->attachListener(loop_, listener_->fd())) {
        listener_.reset();
        return {false, "Failed to attach TCP listener to event loop"};
    }

    server_ = std::move(server);
    server_->setSaveRdbOnStop(persistence_config.save_rdb_on_stop);

    server_cron_event_id_ = loop_.addTimeEvent(
        ServerCronInterval, [this]()-> std::optional<std::chrono::milliseconds> {
            if (!running_ || server_ == nullptr) {
                return std::nullopt;
            }
            server_->serverCron(loop_, ExpireClock::now());
            return ServerCronInterval;
        });
    if (!server_cron_event_id_.has_value()) {
        server_->detachListener(loop_,listener_->fd());
        listener_.reset();
        server.reset();
        return {false,"Failed to register serverCron time event"};
    }

    running_ = true;

    return {true,""};
}

void hyper::RedisServerRunner::runOnce(std::chrono::milliseconds timeout) {
    if (running_) {
        loop_.runOnce(timeout);
    }
}

void hyper::RedisServerRunner::stop() {
    if (!running_) {
        return;
    }
    if (server_cron_event_id_.has_value()) {
        loop_.removeTimeEvent(server_cron_event_id_.value());
        server_cron_event_id_.reset();
    }
    server_->detachListener(loop_, listener_.value().fd());
    listener_.reset();
    if (server_->saveRdbOnStop()) {
        (void)server_->saveRdb(ExpireClock::now());
    }
    server_.reset();
    running_ = false;
}

std::uint16_t hyper::RedisServerRunner::port() const noexcept {
    if (listener_.has_value()) {
        return listener_->port();
    }
    return 0;
}
