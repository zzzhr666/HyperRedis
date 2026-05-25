#include "hyper/server/redis_server_runner.hpp"
#include "hyper/server/redis_server.hpp"
#include "hyper/storage/aof_appender.hpp"
#include "hyper/storage/rdb_saver.hpp"

hyper::RedisServerRunner::RedisServerRunner() : server_(nullptr), running_(false), save_rdb_on_stop_(false) {}

hyper::RedisServerRunner::~RedisServerRunner() = default;

bool hyper::RedisServerRunner::start(const RedisServerRunnerConfig& config) {
    if (running_) {
        return false;
    }
    save_rdb_on_stop_ = false;


    auto& [listen_option , db_count,persistence_config] = config;
    if ((persistence_config.load_rdb_on_start || persistence_config.save_rdb_on_stop) &&
        !persistence_config.rdb_path.has_value()) {
        return false;
    }
    if (persistence_config.load_aof_on_start && !persistence_config.aof_path.has_value()) {
        return false;
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
            return false;
        }
    } else if (persistence_config.load_rdb_on_start) {
        if (!server->loadRdb(ExpireClock::now())) {
            return false;
        }
    }

    listener_ = TcpListener::create(listen_option);
    if (!listener_.has_value()) {
        return false;
    }

    if (!server->attachListener(loop_, listener_->fd())) {
        listener_.reset();
        return false;
    }

    server_ = std::move(server);
    running_ = true;

    save_rdb_on_stop_ = persistence_config.save_rdb_on_stop;
    return true;
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
    server_->detachListener(loop_, listener_.value().fd());
    listener_.reset();
    if (save_rdb_on_stop_) {
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
