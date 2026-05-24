#include "hyper/server/redis_server_runner.hpp"

bool hyper::RedisServerRunner::start(const TcpListenOptions& option) {
    if (running_) {
        return false;
    }


    listener_ = TcpListener::create(option);
    if (!listener_.has_value()) {
        return false;
    }

    if (!server_.attachListener(loop_, listener_->fd())) {
        listener_.reset();
        return false;
    }

    running_ = true;
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
    server_.detachListener(loop_, listener_.value().fd());
    listener_.reset();
    running_ = false;
}

std::uint16_t hyper::RedisServerRunner::port() const noexcept {
    if (listener_.has_value()) {
        return listener_->port();
    }
    return 0;
}
