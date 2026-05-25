#include "hyper/server/redis_server.hpp"

#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <utility>
#include <sys/socket.h>

#include "hyper/server/client_socket_io.hpp"
#include "hyper/server/command_registry.hpp"
#include "hyper/server/event_loop.hpp"
#include "hyper/storage/aof_appender.hpp"
#include "hyper/storage/rdb_saver.hpp"
#include "hyper/time.hpp"
#include "hyper/storage/aof_replayer.hpp"


namespace {
    bool setNonBlocking(int fd) noexcept {
        const int flags = ::fcntl(fd,F_GETFL, 0);
        if (flags < 0) {
            return false;
        }
        return ::fcntl(fd,F_SETFL, flags | O_NONBLOCK) == 0;
    }
}

hyper::RedisServer::RedisServer(std::size_t db_count, std::unique_ptr<AofAppender> aof_appender,
                                std::unique_ptr<RdbSaver> rdb_saver)
    : manager_(db_count), aof_appender_(std::move(aof_appender)), rdb_saver_(std::move(rdb_saver)),
      processor_(aof_appender_.get()), dirty_count_(0) {}

hyper::RedisServer::RedisServer(std::size_t db_count)
    : RedisServer(db_count, nullptr, nullptr) {}

hyper::RedisServer::~RedisServer() {
    for (auto fd : owned_client_fds_) {
        ::close(fd);
    }
}

std::size_t hyper::RedisServer::activeExpireCycle(ExpireTimePoint now, std::size_t max_checks_per_db) {
    std::size_t deleted{};
    for (std::size_t i = 0; i < manager_.dbCount(); ++i) {
        if (auto db = manager_.db(i)) {
            deleted += db->activeExpireCycle(now, max_checks_per_db);
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

bool hyper::RedisServer::addClient(int fd) {
    if (fd < 0) {
        return false;
    }
    return client_sessions_.try_emplace(fd, fd).second;
}

bool hyper::RedisServer::removeClient(int fd) {
    if (auto it = client_sessions_.find(fd); it != client_sessions_.end()) {
        client_sessions_.erase(it);
        return true;
    }
    return false;
}

hyper::ClientSession* hyper::RedisServer::clientSession(int fd) noexcept {
    auto it = client_sessions_.find(fd);
    if (it == client_sessions_.end()) {
        return nullptr;
    }
    return &it->second;
}

const hyper::ClientSession* hyper::RedisServer::clientSession(int fd) const noexcept {
    auto it = client_sessions_.find(fd);
    if (it == client_sessions_.end()) {
        return nullptr;
    }
    return &it->second;
}

bool hyper::RedisServer::attachClient(EventLoop& loop, int fd) {
    if (!addClient(fd)) {
        return false;
    }
    bool res = loop.addFileEvent(fd, FileEventMask::Readable, [this,&loop](int rd_fd, FileEventMask) {
        auto* session = clientSession(rd_fd);
        if (session == nullptr) {
            loop.removeFileEvent(rd_fd, FileEventMask::Readable | FileEventMask::Writable);
            return;
        }
        const auto now = ExpireClock::now();
        if (const auto [status,_] = readClientQuery(*session, *this, now);
            status == ClientIoStatus::Closed || status == ClientIoStatus::Error) {
            detachClient(loop, rd_fd);
            return;
        }
        if (!session->replyBuffer().empty()) {
            enableClientWritable_(loop, rd_fd);
        }
    });
    if (!res) {
        (void)removeClient(fd);
        return false;
    }
    return true;
}

void hyper::RedisServer::detachClient(EventLoop& loop, int fd) {
    loop.removeFileEvent(fd, FileEventMask::Readable | FileEventMask::Writable);
    (void)removeClient(fd);
    if (owned_client_fds_.erase(fd) > 0) {
        ::close(fd);
    }
}

bool hyper::RedisServer::attachListener(EventLoop& loop, int listen_fd) {
    if (listen_fd < 0) {
        return false;
    }

    if (!setNonBlocking(listen_fd)) {
        return false;
    }
    return loop.addFileEvent(listen_fd, FileEventMask::Readable, [this,listen_fd,&loop](int, FileEventMask) {
        while (true) {
            int client_fd = ::accept4(listen_fd, nullptr, nullptr,SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (client_fd < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                break;
            }

            adoptClient_(loop, client_fd);
        }
    });
}

void hyper::RedisServer::detachListener(EventLoop& loop, int listen_fd) {
    loop.removeFileEvent(listen_fd, FileEventMask::Readable);
}

bool hyper::RedisServer::loadRdb(ExpireTimePoint now) {
    if (hasRdbSaver()) {
        return rdb_saver_->load(manager_, now);
    }
    return false;
}

bool hyper::RedisServer::saveRdb(ExpireTimePoint now) {
    if (hasRdbSaver()) {
        return rdb_saver_->save(manager_, now);
    }
    return false;
}

bool hyper::RedisServer::loadAof(ExpireTimePoint now) {
    if (hasAofAppender()) {
        auto [replay_success,current_db_index] = AofReplayer::replay(aof_appender_->path(), manager_, now);
        if (!replay_success) {
            return false;
        }
        aof_appender_->setSelectedDbIndex(current_db_index);
        return true;
    }
    return false;
}

void hyper::RedisServer::enableClientWritable_(EventLoop& loop, int fd) {
    loop.addFileEvent(fd, FileEventMask::Writable, [this,&loop](int writable_fd, FileEventMask) {
        auto* writable_session = clientSession(writable_fd);
        if (writable_session == nullptr) {
            loop.removeFileEvent(writable_fd, FileEventMask::Writable);
            return;
        }
        if (auto [status,_] = writeClientReply(*writable_session);
            status == ClientIoStatus::Closed || status == ClientIoStatus::Error) {
            detachClient(loop, writable_fd);
            return;
        }
        if (writable_session->replyBuffer().empty()) {
            loop.removeFileEvent(writable_fd, FileEventMask::Writable);
            if (writable_session->closeAfterReply()) {
                detachClient(loop, writable_fd);
            }
        }
    });
}

bool hyper::RedisServer::adoptClient_(EventLoop& loop, int fd) {
    if (attachClient(loop, fd)) {
        owned_client_fds_.insert(fd);
        return true;
    }
    ::close(fd);
    return false;
}
