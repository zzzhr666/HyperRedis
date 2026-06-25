#include "hyper/server/redis_server.hpp"

#include <cerrno>
#include <fcntl.h>
#include <fstream>
#include <unistd.h>
#include <utility>
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <sys/wait.h>

#include "hyper/server/client_socket_io.hpp"
#include "hyper/server/command_registry.hpp"
#include "hyper/server/event_loop.hpp"
#include "hyper/storage/aof_appender.hpp"
#include "hyper/storage/rdb_saver.hpp"
#include "hyper/time.hpp"
#include "hyper/version.hpp"
#include "hyper/server/server_options.hpp"
#include "hyper/storage/aof_replayer.hpp"
#include "hyper/storage/aof_rewriter.hpp"

namespace {
    constexpr std::size_t CheckKeysNumber = 20;
    constexpr std::size_t DefaultMaxClients = 1024;
}

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
      processor_(aof_appender_.get()), last_save_time_(ExpireClock::now()), save_rdb_on_stop_(false),
      dirty_count_(0), total_commands_(0), max_clients_(DefaultMaxClients), start_time_(ExpireClock::now()),
      rdb_child_pid_(-1) {}

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
    ++total_commands_;
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

    if (res && res->command_name == CommandName::Save && !std::holds_alternative<RespError>(execute_res)) {
        if (!hasRdbSaver()) {
            return respError("ERR save is not configured");
        }
        bool save_success = saveRdb(now);
        if (!save_success) {
            return respError("ERR save failed");
        }
        dirty_count_ = 0;
        last_save_time_ = now;
    }

    if (res && res->command_name == CommandName::BgSave && !std::holds_alternative<RespError>(execute_res)) {
        return bgSave(now);
    }

    if (res && res->command_name == CommandName::LastSave && !std::holds_alternative<RespError>(execute_res)) {
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(last_save_time_.time_since_epoch());
        return respInteger(seconds.count());
    }

    if (res && res->command_name == CommandName::Info && !std::holds_alternative<RespError>(execute_res)) {
        std::string info_string = generateInfoString_(args);
        return respBulk(std::move(info_string));
    }

    if (res && res->command_name == CommandName::Config) {
        return handleConfig_(args);
    }

    if (res && res->command_name == CommandName::RewriteAof && !std::holds_alternative<RespError>(execute_res)) {
        return rewriteAof_(now);
    }

    if (res && res->command_name == CommandName::BgRewriteAof && !std::holds_alternative<RespError>(execute_res)) {
        return bgRewriteAof(now);
    }

    if (res && res->command_name == CommandName::Publish && !std::holds_alternative<RespError>(execute_res)) {
        return respInteger(publish(std::string(args[1]), std::string(args[2])));
    }

    if (res && res->command_name == CommandName::Subscribe && !std::holds_alternative<RespError>(execute_res)) {
        for (int i = 1; i < args.size(); ++i) {
            std::string channel(args[i]);
            subscribe(client.session(), channel);


            auto reply = std::make_shared<RespArray>();
            reply->values.push_back(respBulk("subscribe"));
            reply->values.push_back(respBulk(channel));
            reply->values.push_back(respInteger(static_cast<std::int64_t>(client.pubsubChannels().size())));
            if (i < args.size() - 1) {
                client.session()->appendReply(reply);
            } else {
                return reply;
            }
        }
    }
    if (res && res->command_name == CommandName::Unsubscribe && !std::holds_alternative<RespError>(execute_res)) {
        std::vector<std::string> channels;
        if (args.size() == 1) {
            channels = std::vector<std::string>(client.pubsubChannels().begin(), client.pubsubChannels().end());
        } else {
            channels = std::vector<std::string>(args.begin() + 1, args.end());
        }

        if (channels.empty()) {
            auto reply = std::make_shared<RespArray>();
            reply->values.push_back(respBulk("unsubscribe"));
            reply->values.push_back(respNullBulk());
            reply->values.push_back(respInteger(0));
            return reply;
        }

        for (int i = 0; i < channels.size(); ++i) {
            unsubscribe(client.session(), channels[i]);

            auto reply = std::make_shared<RespArray>();
            reply->values.push_back(respBulk("unsubscribe"));
            reply->values.push_back(respBulk(channels[i]));
            reply->values.push_back(respInteger(static_cast<std::int64_t>(client.pubsubChannels().size())));

            if (i == channels.size() - 1) {
                return reply;
            } else {
                client.session()->appendReply(reply);
            }
        }
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
        unsubscribeAll(&it->second);
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
        SPDLOG_INFO("Loading RDB from {}", rdb_saver_->path().string());
        return rdb_saver_->load(manager_, now);
    }
    return false;
}

bool hyper::RedisServer::saveRdb(ExpireTimePoint now) {
    if (hasActiveChildProcess()) {
        return false;
    }
    if (hasRdbSaver()) {
        SPDLOG_INFO("Saving RDB to {}", rdb_saver_->path().string());
        return rdb_saver_->save(manager_, now);
    }
    return false;
}

hyper::RespValue hyper::RedisServer::bgSave(ExpireTimePoint now) {
    if (!hasRdbSaver()) {
        return respError("ERR save is not configured");
    }
    if (hasActiveChildProcess()) {
        return respError("ERR background save already in progress");
    }
    SPDLOG_INFO("Starting BGSAVE");
    pid_t pid = ::fork();
    if (pid == 0) {
        //child progress
        bool success = rdb_saver_->save(manager_, now);
        std::_Exit(success ? 0 : 1);
    }
    if (pid == -1) {
        //fork() failed
        SPDLOG_ERROR("BGSAVE fork failed");
        return respError("ERR can't bgsave: fork failed");
    }

    //parent progress
    rdb_child_pid_ = pid;
    return respBulk("background saving started");
}

hyper::RespValue hyper::RedisServer::bgRewriteAof(ExpireTimePoint now) {
    if (!hasAofAppender()) {
        return respError("ERR aof is not enabled");
    }
    if (hasActiveChildProcess()) {
        return respError("ERR background append only file rewriting already in progress");
    }
    SPDLOG_INFO("Starting BGREWRITEAOF");
    aof_appender_->startRewrite();
    pid_t pid = ::fork();
    if (pid == 0) {
        // child process
        auto target_path = aof_appender_->path();
        std::filesystem::path tmp_path(target_path.string() + ".bg-rewrite.tmp");
        bool success = AofRewriter::rewrite(tmp_path, manager_, now);
        std::_Exit(success ? 0 : 1);
    }

    if (pid == -1) {
        //fork failed
        SPDLOG_ERROR("BGREWRITEAOF fork failed");
        (void)aof_appender_->stopRewrite();
        return respError("ERR can't bgrewriteaof: fork failed");
    }

    // parent process
    aof_child_pid_ = pid;
    return respBulk("background append only file rewriting started");
}

bool hyper::RedisServer::loadAof(ExpireTimePoint now) {
    if (hasAofAppender()) {
        SPDLOG_INFO("Loading AOF from {}", aof_appender_->path().string());
        auto [replay_success,current_db_index] = AofReplayer::replay(aof_appender_->path(), manager_, now);
        if (!replay_success) {
            SPDLOG_ERROR("AOF replay failed for {}", aof_appender_->path().string());
            return false;
        }
        aof_appender_->setSelectedDbIndex(current_db_index);
        SPDLOG_INFO("AOF replay successful, selected DB index: {}", current_db_index);
        return true;
    }
    return false;
}

std::size_t hyper::RedisServer::serverCron(EventLoop& loop, ExpireTimePoint now) {
    std::size_t done = activeExpireCycle(now, CheckKeysNumber);
    checkChildrenDone();
    if (timeout_seconds_.count() > 0) {
        std::vector<int> fds;
        for (const auto& [fd,session] : client_sessions_) {
            if (now - session.lastInteractionTime() > timeout_seconds_) {
                fds.emplace_back(fd);
            }
        }
        for (int fd : fds) {
            detachClient(loop, fd);
            SPDLOG_INFO("Closing idle connection:fd={}", fd);
        }
    }
    if (aof_appender_) {
        aof_appender_->flushIfNeeded(now);
    }
    return done;
}

bool hyper::RedisServer::subscribe(ClientSession* client, const std::string& channel) {
    if (client->context().addPubSubChannel(channel)) {
        sub_clients_[channel].insert(client);
        return true;
    }
    return false;
}

bool hyper::RedisServer::unsubscribe(ClientSession* client, const std::string& channel) {
    if (client->context().removePubSubChannel(channel)) {
        if (auto it = sub_clients_.find(channel); it != sub_clients_.end()) {
            it->second.erase(client);
            if (it->second.empty()) {
                sub_clients_.erase(it);
            }
        }

        return true;
    }
    return false;
}

void hyper::RedisServer::unsubscribeAll(ClientSession* client) {
    auto& sub_channels = client->context().pubsubChannels();
    for (auto& channel : sub_channels) {
        sub_clients_[channel].erase(client);
        if (sub_clients_[channel].empty()) {
            sub_clients_.erase(channel);
        }
    }
    client->context().clearChannels();
}

int hyper::RedisServer::publish(const std::string& channel, const std::string& message) {
    auto it = sub_clients_.find(channel);
    if (it == sub_clients_.end() || it->second.empty()) {
        return 0;
    }
    auto reply = std::make_shared<RespArray>();
    reply->values.push_back(respBulk("message"));
    reply->values.push_back(respBulk(channel));
    reply->values.push_back(respBulk(message));

    for (auto* client : it->second) {
        client->appendReply(reply);
        if (current_loop_) {
            enableClientWritable_(*current_loop_, client->fd());
        }
    }

    return static_cast<int>(it->second.size());
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
    if (clientCount() >= max_clients_) {
        sendImmediateErrorAndClose(fd, "max number of clients reached");
        return false;
    }
    if (attachClient(loop, fd)) {
        owned_client_fds_.insert(fd);
        return true;
    }
    ::close(fd);
    return false;
}

hyper::RespValue hyper::RedisServer::handleConfig_(CommandExecutor::Args args) {
    std::string command(args[1]);
    std::ranges::transform(command, command.begin(), [](unsigned char c) {
        return std::toupper(c);
    });

    if (command == "GET") {
        if (args.size() < 3) {
            return respError("ERR wrong number of arguments for CONFIG GET");
        }
        std::string key(args[2]);
        std::ranges::transform(key, key.begin(), [](unsigned char c) {
            return std::tolower(c);
        });

        auto ret = std::make_shared<RespArray>();
        if (key == "appendfsync") {
            if (hasAofAppender()) {
                ret->values.emplace_back(respBulk(key));
                ret->values.emplace_back(respBulk(policyToString(aof_appender_->fsyncPolicy())));
            }
        } else if (key == "databases") {
            ret->values.emplace_back(respBulk(key));
            ret->values.emplace_back(respBulk(std::to_string(manager_.dbCount())));
        } else if (key == "save-rdb-on-stop") {
            ret->values.emplace_back(respBulk(key));
            ret->values.emplace_back(respBulk(save_rdb_on_stop_ ? "yes" : "no"));
        } else if (key == "aof-path") {
            if (hasAofAppender()) {
                ret->values.emplace_back(respBulk(key));
                ret->values.emplace_back(respBulk(aof_appender_->path().string()));
            }
        } else if (key == "rdb-path") {
            if (hasRdbSaver()) {
                ret->values.emplace_back(respBulk(key));
                ret->values.emplace_back(respBulk(rdb_saver_->path().string()));
            }
        } else if (key == "maxclients") {
            ret->values.emplace_back(respBulk(key));
            ret->values.emplace_back(respBulk(std::to_string(max_clients_)));
        } else if (key == "timeout") {
            ret->values.emplace_back(respBulk(key));
            ret->values.emplace_back(respBulk(std::to_string(timeout_seconds_.count())));
        }
        return ret;
    }

    if (command == "SET") {
        if (args.size() != 4) {
            return respError("ERR wrong number of arguments for CONFIG SET");
        }
        std::string key(args[2]);
        std::ranges::transform(key, key.begin(), [](unsigned char c) {
            return std::tolower(c);
        });

        if (key == "appendfsync") {
            AofFsyncPolicy policy;
            if (!parseAofFsyncPolicy(args[3], policy)) {
                return respError(std::string(ErrSyntaxError));
            }
            if (hasAofAppender()) {
                aof_appender_->setFsyncPolicy(policy);
                return respOk();
            }
            return respError("ERR AOF is not enabled");
        }

        if (key == "save-rdb-on-stop") {
            std::string val(args[3]);
            std::ranges::transform(val, val.begin(), [](unsigned char c) {
                return std::tolower(c);
            });
            if (val == "yes") {
                save_rdb_on_stop_ = true;
                return respOk();
            } else if (val == "no") {
                save_rdb_on_stop_ = false;
                return respOk();
            }
            return respError(std::string(ErrSyntaxError));
        }

        if (key == "maxclients") {
            std::size_t max_size;
            auto& size_str = args[3];
            const auto* first = size_str.data();
            const auto* last = first + size_str.size();
            auto [ptr, ec] = std::from_chars(first, last, max_size);
            if (ptr == last && ec == std::errc()) {
                setMaxClients(max_size);
                return respOk();
            }
            return respError(std::string(ErrInvalidInteger));
        }

        if (key == "timeout") {
            std::uint32_t seconds;
            auto& sec_str = args[3];
            const auto* first = sec_str.data();
            const auto* last = first + sec_str.size();
            auto [ptr, ec] = std::from_chars(first, last, seconds);
            if (ptr == last && ec == std::errc()) {
                setTimeout(seconds);
                return respOk();
            }
            return respError(std::string(ErrInvalidInteger));
        }

        if (key == "databases" || key == "aof-path" || key == "rdb-path") {
            return respError("ERR Constant configuration parameter cannot be modified");
        }
    }

    return respError(std::string(ErrSyntaxError));
}

std::string hyper::RedisServer::generateInfoString_(CommandExecutor::Args args) {
    std::string section = "all";
    if (args.size() > 1) {
        section = args[1];
        std::ranges::transform(section, section.begin(), [](unsigned char c) {
            return std::tolower(c);
        });
    }
    bool all = section == "all";
    std::string res;
    if (all || section == "server") {
        res.append("# Server\r\n");
        res.append("hyper_redis_version:" + std::string(ProjectVersion) + "\r\n");
        res.append("rdb_version:" + std::string(RdbVersion) + "\r\n");
        auto uptime = std::chrono::duration_cast<std::chrono::seconds>(ExpireClock::now() - start_time_);
        res.append("uptime_in_seconds:" + std::to_string(uptime.count()) + "\r\n");
        res.append("os:linux\r\n");
    }

    if (all || section == "client") {
        if (all) {
            res.append("\r\n");
        }
        res.append("# Clients\r\n");
        res.append("connected_clients:" + std::to_string(client_sessions_.size()) + "\r\n");
    }

    if (all || section == "stats") {
        if (all) {
            res.append("\r\n");
        }
        res.append("# Stats\r\n");
        res.append("total_commands_processed:" + std::to_string(total_commands_) + "\r\n");
    }

    if (all || section == "persistence") {
        if (all) {
            res.append("\r\n");
        }
        res.append("# Persistence\r\n");
        res.append("rdb_changes_since_last_save:" + std::to_string(dirty_count_) + "\r\n");
        auto last_save_seconds = std::chrono::duration_cast<std::chrono::seconds>(last_save_time_.time_since_epoch());
        res.append("rdb_last_save_time:" + std::to_string(last_save_seconds.count()) + "\r\n");
    }

    if (all || section == "keyspace") {
        if (all) {
            res.append("\r\n");
        }
        res.append("# Keyspace\r\n");
        for (std::size_t i = 0; i < manager_.dbCount(); ++i) {
            if (auto db = manager_.db(i); db && db->size() > 0) {
                res.append(
                    "db" + std::to_string(i) + ":keys=" + std::to_string(db->size()) + ",expires=" + std::to_string(
                        db->expireSize()) + "\r\n");
            }
        }
    }

    return res;
}

hyper::RespValue hyper::RedisServer::rewriteAof_(ExpireTimePoint now) {
    if (!hasAofAppender()) {
        return respError("ERR AOF is not enabled");
    }

    auto target_path = aof_appender_->path();
    std::filesystem::path tmp_path(target_path.string() + ".tmp");

    SPDLOG_INFO("AOF Rewrite: target={}, tmp={}", target_path.string(), tmp_path.string());

    if (!AofRewriter::rewrite(tmp_path, manager_, now)) {
        std::filesystem::remove(tmp_path);
        SPDLOG_ERROR("AOF Rewrite: serialization failed");
        return respError("ERR AOF rewrite failed during serialization");
    }

    std::error_code ec;
    std::filesystem::rename(tmp_path, target_path, ec);
    if (ec) {
        std::filesystem::remove(tmp_path);
        SPDLOG_ERROR("AOF Rewrite: rename failed: {}", ec.message());
        return respError("ERR AOF rewrite rename failed: " + ec.message());
    }

    aof_appender_->reload();
    SPDLOG_INFO("AOF Rewrite: completed successfully");
    return respOk();
}

void hyper::RedisServer::checkChildrenDone() {
    if (!hasActiveChildProcess()) {
        return;
    }
    int stat_loc;
    pid_t pid = waitpid(-1, &stat_loc,WNOHANG);

    if (pid == rdb_child_pid_) {
        rdb_child_pid_ = -1;
        int exit_code = WEXITSTATUS(stat_loc);
        if (exit_code == 0) {
            SPDLOG_INFO("background saveing terminated with success");
            last_save_time_ = ExpireClock::now();
        } else {
            SPDLOG_ERROR("back ground saving terminated with error code {}", exit_code);
        }
    } else if (pid == aof_child_pid_) {
        aof_child_pid_ = -1;
        int exit_code = WEXITSTATUS(stat_loc);
        if (exit_code == 0) {
            SPDLOG_INFO("Background AOF rewrite terminated with success");
            std::string diff = aof_appender_->stopRewrite();
            auto target_path = aof_appender_->path();
            std::filesystem::path tmp_path(target_path.string() + ".bg-rewrite.tmp");
            std::ofstream out(tmp_path, std::ios::binary | std::ios::app);
            if (out) {
                out.write(diff.data(), static_cast<std::streamsize>(diff.size()));
                out.close();
            }
            std::error_code ec;
            std::filesystem::rename(tmp_path, target_path, ec);
            if (!ec) {
                aof_appender_->reload();
                SPDLOG_INFO("background AOF rewrite successful and swapped");
            } else {
                SPDLOG_ERROR("failed to rename new AOF file:{}", ec.message());
            }
        } else {
            SPDLOG_ERROR("Background AOF rewrite terminated with error code {}", exit_code);
        }
    }
}
