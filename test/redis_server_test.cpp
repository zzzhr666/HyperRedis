#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>

#include "hyper/server/client_context.hpp"
#include "hyper/server/event_loop.hpp"
#include "hyper/server/redis_server.hpp"
#include "hyper/server/resp_codec.hpp"
#include "hyper/server/resp_value.hpp"
#include "hyper/server/tcp_listener.hpp"
#include "hyper/time.hpp"

using namespace hyper;

namespace {
    class SocketPair {
    public:
        SocketPair() {
            EXPECT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds_.data()), 0);
        }

        SocketPair(const SocketPair&) = delete;
        SocketPair& operator=(const SocketPair&) = delete;

        ~SocketPair() {
            for (const auto fd : fds_) {
                if (fd >= 0) {
                    close(fd);
                }
            }
        }

        [[nodiscard]] int serverFd() const noexcept {
            return fds_[0];
        }

        [[nodiscard]] int peerFd() const noexcept {
            return fds_[1];
        }

        void closePeer() noexcept {
            if (fds_[1] >= 0) {
                close(fds_[1]);
                fds_[1] = -1;
            }
        }

    private:
        std::array<int, 2> fds_{{-1, -1}};
    };

    class LocalStreamListener {
    public:
        LocalStreamListener() {
            fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
            if (fd_ < 0) {
                recordError_("socket");
                return;
            }

            path_ = "/tmp/hyperredis-test-" + std::to_string(getpid()) + "-" + std::to_string(fd_) + ".sock";
            if (path_.size() >= sizeof(sockaddr_un::sun_path)) {
                error_stage_ = "path";
                error_errno_ = ENAMETOOLONG;
                return;
            }

            sockaddr_un addr{};
            addr.sun_family = AF_UNIX;
            std::memcpy(addr.sun_path, path_.c_str(), path_.size() + 1);

            unlink(path_.c_str());
            if (bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
                recordError_("bind");
                return;
            }
            if (listen(fd_, SOMAXCONN) != 0) {
                recordError_("listen");
                return;
            }
            ready_ = true;
        }

        LocalStreamListener(const LocalStreamListener&) = delete;
        LocalStreamListener& operator=(const LocalStreamListener&) = delete;

        ~LocalStreamListener() {
            if (fd_ >= 0) {
                close(fd_);
            }
            if (!path_.empty()) {
                unlink(path_.c_str());
            }
        }

        [[nodiscard]] bool valid() const noexcept {
            return ready_;
        }

        [[nodiscard]] bool unavailableByPermission() const noexcept {
            return error_errno_ == EPERM || error_errno_ == EACCES;
        }

        [[nodiscard]] const char* errorStage() const noexcept {
            return error_stage_;
        }

        [[nodiscard]] int errorNumber() const noexcept {
            return error_errno_;
        }

        [[nodiscard]] int fd() const noexcept {
            return fd_;
        }

        [[nodiscard]] const std::string& path() const noexcept {
            return path_;
        }

    private:
        void recordError_(const char* stage) noexcept {
            error_stage_ = stage;
            error_errno_ = errno;
        }

        int fd_{-1};
        std::string path_;
        bool ready_{false};
        const char* error_stage_{"none"};
        int error_errno_{0};
    };

    class LocalStreamClient {
    public:
        explicit LocalStreamClient(const std::string& path) {
            fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
            EXPECT_GE(fd_, 0);
            if (fd_ < 0) {
                return;
            }

            EXPECT_LT(path.size(), sizeof(sockaddr_un::sun_path));
            if (path.size() >= sizeof(sockaddr_un::sun_path)) {
                return;
            }

            sockaddr_un addr{};
            addr.sun_family = AF_UNIX;
            std::memcpy(addr.sun_path, path.c_str(), path.size() + 1);

            if (connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
                connected_ = true;
            } else {
                ADD_FAILURE() << "connect failed with errno " << errno;
            }
        }

        LocalStreamClient(const LocalStreamClient&) = delete;
        LocalStreamClient& operator=(const LocalStreamClient&) = delete;

        ~LocalStreamClient() {
            if (fd_ >= 0) {
                close(fd_);
            }
        }

        [[nodiscard]] bool valid() const noexcept {
            return connected_;
        }

        [[nodiscard]] int fd() const noexcept {
            return fd_;
        }

    private:
        int fd_{-1};
        bool connected_{false};
    };

    class TcpClient {
    public:
        explicit TcpClient(std::uint16_t port) {
            fd_ = socket(AF_INET, SOCK_STREAM, 0);
            if (fd_ < 0) {
                recordError_("socket");
                return;
            }

            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            addr.sin_port = htons(port);

            if (connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
                recordError_("connect");
                return;
            }
            connected_ = true;
        }

        TcpClient(const TcpClient&) = delete;
        TcpClient& operator=(const TcpClient&) = delete;

        ~TcpClient() {
            if (fd_ >= 0) {
                close(fd_);
            }
        }

        [[nodiscard]] int fd() const noexcept {
            return fd_;
        }

        [[nodiscard]] bool valid() const noexcept {
            return connected_;
        }

        [[nodiscard]] const char* errorStage() const noexcept {
            return error_stage_;
        }

        [[nodiscard]] int errorNumber() const noexcept {
            return error_errno_;
        }

    private:
        void recordError_(const char* stage) noexcept {
            error_stage_ = stage;
            error_errno_ = errno;
        }

        int fd_{-1};
        bool connected_{false};
        const char* error_stage_{"none"};
        int error_errno_{0};
    };

    [[nodiscard]] bool shouldRunTcpListenerTests() noexcept {
        const char* value = std::getenv("HYPERREDIS_RUN_TCP_LISTENER_TESTS");
        return value != nullptr && std::string_view{value} == "1";
    }

    [[nodiscard]] int runLoopUntilEvents(EventLoop& loop, int expected_events) {
        int fired{};
        for (int i = 0; i < 10 && fired < expected_events; ++i) {
            const int n = loop.runOnce(std::chrono::milliseconds{10});
            if (n > 0) {
                fired += n;
            }
        }
        return fired;
    }

    [[nodiscard]] std::string readReplyWhileRunningLoop(EventLoop& loop, int fd, std::string_view expected) {
        std::string reply;
        std::array<char, 64> buffer{};
        for (int i = 0; i < 20 && reply.size() < expected.size(); ++i) {
            (void)loop.runOnce(std::chrono::milliseconds{10});
            while (reply.size() < expected.size()) {
                const auto n = recv(fd, buffer.data(), buffer.size(), MSG_DONTWAIT);
                if (n > 0) {
                    reply.append(buffer.data(), static_cast<std::size_t>(n));
                    continue;
                }
                if (n == 0) {
                    return reply;
                }
                if (errno == EINTR) {
                    continue;
                }
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                return reply;
            }
        }
        return reply;
    }

    [[nodiscard]] std::string commandBytes(std::initializer_list<std::string_view> args) {
        std::vector<std::string_view> values{args};
        return serializeRespCommand(values);
    }

    [[nodiscard]] ExpireTimePoint makeTime(UnixMilliseconds ms) {
        return ExpireTimePoint{Milliseconds{ms}};
    }

    template<std::size_t N>
    [[nodiscard]] constexpr std::string_view literalBytes(const char (&value)[N]) noexcept {
        return {value, N - 1};
    }

    void expectSimpleString(const RespValue& value, std::string_view expected) {
        const auto* simple = std::get_if<RespSimpleString>(&value);
        ASSERT_NE(simple, nullptr);
        EXPECT_EQ(simple->value, expected);
    }

    void expectBulkString(const RespValue& value, std::string_view expected) {
        const auto* bulk = std::get_if<RespBulkString>(&value);
        ASSERT_NE(bulk, nullptr);
        ASSERT_TRUE(bulk->value.has_value());
        EXPECT_EQ(*bulk->value, expected);
    }

    void expectInteger(const RespValue& value, std::int64_t expected) {
        const auto* integer = std::get_if<RespInteger>(&value);
        ASSERT_NE(integer, nullptr);
        EXPECT_EQ(integer->value, expected);
    }

    void expectError(const RespValue& value, std::string_view expected) {
        const auto* error = std::get_if<RespError>(&value);
        ASSERT_NE(error, nullptr);
        EXPECT_EQ(error->message, expected);
    }
}

TEST(RedisServerTest, SupportsCustomDbCount) {
    RedisServer server(3);

    EXPECT_EQ(server.manager().dbCount(), 3);
    EXPECT_EQ(server.dirtyCount(), 0);
}

TEST(RedisServerTest, StartsWithoutClients) {
    RedisServer server;
    const RedisServer& const_server = server;

    EXPECT_EQ(server.clientCount(), 0U);
    EXPECT_EQ(server.clientSession(10), nullptr);
    EXPECT_EQ(const_server.clientSession(10), nullptr);
}

TEST(RedisServerTest, AddClientStoresSessionByFd) {
    RedisServer server;

    EXPECT_TRUE(server.addClient(10));

    auto* client = server.clientSession(10);
    ASSERT_NE(client, nullptr);
    EXPECT_EQ(client->fd(), 10);
    EXPECT_EQ(server.clientCount(), 1U);

    const RedisServer& const_server = server;
    const auto* const_client = const_server.clientSession(10);
    ASSERT_NE(const_client, nullptr);
    EXPECT_EQ(const_client->fd(), 10);
}

TEST(RedisServerTest, AddClientRejectsInvalidFd) {
    RedisServer server;

    EXPECT_FALSE(server.addClient(-1));

    EXPECT_EQ(server.clientCount(), 0U);
    EXPECT_EQ(server.clientSession(-1), nullptr);
}

TEST(RedisServerTest, AddClientRejectsDuplicateFdWithoutOverwritingSession) {
    RedisServer server;

    ASSERT_TRUE(server.addClient(10));
    auto* client = server.clientSession(10);
    ASSERT_NE(client, nullptr);
    client->appendQueryBytes("partial");

    EXPECT_FALSE(server.addClient(10));

    client = server.clientSession(10);
    ASSERT_NE(client, nullptr);
    EXPECT_EQ(client->fd(), 10);
    EXPECT_EQ(client->queryBuffer(), "partial");
    EXPECT_EQ(server.clientCount(), 1U);
}

TEST(RedisServerTest, RemoveClientErasesSession) {
    RedisServer server;

    ASSERT_TRUE(server.addClient(10));

    EXPECT_TRUE(server.removeClient(10));
    EXPECT_EQ(server.clientCount(), 0U);
    EXPECT_EQ(server.clientSession(10), nullptr);
    EXPECT_FALSE(server.removeClient(10));
}

TEST(RedisServerTest, AttachClientReadableEventQueuesReply) {
    RedisServer server;
    EventLoop loop;
    SocketPair sockets;
    const auto ping = serializeRespCommand(std::array<std::string_view, 1>{"PING"});

    ASSERT_TRUE(server.attachClient(loop, sockets.serverFd()));
    ASSERT_EQ(write(sockets.peerFd(), ping.data(), ping.size()), static_cast<ssize_t>(ping.size()));

    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 1);

    auto* client = server.clientSession(sockets.serverFd());
    ASSERT_NE(client, nullptr);
    EXPECT_TRUE(client->queryBuffer().empty());
    EXPECT_EQ(client->replyBuffer(), literalBytes("+PONG\r\n"));
    EXPECT_EQ(server.clientCount(), 1U);
}

TEST(RedisServerTest, AttachClientWritableEventFlushesReplyToPeer) {
    RedisServer server;
    EventLoop loop;
    SocketPair sockets;
    const auto ping = serializeRespCommand(std::array<std::string_view, 1>{"PING"});

    ASSERT_TRUE(server.attachClient(loop, sockets.serverFd()));
    ASSERT_EQ(write(sockets.peerFd(), ping.data(), ping.size()), static_cast<ssize_t>(ping.size()));
    ASSERT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 1);

    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 1);

    std::array<char, 16> buffer{};
    const auto n = read(sockets.peerFd(), buffer.data(), buffer.size());
    ASSERT_GT(n, 0);
    EXPECT_EQ(std::string_view(buffer.data(), static_cast<std::size_t>(n)), literalBytes("+PONG\r\n"));

    auto* client = server.clientSession(sockets.serverFd());
    ASSERT_NE(client, nullptr);
    EXPECT_TRUE(client->replyBuffer().empty());
}

TEST(RedisServerTest, AttachedClientUsesCurrentTimeForExpiration) {
    RedisServer server;
    EventLoop loop;
    SocketPair sockets;
    ASSERT_TRUE(server.attachClient(loop, sockets.serverFd()));

    const std::array<std::string, 4> commands{
        commandBytes({"SET", "key", "value"}),
        commandBytes({"PEXPIRE", "key", "1"}),
        commandBytes({"GET", "key"}),
        commandBytes({"GET", "key"})
    };

    ASSERT_EQ(write(sockets.peerFd(), commands[0].data(), commands[0].size()),
              static_cast<ssize_t>(commands[0].size()));
    EXPECT_EQ(readReplyWhileRunningLoop(loop, sockets.peerFd(), literalBytes("+OK\r\n")),
              literalBytes("+OK\r\n"));

    ASSERT_EQ(write(sockets.peerFd(), commands[1].data(), commands[1].size()),
              static_cast<ssize_t>(commands[1].size()));
    EXPECT_EQ(readReplyWhileRunningLoop(loop, sockets.peerFd(), literalBytes(":1\r\n")),
              literalBytes(":1\r\n"));

    ASSERT_EQ(write(sockets.peerFd(), commands[2].data(), commands[2].size()),
              static_cast<ssize_t>(commands[2].size()));
    EXPECT_EQ(readReplyWhileRunningLoop(loop, sockets.peerFd(), literalBytes("$5\r\nvalue\r\n")),
              literalBytes("$5\r\nvalue\r\n"));

    std::this_thread::sleep_for(std::chrono::milliseconds{5});

    ASSERT_EQ(write(sockets.peerFd(), commands[3].data(), commands[3].size()),
              static_cast<ssize_t>(commands[3].size()));
    EXPECT_EQ(readReplyWhileRunningLoop(loop, sockets.peerFd(), literalBytes("$-1\r\n")),
              literalBytes("$-1\r\n"));
}

TEST(RedisServerTest, AttachClientDetachesWhenPeerCloses) {
    RedisServer server;
    EventLoop loop;
    SocketPair sockets;

    ASSERT_TRUE(server.attachClient(loop, sockets.serverFd()));

    sockets.closePeer();

    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 1);
    EXPECT_EQ(server.clientSession(sockets.serverFd()), nullptr);
    EXPECT_EQ(server.clientCount(), 0U);
}

TEST(RedisServerTest, AttachListenerSetsListenFdNonBlocking) {
    RedisServer server;
    EventLoop loop;
    SocketPair sockets;

    ASSERT_TRUE(server.attachListener(loop, sockets.serverFd()));

    const int flags = fcntl(sockets.serverFd(), F_GETFL, 0);
    ASSERT_GE(flags, 0);
    EXPECT_NE(flags & O_NONBLOCK, 0);
}

TEST(RedisServerTest, AttachListenerAcceptsPendingClient) {
    RedisServer server;
    EventLoop loop;
    LocalStreamListener listener;
    if (!listener.valid() && listener.unavailableByPermission()) {
        GTEST_SKIP() << "listening sockets are not permitted in this environment";
    }
    ASSERT_TRUE(listener.valid()) << listener.errorStage() << " failed with errno " << listener.errorNumber();

    ASSERT_TRUE(server.attachListener(loop, listener.fd()));
    LocalStreamClient client(listener.path());
    ASSERT_TRUE(client.valid());

    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 1);
    EXPECT_EQ(server.clientCount(), 1U);
}

TEST(RedisServerTest, AttachedListenerClientCanProcessPing) {
    RedisServer server;
    EventLoop loop;
    LocalStreamListener listener;
    if (!listener.valid() && listener.unavailableByPermission()) {
        GTEST_SKIP() << "listening sockets are not permitted in this environment";
    }
    ASSERT_TRUE(listener.valid()) << listener.errorStage() << " failed with errno " << listener.errorNumber();
    const auto ping = serializeRespCommand(std::array<std::string_view, 1>{"PING"});

    ASSERT_TRUE(server.attachListener(loop, listener.fd()));
    LocalStreamClient client(listener.path());
    ASSERT_TRUE(client.valid());
    ASSERT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 1);
    ASSERT_EQ(write(client.fd(), ping.data(), ping.size()), static_cast<ssize_t>(ping.size()));

    EXPECT_EQ(readReplyWhileRunningLoop(loop, client.fd(), literalBytes("+PONG\r\n")),
              literalBytes("+PONG\r\n"));
}

TEST(RedisServerTcpIntegrationTest, ProductionTcpListenerAcceptsClientAndProcessesPing) {
    if (!shouldRunTcpListenerTests()) {
        GTEST_SKIP() << "set HYPERREDIS_RUN_TCP_LISTENER_TESTS=1 to run TCP listener integration tests";
    }

    TcpListenOptions options;
    options.port = 0;
    auto listener = TcpListener::create(options);
    ASSERT_TRUE(listener.has_value());

    RedisServer server;
    EventLoop loop;
    const auto ping = serializeRespCommand(std::array<std::string_view, 1>{"PING"});

    ASSERT_TRUE(server.attachListener(loop, listener->fd()));
    TcpClient client(listener->port());
    ASSERT_TRUE(client.valid()) << client.errorStage() << " failed with errno " << client.errorNumber();

    ASSERT_EQ(runLoopUntilEvents(loop, 1), 1);
    ASSERT_EQ(server.clientCount(), 1U);
    ASSERT_EQ(write(client.fd(), ping.data(), ping.size()), static_cast<ssize_t>(ping.size()));

    EXPECT_EQ(readReplyWhileRunningLoop(loop, client.fd(), literalBytes("+PONG\r\n")),
              literalBytes("+PONG\r\n"));
}

TEST(RedisServerTest, DetachListenerStopsAcceptingNewClients) {
    RedisServer server;
    EventLoop loop;
    SocketPair sockets;

    ASSERT_TRUE(server.attachListener(loop, sockets.serverFd()));
    server.detachListener(loop, sockets.serverFd());
    ASSERT_EQ(write(sockets.peerFd(), "x", 1), 1);

    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 0);
    EXPECT_EQ(server.clientCount(), 0U);
}

TEST(RedisServerTest, ExecuteCanSetAndGetThroughClientContext) {
    RedisServer server;
    RedisClientContext client;
    const auto now = makeTime(1'000);

    const std::array<std::string_view, 3> set_args{"SET", "name", "hyper"};
    const std::array<std::string_view, 2> get_args{"GET", "name"};

    expectSimpleString(server.execute(client, set_args, now), "OK");
    expectBulkString(server.execute(client, get_args, now), "hyper");
}

TEST(RedisServerTest, ReadCommandDoesNotIncreaseDirtyCount) {
    RedisServer server;
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> set_args{"SET", "name", "hyper"};
    const std::array<std::string_view, 2> get_args{"GET", "name"};

    expectSimpleString(server.execute(client, set_args, now), "OK");
    ASSERT_EQ(server.dirtyCount(), 1);

    expectBulkString(server.execute(client, get_args, now), "hyper");

    EXPECT_EQ(server.dirtyCount(), 1);
}

TEST(RedisServerTest, SuccessfulWriteCommandIncreasesDirtyCount) {
    RedisServer server;
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> first_set{"SET", "first", "1"};
    const std::array<std::string_view, 3> second_set{"SET", "second", "2"};

    expectSimpleString(server.execute(client, first_set, now), "OK");
    EXPECT_EQ(server.dirtyCount(), 1);

    expectSimpleString(server.execute(client, second_set, now), "OK");
    EXPECT_EQ(server.dirtyCount(), 2);
}

TEST(RedisServerTest, FailedWriteCommandDoesNotIncreaseDirtyCount) {
    RedisServer server;
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> set_args{"SET", "key", "not-an-int"};
    const std::array<std::string_view, 2> incr_args{"INCR", "key"};

    expectSimpleString(server.execute(client, set_args, now), "OK");
    ASSERT_EQ(server.dirtyCount(), 1);

    expectError(server.execute(client, incr_args, now),
                "ERR value is not an integer or out of range");

    EXPECT_EQ(server.dirtyCount(), 1);
}

TEST(RedisServerTest, LowercaseWriteCommandStillIncreasesDirtyCount) {
    RedisServer server;
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> set_args{"set", "key", "value"};
    const std::array<std::string_view, 2> get_args{"get", "key"};

    expectSimpleString(server.execute(client, set_args, now), "OK");
    EXPECT_EQ(server.dirtyCount(), 1);

    expectBulkString(server.execute(client, get_args, now), "value");
    EXPECT_EQ(server.dirtyCount(), 1);
}

TEST(RedisServerTest, EmptyCommandReturnsErrorWithoutIncreasingDirtyCount) {
    RedisServer server;
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 0> args{};

    expectError(server.execute(client, args, now), "ERR empty command");

    EXPECT_EQ(server.dirtyCount(), 0);
}

TEST(RedisServerTest, ActiveExpireCycleRemovesExpiredKeysAcrossDatabases) {
    RedisServer server(2);
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> set_db0{"SET", "db0-key", "zero"};
    const std::array<std::string_view, 3> expire_db0{"PEXPIRE", "db0-key", "10"};
    const std::array<std::string_view, 2> select_db1{"SELECT", "1"};
    const std::array<std::string_view, 3> set_db1{"SET", "db1-key", "one"};
    const std::array<std::string_view, 3> expire_db1{"PEXPIRE", "db1-key", "10"};

    expectSimpleString(server.execute(client, set_db0, now), "OK");
    expectInteger(server.execute(client, expire_db0, now), 1);
    expectSimpleString(server.execute(client, select_db1, now), "OK");
    expectSimpleString(server.execute(client, set_db1, now), "OK");
    expectInteger(server.execute(client, expire_db1, now), 1);

    EXPECT_EQ(server.activeExpireCycle(now + Milliseconds{10}, 10), 2);

    ASSERT_NE(server.manager().db(0), nullptr);
    ASSERT_NE(server.manager().db(1), nullptr);
    EXPECT_EQ(server.manager().db(0)->get("db0-key", now + Milliseconds{10}), nullptr);
    EXPECT_EQ(server.manager().db(1)->get("db1-key", now + Milliseconds{10}), nullptr);
}

TEST(RedisServerTest, ActiveExpireCycleIncreasesDirtyCountByDeletedKeys) {
    RedisServer server(2);
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> set_db0{"SET", "db0-key", "zero"};
    const std::array<std::string_view, 3> expire_db0{"PEXPIRE", "db0-key", "10"};
    const std::array<std::string_view, 2> select_db1{"SELECT", "1"};
    const std::array<std::string_view, 3> set_db1{"SET", "db1-key", "one"};
    const std::array<std::string_view, 3> expire_db1{"PEXPIRE", "db1-key", "10"};

    expectSimpleString(server.execute(client, set_db0, now), "OK");
    expectInteger(server.execute(client, expire_db0, now), 1);
    expectSimpleString(server.execute(client, select_db1, now), "OK");
    expectSimpleString(server.execute(client, set_db1, now), "OK");
    expectInteger(server.execute(client, expire_db1, now), 1);
    ASSERT_EQ(server.dirtyCount(), 4);

    EXPECT_EQ(server.activeExpireCycle(now + Milliseconds{10}, 10), 2);

    EXPECT_EQ(server.dirtyCount(), 6);
}
