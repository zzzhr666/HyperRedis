#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <memory>
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
#include "hyper/storage/aof_appender.hpp"
#include "hyper/storage/database.hpp"
#include "hyper/storage/object.hpp"
#include "hyper/storage/rdb_saver.hpp"
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

    [[nodiscard]] bool skipTcpListenerTests() noexcept {
        const char* value = std::getenv("HYPERREDIS_SKIP_TCP_LISTENER_TESTS");
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

    [[nodiscard]] std::filesystem::path testPath(std::string_view name) {
        return std::filesystem::temp_directory_path() / std::string(name);
    }

    [[nodiscard]] std::string readFileIfExists(const std::filesystem::path& path) {
        if (!std::filesystem::exists(path)) {
            return {};
        }
        std::ifstream input(path, std::ios::binary);
        return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
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

    void expectNullBulkString(const RespValue& value) {
        const auto* bulk = std::get_if<RespBulkString>(&value);
        ASSERT_NE(bulk, nullptr);
        EXPECT_FALSE(bulk->value.has_value());
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

TEST(RedisServerTest, RdbOperationsWithoutSaverReturnFalse) {
    RedisServer server;
    const auto now = makeTime(1'000);

    EXPECT_FALSE(server.hasRdbSaver());
    EXPECT_FALSE(server.saveRdb(now));
    EXPECT_FALSE(server.loadRdb(now));
}

TEST(RedisServerTest, SaveAndLoadRdbThroughConfiguredSaver) {
    const auto path = testPath("hyperredis-server-rdb-round-trip.rdb");
    std::filesystem::remove(path);

    const auto now = makeTime(1'000);
    RedisClientContext client;
    {
        RedisServer source(2, nullptr, std::make_unique<RdbSaver>(path));
        const std::array<std::string_view, 3> set_db0{"SET", "db0-key", "zero"};
        const std::array<std::string_view, 2> select_db1{"SELECT", "1"};
        const std::array<std::string_view, 3> set_db1{"SET", "db1-key", "one"};

        EXPECT_TRUE(source.hasRdbSaver());
        expectSimpleString(source.execute(client, set_db0, now), "OK");
        expectSimpleString(source.execute(client, select_db1, now), "OK");
        expectSimpleString(source.execute(client, set_db1, now), "OK");
        ASSERT_TRUE(source.saveRdb(now));
        ASSERT_TRUE(std::filesystem::is_regular_file(path));
    }

    RedisServer target(2, nullptr, std::make_unique<RdbSaver>(path));
    RedisClientContext target_client;
    const std::array<std::string_view, 2> get_db0{"GET", "db0-key"};
    const std::array<std::string_view, 2> select_db1{"SELECT", "1"};
    const std::array<std::string_view, 2> get_db1{"GET", "db1-key"};

    EXPECT_TRUE(target.hasRdbSaver());
    ASSERT_TRUE(target.loadRdb(now));
    expectBulkString(target.execute(target_client, get_db0, now), "zero");
    expectSimpleString(target.execute(target_client, select_db1, now), "OK");
    expectBulkString(target.execute(target_client, get_db1, now), "one");

    std::filesystem::remove(path);
}

TEST(RedisServerTest, LoadMissingRdbKeepsExistingData) {
    const auto path = testPath("hyperredis-server-missing-load.rdb");
    std::filesystem::remove(path);

    const auto now = makeTime(1'000);
    RedisServer server(1, nullptr, std::make_unique<RdbSaver>(path));
    RedisClientContext client;
    const std::array<std::string_view, 3> set_args{"SET", "keep", "value"};
    const std::array<std::string_view, 2> get_args{"GET", "keep"};

    expectSimpleString(server.execute(client, set_args, now), "OK");
    EXPECT_FALSE(server.loadRdb(now));
    expectBulkString(server.execute(client, get_args, now), "value");
}

TEST(RedisServerTest, LoadBadRdbKeepsExistingData) {
    const auto path = testPath("hyperredis-server-bad-load.rdb");
    std::filesystem::remove(path);

    {
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(output);
        output << "not an rdb";
    }

    const auto now = makeTime(1'000);
    RedisServer server(1, nullptr, std::make_unique<RdbSaver>(path));
    RedisClientContext client;
    const std::array<std::string_view, 3> set_args{"SET", "keep", "value"};
    const std::array<std::string_view, 2> get_args{"GET", "keep"};

    expectSimpleString(server.execute(client, set_args, now), "OK");
    EXPECT_FALSE(server.loadRdb(now));
    expectBulkString(server.execute(client, get_args, now), "value");

    std::filesystem::remove(path);
}

TEST(RedisServerTest, WriteCommandAppendsToConfiguredAof) {
    const auto path = testPath("hyperredis-server-aof-write.aof");
    std::filesystem::remove(path);

    RedisServer server(1, std::make_unique<AofAppender>(path), nullptr);
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> set_args{"SET", "key", "value"};

    expectSimpleString(server.execute(client, set_args, now), "OK");

    EXPECT_EQ(readFileIfExists(path), serializeRespCommand(set_args));

    std::filesystem::remove(path);
}

TEST(RedisServerTest, ReadCommandDoesNotAppendToConfiguredAof) {
    const auto path = testPath("hyperredis-server-aof-read.aof");
    std::filesystem::remove(path);

    RedisServer server(1, std::make_unique<AofAppender>(path), nullptr);
    RedisClientContext client;
    const auto now = makeTime(1'000);
    ASSERT_NE(server.manager().db(0), nullptr);
    server.manager().db(0)->set("key", RedisObject::createSharedStringObject("value"));
    const std::array<std::string_view, 2> get_args{"GET", "key"};

    expectBulkString(server.execute(client, get_args, now), "value");

    EXPECT_EQ(readFileIfExists(path), "");

    std::filesystem::remove(path);
}

TEST(RedisServerTest, FailedWriteCommandDoesNotAppendToConfiguredAof) {
    const auto path = testPath("hyperredis-server-aof-failed-write.aof");
    std::filesystem::remove(path);

    RedisServer server(1, std::make_unique<AofAppender>(path), nullptr);
    RedisClientContext client;
    const auto now = makeTime(1'000);
    ASSERT_NE(server.manager().db(0), nullptr);
    server.manager().db(0)->set("key", RedisObject::createSharedStringObject("not-an-int"));
    const std::array<std::string_view, 2> incr_args{"INCR", "key"};

    expectError(server.execute(client, incr_args, now),
                "ERR value is not an integer or out of range");

    EXPECT_EQ(readFileIfExists(path), "");

    std::filesystem::remove(path);
}

TEST(RedisServerTest, AofFailureRejectsLaterWriteButStillAllowsReads) {
    const auto path = testPath("hyperredis-server-aof-as-directory.aof");
    std::filesystem::remove_all(path);
    ASSERT_TRUE(std::filesystem::create_directory(path));

    RedisServer server(1, std::make_unique<AofAppender>(path), nullptr);
    RedisClientContext client;
    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> first_set{"SET", "first", "value"};
    const std::array<std::string_view, 3> second_set{"SET", "second", "value"};
    const std::array<std::string_view, 2> get_first{"GET", "first"};

    expectError(server.execute(client, first_set, now), "ERR append only file write failed");
    expectError(server.execute(client, second_set, now), "ERR append only file write failed");
    expectBulkString(server.execute(client, get_first, now), "value");
    expectNullBulkString(server.execute(client, std::array<std::string_view, 2>{"GET", "second"}, now));

    std::filesystem::remove_all(path);
}

TEST(RedisServerTest, LoadAofSynchronizesAppenderSelectedDbIndex) {
    const auto path = testPath("hyperredis-server-load-aof-selected-db.aof");
    std::filesystem::remove(path);

    const auto now = makeTime(1'000);
    const std::array<std::string_view, 3> db1_set{"SET", "db1-key", "one"};
    const std::array<std::string_view, 3> db0_set{"SET", "db0-key", "zero"};
    const std::array<std::string_view, 2> select_db1{"SELECT", "1"};
    const std::array<std::string_view, 2> select_db0{"SELECT", "0"};

    {
        AofAppender appender(path);
        ASSERT_TRUE(appender.appendCommand(1, db1_set, now));
    }

    RedisServer server(2, std::make_unique<AofAppender>(path), nullptr);
    RedisClientContext client;

    ASSERT_TRUE(server.loadAof(now));
    expectSimpleString(server.execute(client, db0_set, now), "OK");

    EXPECT_EQ(readFileIfExists(path),
              serializeRespCommand(select_db1)
                  + serializeRespCommand(db1_set)
                  + serializeRespCommand(select_db0)
                  + serializeRespCommand(db0_set));

    std::filesystem::remove(path);
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
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
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
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
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
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
    }

    TcpListenOptions options;
    options.port = 0;
    auto listener_res = TcpListener::create(options);
    ASSERT_TRUE(std::holds_alternative<TcpListener>(listener_res));
    auto& listener = std::get<TcpListener>(listener_res);

    RedisServer server;
    EventLoop loop;
    const auto ping = serializeRespCommand(std::array<std::string_view, 1>{"PING"});

    ASSERT_TRUE(server.attachListener(loop, listener.fd()));
    TcpClient client(listener.port());
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

TEST(RedisServerTest, ServerCronRunsActiveExpireCycle) {
    RedisServer server(2);
    const auto now = makeTime(1'000);
    auto* db0 = server.manager().db(0);
    auto* db1 = server.manager().db(1);
    ASSERT_NE(db0, nullptr);
    ASSERT_NE(db1, nullptr);

    db0->set("db0-key", RedisObject::createSharedStringObject("zero"));
    db1->set("db1-key", RedisObject::createSharedStringObject("one"));
    ASSERT_TRUE(db0->expireAfter("db0-key", Milliseconds{10}, now));
    ASSERT_TRUE(db1->expireAfter("db1-key", Milliseconds{10}, now));

    EventLoop loop;
    EXPECT_EQ(server.serverCron(loop, now + Milliseconds{10}), 2);
    EXPECT_EQ(server.dirtyCount(), 2);
    EXPECT_EQ(db0->get("db0-key", now + Milliseconds{10}), nullptr);
    EXPECT_EQ(db1->get("db1-key", now + Milliseconds{10}), nullptr);
}

TEST(RedisServerTest, SaveCommandTriggersRdbPersistence) {
    const auto path = testPath("save_command.rdb");
    std::filesystem::remove(path);
    
    RedisServer server(1, nullptr, std::make_unique<RdbSaver>(path));
    RedisClientContext client;
    const auto now = makeTime(1000);

    const std::array<std::string_view, 3> set_cmd{"SET", "key", "value"};
    const std::array<std::string_view, 1> save_cmd{"SAVE"};

    expectSimpleString(server.execute(client, set_cmd, now), "OK");
    expectSimpleString(server.execute(client, save_cmd, now), "OK");

    ASSERT_TRUE(std::filesystem::exists(path));
    
    // Verify content by loading it into another server
    RedisServer loader(1, nullptr, std::make_unique<RdbSaver>(path));
    ASSERT_TRUE(loader.loadRdb(now));
    auto* db = loader.manager().db(0);
    auto val = db->get("key", now);
    ASSERT_NE(val, nullptr);
    EXPECT_EQ(val->asString(), "value");

    std::filesystem::remove(path);
}

TEST(RedisServerTest, SaveCommandReturnsErrorWhenNoRdbSaver) {
    RedisServer server(1);
    RedisClientContext client;
    const auto now = makeTime(1000);
    const std::array<std::string_view, 1> save_cmd{"SAVE"};

    const auto res = server.execute(client, save_cmd, now);
    const auto* error = std::get_if<RespError>(&res);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->message, "ERR save is not configured");
}

TEST(RedisServerTest, LastSaveCommandReturnsRecentSaveTime) {
    const auto path = testPath("lastsave_command.rdb");
    std::filesystem::remove(path);
    
    RedisServer server(1, nullptr, std::make_unique<RdbSaver>(path));
    RedisClientContext client;
    const auto now = makeTime(1000LL * 123456789); // A specific Unix timestamp

    const std::array<std::string_view, 1> lastsave_cmd{"LASTSAVE"};
    const std::array<std::string_view, 1> save_cmd{"SAVE"};

    // Initial lastsave should be roughly server start time (0 in our mocked now)
    // After implementation, this might return the initial last_save_time_
    
    // Execute SAVE
    expectSimpleString(server.execute(client, save_cmd, now), "OK");
    
    // LASTSAVE should now return 123456789
    const auto res = server.execute(client, lastsave_cmd, now);
    expectInteger(res, 123456789);

    std::filesystem::remove(path);
}

TEST(RedisServerTest, InfoCommandReturnsSystemInformation) {
    RedisServer server(2);
    RedisClientContext client;
    const auto now = makeTime(1000LL * 123456789);
    
    // Set some data to verify Keyspace and Persistence
    const std::array<std::string_view, 3> set_cmd{"SET", "k1", "v1"};
    const std::array<std::string_view, 3> expire_cmd{"EXPIRE", "k1", "10"};
    (void)server.execute(client, set_cmd, now);
    (void)server.execute(client, expire_cmd, now);

    // 1. Test full INFO
    {
        const std::array<std::string_view, 1> info_cmd{"INFO"};
        const auto res = server.execute(client, info_cmd, now);
        const auto* bulk = std::get_if<RespBulkString>(&res);
        ASSERT_NE(bulk, nullptr);
        ASSERT_TRUE(bulk->value.has_value());
        const std::string& s = *bulk->value;
        
        EXPECT_NE(s.find("# Server"), std::string::npos);
        EXPECT_NE(s.find("hyper_redis_version:0.1.0"), std::string::npos);
        EXPECT_NE(s.find("# Clients"), std::string::npos);
        EXPECT_NE(s.find("# Persistence"), std::string::npos);
        EXPECT_NE(s.find("rdb_changes_since_last_save:2"), std::string::npos);
        EXPECT_NE(s.find("# Keyspace"), std::string::npos);
        EXPECT_NE(s.find("db0:keys=1,expires=1"), std::string::npos);
    }

    // 2. Test specific section (Persistence)
    {
        const std::array<std::string_view, 2> info_persistence{"INFO", "persistence"};
        const auto res = server.execute(client, info_persistence, now);
        const auto* bulk = std::get_if<RespBulkString>(&res);
        ASSERT_NE(bulk, nullptr);
        const std::string& s = *bulk->value;

        EXPECT_EQ(s.find("# Server"), std::string::npos);
        EXPECT_NE(s.find("# Persistence"), std::string::npos);
        EXPECT_NE(s.find("rdb_last_save_time:"), std::string::npos);
    }

    // 3. Test case insensitivity
    {
        const std::array<std::string_view, 2> info_caps{"INFO", "SERVER"};
        const auto res = server.execute(client, info_caps, now);
        const auto* bulk = std::get_if<RespBulkString>(&res);
        ASSERT_NE(bulk, nullptr);
        const std::string& s = *bulk->value;
        EXPECT_NE(s.find("# Server"), std::string::npos);
    }
}

TEST(RedisServerTest, ConfigGetAndSet) {
    RedisServer server(1);
    RedisClientContext client;
    const auto now = makeTime(1000);

    // Test GET defaults
    {
        const std::array<std::string_view, 3> cmd{"CONFIG", "GET", "maxclients"};
        const auto res = server.execute(client, cmd, now);
        const auto* arr_ptr = std::get_if<std::shared_ptr<RespArray>>(&res);
        ASSERT_NE(arr_ptr, nullptr);
        const auto& arr = *arr_ptr;
        ASSERT_EQ(arr->values.size(), 2);
        expectBulkString(arr->values[1], "1024");
    }

    // Test SET and GET
    {
        const std::array<std::string_view, 4> set_cmd{"CONFIG", "SET", "maxclients", "2048"};
        expectSimpleString(server.execute(client, set_cmd, now), "OK");
        EXPECT_EQ(server.maxClients(), 2048);

        const std::array<std::string_view, 3> get_cmd{"CONFIG", "GET", "maxclients"};
        const auto res = server.execute(client, get_cmd, now);
        const auto* arr_ptr = std::get_if<std::shared_ptr<RespArray>>(&res);
        ASSERT_NE(arr_ptr, nullptr);
        const auto& arr = *arr_ptr;
        expectBulkString(arr->values[1], "2048");
    }

    // Test timeout
    {
        const std::array<std::string_view, 4> set_cmd{"CONFIG", "SET", "timeout", "30"};
        expectSimpleString(server.execute(client, set_cmd, now), "OK");
        EXPECT_EQ(server.timeout(), 30);
    }

    // Test read-only parameter
    {
        const std::array<std::string_view, 4> set_cmd{"CONFIG", "SET", "databases", "16"};
        const auto res = server.execute(client, set_cmd, now);
        const auto* err = std::get_if<RespError>(&res);
        ASSERT_NE(err, nullptr);
        EXPECT_NE(err->message.find("Constant"), std::string::npos);
    }
}

TEST(RedisServerTest, MaxClientsEnforcement) {
    RedisServer server(1);
    server.setMaxClients(1); // Only allow 1 client

    LocalStreamListener listener;
    ASSERT_TRUE(listener.valid());

    EventLoop loop;
    ASSERT_TRUE(server.attachListener(loop, listener.fd()));

    LocalStreamClient c1(listener.path());
    ASSERT_TRUE(c1.valid());

    // Trigger the accept callback in event loop
    loop.runOnce(std::chrono::milliseconds(10));
    EXPECT_EQ(server.clientCount(), 1);

    LocalStreamClient c2(listener.path());
    ASSERT_TRUE(c2.valid());

    // Trigger the accept callback again
    loop.runOnce(std::chrono::milliseconds(10));
    // c2 should be rejected, so count stays 1
    EXPECT_EQ(server.clientCount(), 1);

    // Verify error message on c2
    char buf[128];
    ssize_t n = read(c2.fd(), buf, sizeof(buf));
    ASSERT_GT(n, 0);
    std::string_view msg(buf, n);
    EXPECT_NE(msg.find("-ERR max number of clients reached"), std::string::npos);
}

TEST(RedisServerTest, ClientIdleTimeout) {
    RedisServer server(1);
    server.setTimeout(1); // 1 second timeout

    EventLoop loop;
    SocketPair s1;
    // attachClient is public
    ASSERT_TRUE(server.attachClient(loop, s1.serverFd()));
    EXPECT_EQ(server.clientCount(), 1);

    const auto start = ExpireClock::now();

    // Initially not timed out
    server.serverCron(loop, start);
    EXPECT_EQ(server.clientCount(), 1);

    // After 2 seconds, it should be timed out
    server.serverCron(loop, start + std::chrono::seconds(2));
    EXPECT_EQ(server.clientCount(), 0);
}




