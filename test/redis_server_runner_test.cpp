#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <netinet/in.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>

#include "hyper/server/resp_codec.hpp"
#include "hyper/server/redis_server_runner.hpp"
#include "hyper/server/resp_value.hpp"
#include "hyper/storage/rdb_saver.hpp"
#include "hyper/storage/redis_manager.hpp"
#include "hyper/storage/object.hpp"
#include "hyper/storage/database.hpp"

using namespace hyper;

namespace {
    class TcpClient {
    public:
        explicit TcpClient(std::uint16_t port) {
            fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
            if (fd_ < 0) {
                recordError_("socket");
                return;
            }

            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            addr.sin_port = htons(port);

            if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
                recordError_("connect");
                ::close(fd_);
                fd_ = -1;
                return;
            }
            connected_ = true;
        }

        TcpClient(const TcpClient&) = delete;
        TcpClient& operator=(const TcpClient&) = delete;

        ~TcpClient() {
            if (fd_ >= 0) {
                ::close(fd_);
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

    [[nodiscard]] TcpListenOptions portZeroOptions() {
        TcpListenOptions options;
        options.port = 0;
        return options;
    }

    [[nodiscard]] RedisServerRunnerConfig runnerConfig(TcpListenOptions options) {
        RedisServerRunnerConfig config;
        config.listen_options = std::move(options);
        return config;
    }

    [[nodiscard]] RedisServerRunnerConfig portZeroConfig() {
        return runnerConfig(portZeroOptions());
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

    void writeFile(const std::filesystem::path& path, std::string_view bytes) {
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(output);
        output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        ASSERT_TRUE(output.good());
    }

    [[nodiscard]] std::string readReplyWhileRunning(RedisServerRunner& runner, int fd,
                                                    std::string_view expected) {
        std::string reply;
        std::array<char, 64> buffer{};
        for (int i = 0; i < 20 && reply.size() < expected.size(); ++i) {
            runner.runOnce(std::chrono::milliseconds{10});
            while (reply.size() < expected.size()) {
                const auto n = ::recv(fd, buffer.data(), buffer.size(), MSG_DONTWAIT);
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

    [[nodiscard]] ExpireTimePoint makeTime(UnixMilliseconds ms) {
        return ExpireTimePoint{Milliseconds{ms}};
    }

    template<std::size_t N>
    [[nodiscard]] constexpr std::string_view literalBytes(const char (&value)[N]) noexcept {
        return {value, N - 1};
    }
}

TEST(RedisServerRunnerTest, StartsStopped) {
    RedisServerRunner runner;

    EXPECT_FALSE(runner.running());
    EXPECT_EQ(runner.port(), 0);

    runner.runOnce(std::chrono::milliseconds{0});
    runner.stop();

    EXPECT_FALSE(runner.running());
    EXPECT_EQ(runner.port(), 0);
}

TEST(RedisServerRunnerTest, StartFailureLeavesRunnerStopped) {
    RedisServerRunner runner;
    TcpListenOptions options;
    options.host = "0.0.0.0";

    EXPECT_FALSE(runner.start(runnerConfig(options)));
    EXPECT_FALSE(runner.running());
    EXPECT_EQ(runner.port(), 0);
}

TEST(RedisServerRunnerTest, LoadRdbOnStartWithoutPathFailsAndLeavesRunnerStopped) {
    RedisServerRunnerConfig config = portZeroConfig();
    config.persistence.load_rdb_on_start = true;

    RedisServerRunner runner;

    EXPECT_FALSE(runner.start(config));
    EXPECT_FALSE(runner.running());
    EXPECT_EQ(runner.port(), 0);
}

TEST(RedisServerRunnerTest, LoadAofOnStartWithoutPathFailsAndLeavesRunnerStopped) {
    RedisServerRunnerConfig config = portZeroConfig();
    config.persistence.load_aof_on_start = true;

    RedisServerRunner runner;

    EXPECT_FALSE(runner.start(config));
    EXPECT_FALSE(runner.running());
    EXPECT_EQ(runner.port(), 0);
}

TEST(RedisServerRunnerTest, SaveRdbOnStopWithoutPathFailsStartAndLeavesRunnerStopped) {
    RedisServerRunnerConfig config = portZeroConfig();
    config.persistence.save_rdb_on_stop = true;

    RedisServerRunner runner;

    EXPECT_FALSE(runner.start(config));
    EXPECT_FALSE(runner.running());
    EXPECT_EQ(runner.port(), 0);
}

TEST(RedisServerRunnerTest, LoadRdbOnStartWithMissingFileFailsAndLeavesRunnerStopped) {
    const auto path = testPath("hyperredis-runner-missing-load.rdb");
    std::filesystem::remove(path);

    RedisServerRunnerConfig config = portZeroConfig();
    config.persistence.rdb_path = path;
    config.persistence.load_rdb_on_start = true;

    RedisServerRunner runner;

    EXPECT_FALSE(runner.start(config));
    EXPECT_FALSE(runner.running());
    EXPECT_EQ(runner.port(), 0);
}

TEST(RedisServerRunnerTest, LoadAofOnStartWithMissingFileFailsAndLeavesRunnerStopped) {
    const auto path = testPath("hyperredis-runner-missing-load.aof");
    std::filesystem::remove(path);

    RedisServerRunnerConfig config = portZeroConfig();
    config.persistence.aof_path = path;
    config.persistence.load_aof_on_start = true;

    RedisServerRunner runner;

    EXPECT_FALSE(runner.start(config));
    EXPECT_FALSE(runner.running());
    EXPECT_EQ(runner.port(), 0);
}

TEST(RedisServerRunnerTest, LoadAofOnStartWithBadFileFailsAndLeavesRunnerStopped) {
    const auto path = testPath("hyperredis-runner-bad-load.aof");
    std::filesystem::remove(path);
    writeFile(path, "not resp");

    RedisServerRunnerConfig config = portZeroConfig();
    config.persistence.aof_path = path;
    config.persistence.load_aof_on_start = true;

    RedisServerRunner runner;

    EXPECT_FALSE(runner.start(config));
    EXPECT_FALSE(runner.running());
    EXPECT_EQ(runner.port(), 0);

    std::filesystem::remove(path);
}

TEST(RedisServerRunnerTest, LoadRdbOnStartWithBadFileFailsAndLeavesRunnerStopped) {
    const auto path = testPath("hyperredis-runner-bad-load.rdb");
    std::filesystem::remove(path);

    {
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(output);
        output << "not an rdb";
    }

    RedisServerRunnerConfig config = portZeroConfig();
    config.persistence.rdb_path = path;
    config.persistence.load_rdb_on_start = true;

    RedisServerRunner runner;

    EXPECT_FALSE(runner.start(config));
    EXPECT_FALSE(runner.running());
    EXPECT_EQ(runner.port(), 0);

    std::filesystem::remove(path);
}

TEST(RedisServerRunnerTest, StartWithPortZeroPublishesAssignedPortAndRejectsSecondStart) {
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
    }

    RedisServerRunner runner;

    ASSERT_TRUE(runner.start(portZeroConfig()));
    EXPECT_TRUE(runner.running());
    EXPECT_NE(runner.port(), 0);
    EXPECT_FALSE(runner.start(portZeroConfig()));

    runner.stop();

    EXPECT_FALSE(runner.running());
    EXPECT_EQ(runner.port(), 0);
}

TEST(RedisServerRunnerTest, ProcessesPingOverTcp) {
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
    }

    RedisServerRunner runner;
    ASSERT_TRUE(runner.start(portZeroConfig()));

    TcpClient client(runner.port());
    ASSERT_TRUE(client.valid()) << client.errorStage() << " failed with errno " << client.errorNumber();

    constexpr auto ping = literalBytes("*1\r\n$4\r\nPING\r\n");
    ASSERT_EQ(::write(client.fd(), ping.data(), ping.size()), static_cast<ssize_t>(ping.size()));

    EXPECT_EQ(readReplyWhileRunning(runner, client.fd(), literalBytes("+PONG\r\n")),
              literalBytes("+PONG\r\n"));
}

TEST(RedisServerRunnerTest, ServerCronActivelyExpiresKeys) {
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
    }

    RedisServerRunner runner;
    ASSERT_TRUE(runner.start(portZeroConfig()));

    TcpClient client(runner.port());
    ASSERT_TRUE(client.valid()) << client.errorStage() << " failed with errno " << client.errorNumber();

    const auto set = serializeRespCommand(std::array<std::string_view, 3>{"SET", "cron-key", "value"});
    ASSERT_EQ(::write(client.fd(), set.data(), set.size()), static_cast<ssize_t>(set.size()));
    EXPECT_EQ(readReplyWhileRunning(runner, client.fd(), literalBytes("+OK\r\n")),
              literalBytes("+OK\r\n"));

    const auto expire = serializeRespCommand(std::array<std::string_view, 3>{"PEXPIRE", "cron-key", "10"});
    ASSERT_EQ(::write(client.fd(), expire.data(), expire.size()), static_cast<ssize_t>(expire.size()));
    EXPECT_EQ(readReplyWhileRunning(runner, client.fd(), literalBytes(":1\r\n")),
              literalBytes(":1\r\n"));

    runner.runOnce(std::chrono::milliseconds{150});
    runner.runOnce(std::chrono::milliseconds{150});

    const auto dbsize = serializeRespCommand(std::array<std::string_view, 1>{"DBSIZE"});
    ASSERT_EQ(::write(client.fd(), dbsize.data(), dbsize.size()), static_cast<ssize_t>(dbsize.size()));
    EXPECT_EQ(readReplyWhileRunning(runner, client.fd(), literalBytes(":0\r\n")),
              literalBytes(":0\r\n"));

    runner.stop();
}

TEST(RedisServerRunnerTest, AofOptionAppendsTcpWriteCommand) {
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
    }

    const auto path = testPath("hyperredis-runner-aof-append.aof");
    std::filesystem::remove(path);

    RedisServerRunnerConfig config = portZeroConfig();
    config.persistence.aof_path = path;

    RedisServerRunner runner;
    ASSERT_TRUE(runner.start(config));

    TcpClient client(runner.port());
    ASSERT_TRUE(client.valid()) << client.errorStage() << " failed with errno " << client.errorNumber();

    const std::array<std::string_view, 3> args{"SET", "key", "value"};
    const auto set = serializeRespCommand(args);
    ASSERT_EQ(::write(client.fd(), set.data(), set.size()), static_cast<ssize_t>(set.size()));

    EXPECT_EQ(readReplyWhileRunning(runner, client.fd(), literalBytes("+OK\r\n")),
              literalBytes("+OK\r\n"));
    EXPECT_EQ(readFileIfExists(path), set);

    runner.stop();
    std::filesystem::remove(path);
}

TEST(RedisServerRunnerTest, LoadRdbOnStartRestoresDataForTcpCommands) {
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
    }

    const auto path = testPath("hyperredis-runner-load-on-start.rdb");
    std::filesystem::remove(path);

    {
        RedisManager manager(1);
        ASSERT_NE(manager.db(0), nullptr);
        manager.db(0)->set("key", RedisObject::createSharedStringObject("value"));
        RdbSaver saver(path);
        ASSERT_TRUE(saver.save(manager, makeTime(1'000)));
    }

    RedisServerRunnerConfig config = portZeroConfig();
    config.persistence.rdb_path = path;
    config.persistence.load_rdb_on_start = true;

    RedisServerRunner runner;
    ASSERT_TRUE(runner.start(config));

    TcpClient client(runner.port());
    ASSERT_TRUE(client.valid()) << client.errorStage() << " failed with errno " << client.errorNumber();

    const auto get = serializeRespCommand(std::array<std::string_view, 2>{"GET", "key"});
    ASSERT_EQ(::write(client.fd(), get.data(), get.size()), static_cast<ssize_t>(get.size()));

    EXPECT_EQ(readReplyWhileRunning(runner, client.fd(), literalBytes("$5\r\nvalue\r\n")),
              literalBytes("$5\r\nvalue\r\n"));

    runner.stop();
    std::filesystem::remove(path);
}

TEST(RedisServerRunnerTest, LoadAofOnStartRestoresDataForTcpCommands) {
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
    }

    const auto path = testPath("hyperredis-runner-load-on-start.aof");
    std::filesystem::remove(path);

    const auto set = serializeRespCommand(std::array<std::string_view, 3>{"SET", "key", "aof"});
    writeFile(path, set);

    RedisServerRunnerConfig config = portZeroConfig();
    config.persistence.aof_path = path;
    config.persistence.load_aof_on_start = true;

    RedisServerRunner runner;
    ASSERT_TRUE(runner.start(config));

    TcpClient client(runner.port());
    ASSERT_TRUE(client.valid()) << client.errorStage() << " failed with errno " << client.errorNumber();

    const auto get = serializeRespCommand(std::array<std::string_view, 2>{"GET", "key"});
    ASSERT_EQ(::write(client.fd(), get.data(), get.size()), static_cast<ssize_t>(get.size()));

    EXPECT_EQ(readReplyWhileRunning(runner, client.fd(), literalBytes("$3\r\naof\r\n")),
              literalBytes("$3\r\naof\r\n"));

    runner.stop();
    std::filesystem::remove(path);
}

TEST(RedisServerRunnerTest, LoadAofOnStartTakesPrecedenceOverRdb) {
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
    }

    const auto rdb_path = testPath("hyperredis-runner-load-priority.rdb");
    const auto aof_path = testPath("hyperredis-runner-load-priority.aof");
    std::filesystem::remove(rdb_path);
    std::filesystem::remove(aof_path);

    {
        RedisManager manager(1);
        ASSERT_NE(manager.db(0), nullptr);
        manager.db(0)->set("key", RedisObject::createSharedStringObject("rdb"));
        RdbSaver saver(rdb_path);
        ASSERT_TRUE(saver.save(manager, makeTime(1'000)));
    }
    const auto set = serializeRespCommand(std::array<std::string_view, 3>{"SET", "key", "aof"});
    writeFile(aof_path, set);

    RedisServerRunnerConfig config = portZeroConfig();
    config.persistence.rdb_path = rdb_path;
    config.persistence.load_rdb_on_start = true;
    config.persistence.aof_path = aof_path;
    config.persistence.load_aof_on_start = true;

    RedisServerRunner runner;
    ASSERT_TRUE(runner.start(config));

    TcpClient client(runner.port());
    ASSERT_TRUE(client.valid()) << client.errorStage() << " failed with errno " << client.errorNumber();

    const auto get = serializeRespCommand(std::array<std::string_view, 2>{"GET", "key"});
    ASSERT_EQ(::write(client.fd(), get.data(), get.size()), static_cast<ssize_t>(get.size()));

    EXPECT_EQ(readReplyWhileRunning(runner, client.fd(), literalBytes("$3\r\naof\r\n")),
              literalBytes("$3\r\naof\r\n"));

    runner.stop();
    std::filesystem::remove(rdb_path);
    std::filesystem::remove(aof_path);
}

TEST(RedisServerRunnerTest, StopSavesRdbWhenConfigured) {
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
    }

    const auto path = testPath("hyperredis-runner-save-on-stop.rdb");
    std::filesystem::remove(path);

    RedisServerRunnerConfig config = portZeroConfig();
    config.persistence.rdb_path = path;
    config.persistence.save_rdb_on_stop = true;

    RedisServerRunner runner;
    ASSERT_TRUE(runner.start(config));

    TcpClient client(runner.port());
    ASSERT_TRUE(client.valid()) << client.errorStage() << " failed with errno " << client.errorNumber();

    const auto set = serializeRespCommand(std::array<std::string_view, 3>{"SET", "key", "value"});
    ASSERT_EQ(::write(client.fd(), set.data(), set.size()), static_cast<ssize_t>(set.size()));
    EXPECT_EQ(readReplyWhileRunning(runner, client.fd(), literalBytes("+OK\r\n")),
              literalBytes("+OK\r\n"));

    runner.stop();
    ASSERT_TRUE(std::filesystem::is_regular_file(path));

    RedisManager loaded(1);
    RdbSaver saver(path);
    ASSERT_TRUE(saver.load(loaded, makeTime(1'000)));
    ASSERT_NE(loaded.db(0), nullptr);
    auto obj = loaded.db(0)->get("key", makeTime(1'000));
    ASSERT_NE(obj, nullptr);
    ASSERT_EQ(obj->getType(), ObjectType::String);
    EXPECT_EQ(obj->asString(), "value");

    std::filesystem::remove(path);
}
