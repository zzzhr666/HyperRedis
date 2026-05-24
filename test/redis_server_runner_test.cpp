#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <netinet/in.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>

#include "hyper/server/redis_server_runner.hpp"

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

    [[nodiscard]] bool tcpListeningUnavailableByPermission() noexcept {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return errno == EPERM || errno == EACCES;
        }

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;

        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            const int bind_errno = errno;
            ::close(fd);
            return bind_errno == EPERM || bind_errno == EACCES;
        }

        if (::listen(fd, 1) != 0) {
            const int listen_errno = errno;
            ::close(fd);
            return listen_errno == EPERM || listen_errno == EACCES;
        }

        ::close(fd);
        return false;
    }

    [[nodiscard]] TcpListenOptions portZeroOptions() {
        TcpListenOptions options;
        options.port = 0;
        return options;
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

    EXPECT_FALSE(runner.start(options));
    EXPECT_FALSE(runner.running());
    EXPECT_EQ(runner.port(), 0);
}

TEST(RedisServerRunnerTest, StartWithPortZeroPublishesAssignedPortAndRejectsSecondStart) {
    if (tcpListeningUnavailableByPermission()) {
        GTEST_SKIP() << "TCP listening sockets are not permitted in this environment";
    }

    RedisServerRunner runner;

    ASSERT_TRUE(runner.start(portZeroOptions()));
    EXPECT_TRUE(runner.running());
    EXPECT_NE(runner.port(), 0);
    EXPECT_FALSE(runner.start(portZeroOptions()));

    runner.stop();

    EXPECT_FALSE(runner.running());
    EXPECT_EQ(runner.port(), 0);
}

TEST(RedisServerRunnerTest, ProcessesPingOverTcp) {
    if (tcpListeningUnavailableByPermission()) {
        GTEST_SKIP() << "TCP listening sockets are not permitted in this environment";
    }

    RedisServerRunner runner;
    ASSERT_TRUE(runner.start(portZeroOptions()));

    TcpClient client(runner.port());
    ASSERT_TRUE(client.valid()) << client.errorStage() << " failed with errno " << client.errorNumber();

    constexpr auto ping = literalBytes("*1\r\n$4\r\nPING\r\n");
    ASSERT_EQ(::write(client.fd(), ping.data(), ping.size()), static_cast<ssize_t>(ping.size()));

    EXPECT_EQ(readReplyWhileRunning(runner, client.fd(), literalBytes("+PONG\r\n")),
              literalBytes("+PONG\r\n"));
}
