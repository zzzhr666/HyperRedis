#include <gtest/gtest.h>

#include <cerrno>
#include <cstdlib>
#include <cstdint>
#include <fcntl.h>
#include <netinet/in.h>
#include <string_view>
#include <sys/socket.h>
#include <type_traits>
#include <unistd.h>
#include <utility>

#include "hyper/server/tcp_listener.hpp"

using namespace hyper;

namespace {
    class TcpClient {
    public:
        explicit TcpClient(std::uint16_t port) {
            fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
            if (fd_ < 0) {
                error_ = errno;
                return;
            }

            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            addr.sin_port = htons(port);

            if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
                error_ = errno;
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

        [[nodiscard]] bool valid() const noexcept {
            return connected_;
        }

        [[nodiscard]] int errorNumber() const noexcept {
            return error_;
        }

    private:
        int fd_{-1};
        bool connected_{false};
        int error_{0};
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
}

TEST(TcpListenerTest, IsMoveOnly) {
    EXPECT_FALSE(std::is_copy_constructible_v<TcpListener>);
    EXPECT_FALSE(std::is_copy_assignable_v<TcpListener>);
    EXPECT_TRUE(std::is_move_constructible_v<TcpListener>);
    EXPECT_TRUE(std::is_move_assignable_v<TcpListener>);
}

TEST(TcpListenerTest, CreateWithPortZeroBindsToAssignedPort) {
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
    }

    auto listener = TcpListener::create(portZeroOptions());

    ASSERT_TRUE(listener.has_value());
    EXPECT_GE(listener->fd(), 0);
    EXPECT_NE(listener->port(), 0);
    EXPECT_NE(::fcntl(listener->fd(), F_GETFD), -1);
}

TEST(TcpListenerTest, CreatedListenerAcceptsTcpClient) {
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
    }
    auto listener = TcpListener::create(portZeroOptions());
    ASSERT_TRUE(listener.has_value());

    TcpClient client(listener->port());
    ASSERT_TRUE(client.valid()) << "connect failed with errno " << client.errorNumber();

    const int accepted_fd = ::accept(listener->fd(), nullptr, nullptr);
    ASSERT_GE(accepted_fd, 0);
    ::close(accepted_fd);
}

TEST(TcpListenerTest, MoveConstructorTransfersFdOwnership) {
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
    }
    auto listener = TcpListener::create(portZeroOptions());
    ASSERT_TRUE(listener.has_value());
    const int original_fd = listener->fd();
    const auto original_port = listener->port();

    TcpListener moved(std::move(*listener));

    EXPECT_EQ(listener->fd(), -1);
    EXPECT_EQ(listener->port(), 0);
    EXPECT_EQ(moved.fd(), original_fd);
    EXPECT_EQ(moved.port(), original_port);
    EXPECT_NE(::fcntl(moved.fd(), F_GETFD), -1);
}

TEST(TcpListenerTest, MoveAssignmentClosesPreviousFdAndTransfersOwnership) {
    if (skipTcpListenerTests()) {
        GTEST_SKIP() << "HYPERREDIS_SKIP_TCP_LISTENER_TESTS=1";
    }
    auto first = TcpListener::create(portZeroOptions());
    auto second = TcpListener::create(portZeroOptions());
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    const int closed_fd = first->fd();
    const int transferred_fd = second->fd();
    const auto transferred_port = second->port();

    *first = std::move(*second);

    EXPECT_EQ(second->fd(), -1);
    EXPECT_EQ(second->port(), 0);
    EXPECT_EQ(first->fd(), transferred_fd);
    EXPECT_EQ(first->port(), transferred_port);

    errno = 0;
    EXPECT_EQ(::fcntl(closed_fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
}
