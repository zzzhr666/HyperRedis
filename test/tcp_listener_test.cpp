#include <gtest/gtest.h>

#include <cerrno>
#include <cstdint>
#include <fcntl.h>
#include <netinet/in.h>
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
}

TEST(TcpListenerTest, IsMoveOnly) {
    EXPECT_FALSE(std::is_copy_constructible_v<TcpListener>);
    EXPECT_FALSE(std::is_copy_assignable_v<TcpListener>);
    EXPECT_TRUE(std::is_move_constructible_v<TcpListener>);
    EXPECT_TRUE(std::is_move_assignable_v<TcpListener>);
}

TEST(TcpListenerTest, CreateWithPortZeroBindsToAssignedPort) {
    if (tcpListeningUnavailableByPermission()) {
        GTEST_SKIP() << "TCP listening sockets are not permitted in this environment";
    }

    auto listener = TcpListener::create(portZeroOptions());

    ASSERT_TRUE(listener.has_value());
    EXPECT_GE(listener->fd(), 0);
    EXPECT_NE(listener->port(), 0);
    EXPECT_NE(::fcntl(listener->fd(), F_GETFD), -1);
}

TEST(TcpListenerTest, CreatedListenerAcceptsTcpClient) {
    if (tcpListeningUnavailableByPermission()) {
        GTEST_SKIP() << "TCP listening sockets are not permitted in this environment";
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
    if (tcpListeningUnavailableByPermission()) {
        GTEST_SKIP() << "TCP listening sockets are not permitted in this environment";
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
    if (tcpListeningUnavailableByPermission()) {
        GTEST_SKIP() << "TCP listening sockets are not permitted in this environment";
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
