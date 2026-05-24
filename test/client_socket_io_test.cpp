#include <gtest/gtest.h>

#include <array>
#include <fcntl.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>

#include "hyper/server/client_session.hpp"
#include "hyper/server/client_socket_io.hpp"
#include "hyper/server/redis_server.hpp"
#include "hyper/server/resp_codec.hpp"
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

        void setServerNonBlocking() {
            const auto flags = fcntl(fds_[0], F_GETFL, 0);
            ASSERT_NE(flags, -1);
            ASSERT_NE(fcntl(fds_[0], F_SETFL, flags | O_NONBLOCK), -1);
        }

    private:
        std::array<int, 2> fds_{{-1, -1}};
    };

    [[nodiscard]] ExpireTimePoint makeTime(UnixMilliseconds ms) {
        return ExpireTimePoint{Milliseconds{ms}};
    }

    template<std::size_t N>
    [[nodiscard]] constexpr std::string_view literalBytes(const char (&value)[N]) noexcept {
        return {value, N - 1};
    }

    template<std::size_t N>
    [[nodiscard]] std::string commandBytes(const std::array<std::string_view, N>& args) {
        return serializeRespCommand(args);
    }
}

TEST(ClientSocketIoTest, ReadClientQueryAppendsBytesProcessesCommandAndQueuesReply) {
    SocketPair sockets;
    ClientSession client(sockets.serverFd());
    RedisServer server(1);
    const auto ping = commandBytes(std::array<std::string_view, 1>{"PING"});

    ASSERT_EQ(write(sockets.peerFd(), ping.data(), ping.size()), static_cast<ssize_t>(ping.size()));

    const auto result = readClientQuery(client, server, makeTime(1'000));

    EXPECT_EQ(result.status, ClientIoStatus::Ok);
    EXPECT_EQ(result.bytes, ping.size());
    EXPECT_TRUE(client.queryBuffer().empty());
    EXPECT_EQ(client.replyBuffer(), literalBytes("+PONG\r\n"));
}

TEST(ClientSocketIoTest, WriteClientReplyWritesBytesAndConsumesReplyBuffer) {
    SocketPair sockets;
    ClientSession client(sockets.serverFd());
    RedisServer server(1);
    const auto ping = commandBytes(std::array<std::string_view, 1>{"PING"});

    client.appendQueryBytes(ping);
    client.processInput(server, makeTime(1'000));

    const auto result = writeClientReply(client);

    std::array<char, 16> buffer{};
    const auto n = read(sockets.peerFd(), buffer.data(), buffer.size());
    ASSERT_GT(n, 0);

    EXPECT_EQ(result.status, ClientIoStatus::Ok);
    EXPECT_EQ(result.bytes, static_cast<std::size_t>(n));
    EXPECT_EQ(std::string_view(buffer.data(), static_cast<std::size_t>(n)), literalBytes("+PONG\r\n"));
    EXPECT_TRUE(client.replyBuffer().empty());
}

TEST(ClientSocketIoTest, ReadClientQueryReturnsClosedWhenPeerCloses) {
    SocketPair sockets;
    ClientSession client(sockets.serverFd());
    RedisServer server(1);

    sockets.closePeer();

    const auto result = readClientQuery(client, server, makeTime(1'000));

    EXPECT_EQ(result.status, ClientIoStatus::Closed);
    EXPECT_EQ(result.bytes, 0U);
    EXPECT_TRUE(client.queryBuffer().empty());
    EXPECT_TRUE(client.replyBuffer().empty());
}

TEST(ClientSocketIoTest, ReadClientQueryReturnsWouldBlockForNonBlockingSocketWithoutData) {
    SocketPair sockets;
    ClientSession client(sockets.serverFd());
    RedisServer server(1);
    sockets.setServerNonBlocking();

    const auto result = readClientQuery(client, server, makeTime(1'000));

    EXPECT_EQ(result.status, ClientIoStatus::WouldBlock);
    EXPECT_EQ(result.bytes, 0U);
    EXPECT_TRUE(client.queryBuffer().empty());
    EXPECT_TRUE(client.replyBuffer().empty());
}

TEST(ClientSocketIoTest, WriteClientReplyWithEmptyReplyBufferReturnsOkWithoutWriting) {
    SocketPair sockets;
    ClientSession client(sockets.serverFd());

    const auto result = writeClientReply(client);

    EXPECT_EQ(result.status, ClientIoStatus::Ok);
    EXPECT_EQ(result.bytes, 0U);
    EXPECT_TRUE(client.replyBuffer().empty());
}
