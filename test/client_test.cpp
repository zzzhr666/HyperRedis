#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>

#include "hyper/server/client_session.hpp"
#include "hyper/server/redis_server.hpp"
#include "hyper/server/resp_codec.hpp"
#include "hyper/time.hpp"

using namespace hyper;

namespace {
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

TEST(ClientTest, StartsWithConnectionStateAndEmptyBuffers) {
    ClientSession client(42);

    EXPECT_EQ(client.fd(), 42);
    EXPECT_TRUE(client.queryBuffer().empty());
    EXPECT_TRUE(client.replyBuffer().empty());
    EXPECT_FALSE(client.closeAfterReply());
    EXPECT_EQ(client.context().dbIndex(), 0);
}

TEST(ClientTest, IncompleteCommandStaysInQueryBufferWithoutReply) {
    RedisServer server(1);
    ClientSession client(42);
    const std::string partial = "*1\r\n$4\r\nPI";

    client.appendQueryBytes(partial);
    client.processInput(server, makeTime(1'000));

    EXPECT_EQ(client.queryBuffer(), partial);
    EXPECT_TRUE(client.replyBuffer().empty());
    EXPECT_FALSE(client.closeAfterReply());
}

TEST(ClientTest, CompletingPartialCommandProducesReplyAndConsumesQueryBuffer) {
    RedisServer server(1);
    ClientSession client(42);

    client.appendQueryBytes("*1\r\n$4\r\nPI");
    client.processInput(server, makeTime(1'000));
    client.appendQueryBytes("NG\r\n");
    client.processInput(server, makeTime(1'000));

    EXPECT_TRUE(client.queryBuffer().empty());
    EXPECT_EQ(client.replyBuffer(), literalBytes("+PONG\r\n"));
    EXPECT_FALSE(client.closeAfterReply());
}

TEST(ClientTest, ProcessesMultipleCompleteCommandsFromOneQueryBuffer) {
    RedisServer server(1);
    ClientSession client(42);
    const auto first = commandBytes(std::array<std::string_view, 1>{"PING"});
    const auto second = commandBytes(std::array<std::string_view, 2>{"PING", "hello"});

    client.appendQueryBytes(first + second);
    client.processInput(server, makeTime(1'000));

    EXPECT_TRUE(client.queryBuffer().empty());
    EXPECT_EQ(client.replyBuffer(), literalBytes("+PONG\r\n$5\r\nhello\r\n"));
}

TEST(ClientTest, CommandExecutionUsesClientContextAcrossCommands) {
    RedisServer server(2);
    ClientSession client(42);
    const auto select = commandBytes(std::array<std::string_view, 2>{"SELECT", "1"});
    const auto set = commandBytes(std::array<std::string_view, 3>{"SET", "key", "db1"});
    const auto get = commandBytes(std::array<std::string_view, 2>{"GET", "key"});

    client.appendQueryBytes(select + set + get);
    client.processInput(server, makeTime(1'000));

    EXPECT_EQ(client.context().dbIndex(), 1);
    EXPECT_TRUE(client.queryBuffer().empty());
    EXPECT_EQ(client.replyBuffer(), literalBytes("+OK\r\n+OK\r\n$3\r\ndb1\r\n"));
    EXPECT_EQ(server.dirtyCount(), 1);
}

TEST(ClientTest, ProtocolErrorAddsErrorReplyAndMarksClientForClose) {
    RedisServer server(1);
    ClientSession client(42);

    client.appendQueryBytes("+OK\r\n");
    client.processInput(server, makeTime(1'000));

    EXPECT_EQ(client.replyBuffer(), literalBytes("-ERR protocol error\r\n"));
    EXPECT_TRUE(client.closeAfterReply());
}

TEST(ClientTest, TakeReplyBytesReturnsAndClearsReplyBuffer) {
    RedisServer server(1);
    ClientSession client(42);
    const auto ping = commandBytes(std::array<std::string_view, 1>{"PING"});

    client.appendQueryBytes(ping);
    client.processInput(server, makeTime(1'000));

    EXPECT_EQ(client.takeReplyBytes(), std::string(literalBytes("+PONG\r\n")));
    EXPECT_TRUE(client.replyBuffer().empty());
}

TEST(ClientTest, ConsumeReplyBytesKeepsUnwrittenSuffix) {
    RedisServer server(1);
    ClientSession client(42);
    const auto ping = commandBytes(std::array<std::string_view, 2>{"PING", "hello"});

    client.appendQueryBytes(ping);
    client.processInput(server, makeTime(1'000));

    client.consumeReplyBytes(4);

    EXPECT_EQ(client.replyBuffer(), literalBytes("hello\r\n"));
}

TEST(ClientTest, ConsumeReplyBytesCanClearWholeReplyBuffer) {
    RedisServer server(1);
    ClientSession client(42);
    const auto ping = commandBytes(std::array<std::string_view, 1>{"PING"});

    client.appendQueryBytes(ping);
    client.processInput(server, makeTime(1'000));

    client.consumeReplyBytes(100);

    EXPECT_TRUE(client.replyBuffer().empty());
}

TEST(ClientTest, ReplyBufferPreservesBinaryBulkStringBytes) {
    RedisServer server(1);
    ClientSession client(42);
    const std::string binary_arg{"a\0b", 3};
    const std::array<std::string_view, 2> ping{"PING", std::string_view{binary_arg.data(), binary_arg.size()}};

    client.appendQueryBytes(commandBytes(ping));
    client.processInput(server, makeTime(1'000));

    EXPECT_EQ(client.replyBuffer(), literalBytes("$3\r\na\0b\r\n"));
}
