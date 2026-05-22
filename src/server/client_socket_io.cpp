#include "hyper/server/client_socket_io.hpp"

#include <array>
#include <cerrno>
#include <string_view>
#include <unistd.h>


#include "hyper/server/client_session.hpp"

namespace {
    constexpr std::size_t ReadBufferSize = 16 * 1024; //16KB

    ssize_t readWithoutInterrupt(int fd, void* buffer, std::size_t size) {
        while (true) {
            const auto n = ::read(fd, buffer, size);
            if (n < 0 && errno == EINTR) {
                continue;
            }
            return n;
        }
    }

    ssize_t writeWithoutInterrupt(int fd, const void* buffer, std::size_t size) {
        while (true) {
            const auto n = ::write(fd, buffer, size);
            if (n < 0 && errno == EINTR) {
                continue;
            }
            return n;
        }
    }
}

hyper::ClientIoResult hyper::readClientQuery(ClientSession& client, RedisManager& manager,
                                             const CommandProcessor& processor, ExpireTimePoint now) {
    std::array<char, ReadBufferSize> buffer{};
    auto n = readWithoutInterrupt(client.fd(), buffer.data(), ReadBufferSize);
    if (n == 0) {
        return {ClientIoStatus::Closed, 0};
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return {ClientIoStatus::WouldBlock, 0};
    }
    if (n > 0) {
        std::string_view input{buffer.data(), static_cast<std::size_t>(n)};
        client.appendQueryBytes(input);
        client.processInput(manager, processor, now);
        return {ClientIoStatus::Ok, static_cast<std::size_t>(n)};
    }

    return {ClientIoStatus::Error, 0};
}

hyper::ClientIoResult hyper::writeClientReply(ClientSession& client) {
    if (client.replyBuffer().empty()) {
        return {ClientIoStatus::Ok, 0};
    }
    auto n = writeWithoutInterrupt(client.fd(), client.replyBuffer().data(), client.replyBuffer().size());
    if (n == 0) {
        return {ClientIoStatus::WouldBlock, 0};
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return {ClientIoStatus::WouldBlock, 0};
    }
    if (n > 0) {
        client.consumeReplyBytes(n);
        return {ClientIoStatus::Ok, static_cast<std::size_t>(n)};
    }
    return {ClientIoStatus::Error, 0};
}
