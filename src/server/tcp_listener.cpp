#include "hyper/server/tcp_listener.hpp"

#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>

std::optional<hyper::TcpListener> hyper::TcpListener::create(const TcpListenOptions& option) {
    if (option.host != "127.0.0.1") {
        return std::nullopt;
    }

    auto fd = ::socket(AF_INET,SOCK_STREAM, 0);
    if (fd < 0) {
        return std::nullopt;
    }


    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(option.port);

    int reuse = 1;
    if (::setsockopt(fd,SOL_SOCKET,SO_REUSEADDR, &reuse, sizeof(reuse)) != 0) {
        ::close(fd);
        return std::nullopt;
    }

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return std::nullopt;
    }
    if (::listen(fd, option.backlog) != 0) {
        ::close(fd);
        return std::nullopt;
    }
    sockaddr_in bound_addr{};
    socklen_t bound_len = sizeof(bound_addr);

    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&bound_addr), &bound_len) != 0) {
        ::close(fd);
        return std::nullopt;
    }
    auto actual_port = ntohs(bound_addr.sin_port);
    return TcpListener{fd, actual_port};
}


hyper::TcpListener::TcpListener(TcpListener&& other) noexcept
    : fd_(other.fd_), port_(other.port_) {
    other.fd_ = -1;
    other.port_ = 0;
}

hyper::TcpListener& hyper::TcpListener::operator=(TcpListener&& other) noexcept {
    if (this != &other) {
        if (fd_ != -1) {
            ::close(fd_);
        }
        fd_ = other.fd_;
        port_ = other.port_;
        other.fd_ = -1;
        other.port_ = 0;
    }
    return *this;
}

hyper::TcpListener::~TcpListener() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

hyper::TcpListener::TcpListener(int fd, std::uint16_t port) : fd_(fd), port_(port) {}
