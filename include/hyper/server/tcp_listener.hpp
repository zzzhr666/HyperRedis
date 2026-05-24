#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <sys/socket.h>

namespace hyper {
    struct TcpListenOptions {
        std::string host{"127.0.0.1"};
        std::uint16_t port{6379};
        int backlog{SOMAXCONN};
    };

    class TcpListener {
    public:
        static std::optional<TcpListener> create(const TcpListenOptions& option);

        [[nodiscard]] int fd() const noexcept {
            return fd_;
        }
        [[nodiscard]] std::uint16_t port() const noexcept {
            return port_;
        }
        TcpListener(const TcpListener&) = delete;
        TcpListener& operator=(const TcpListener&) = delete;

        TcpListener(TcpListener&& other) noexcept;
        TcpListener& operator=(TcpListener&& other) noexcept;

        ~TcpListener();

    private:
        explicit TcpListener(int fd, std::uint16_t port);

    private:
        int fd_;
        std::uint16_t port_;
    };
}
