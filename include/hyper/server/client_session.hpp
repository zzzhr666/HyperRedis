#pragma once

#include <string>
#include <string_view>

#include "hyper/time.hpp"
#include "hyper/server/client_context.hpp"


namespace hyper {
    class RedisServer;

    class ClientSession {
    public:
        explicit ClientSession(int fd);

        [[nodiscard]] int fd() const noexcept {
            return fd_;
        }

        [[nodiscard]] std::string_view queryBuffer() const noexcept {
            return query_buffer_;
        }

        [[nodiscard]] std::string_view replyBuffer() const noexcept {
            return reply_buffer_;
        }

        [[nodiscard]] RedisClientContext& context() noexcept {
            return context_;
        }

        [[nodiscard]] const RedisClientContext& context() const noexcept {
            return context_;
        }

        [[nodiscard]] bool closeAfterReply() const noexcept {
            return close_after_reply_;
        }

        void appendQueryBytes(std::string_view bytes);

        [[nodiscard]] std::string takeReplyBytes();

        void consumeReplyBytes(std::size_t count);

        void processInput(RedisServer& server, ExpireTimePoint now);

    private:
        int fd_;
        bool close_after_reply_;
        std::string query_buffer_;
        std::string reply_buffer_;
        RedisClientContext context_;
    };
}
