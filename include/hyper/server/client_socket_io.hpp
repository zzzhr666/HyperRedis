#pragma once

#include <cstddef>

#include "hyper/time.hpp"

namespace hyper {
    class CommandProcessor;
    class ClientSession;
    class RedisManager;

    enum class ClientIoStatus {
        Ok,
        WouldBlock,
        Closed,
        Error
    };

    struct ClientIoResult {
        ClientIoStatus status{ClientIoStatus::Ok};
        std::size_t bytes{};
    };

    [[nodiscard]] ClientIoResult readClientQuery(ClientSession& client,
                                                 RedisManager& manager,
                                                 const CommandProcessor& processor,
                                                 ExpireTimePoint now);

    [[nodiscard]] ClientIoResult writeClientReply(ClientSession& client);
}
