#pragma once

#include <cstddef>

#include "hyper/time.hpp"

namespace hyper {
    class ClientSession;
    class RedisServer;

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
                                                 RedisServer& server,
                                                 ExpireTimePoint now);

    [[nodiscard]] ClientIoResult writeClientReply(ClientSession& client);
}
