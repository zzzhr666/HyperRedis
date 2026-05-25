#pragma once

#include <filesystem>

#include "hyper/time.hpp"


namespace hyper {
    class RedisManager;

    struct AofReplayResult {
        bool ok{false};
        std::size_t selected_db_index{0};
    };
    class AofReplayer {
    public:
        static AofReplayResult replay(const std::filesystem::path& path, RedisManager& manager, ExpireTimePoint now);

    };
}
