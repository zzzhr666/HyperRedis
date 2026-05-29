#pragma once
#include <filesystem>

#include "hyper/time.hpp"

namespace hyper {
    class RedisManager;

    class AofRewriter {
    public:
        // Writes a complete rewritten AOF to path_; caller owns atomic replacement.
        [[nodiscard]] static  bool rewrite(const std::filesystem::path& path, RedisManager& manager, ExpireTimePoint now);
    };
}
