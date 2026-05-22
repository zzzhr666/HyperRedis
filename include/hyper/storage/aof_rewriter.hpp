#pragma once
#include <filesystem>

#include "hyper/time.hpp"

namespace hyper {
    class RedisManager;

    class AofRewriter {
    public:
        explicit AofRewriter(std::filesystem::path path);

        // Writes a complete rewritten AOF to path_; caller owns atomic replacement.
        [[nodiscard]] bool rewrite(RedisManager& manager, ExpireTimePoint now) const;

    private:
        std::filesystem::path path_;
    };
}
