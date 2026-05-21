#pragma once
#include <chrono>
#include <filesystem>

namespace hyper {
    class RedisManager;

    using ExpireTimePoint = std::chrono::system_clock::time_point;

    class AofRewriter {
    public:
        explicit AofRewriter(std::filesystem::path path);

        // Writes a complete rewritten AOF to path_; caller owns atomic replacement.
        [[nodiscard]] bool rewrite(RedisManager& manager, ExpireTimePoint now) const;

    private:
        std::filesystem::path path_;
    };
}
