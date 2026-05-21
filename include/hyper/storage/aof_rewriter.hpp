#pragma once
#include <chrono>
#include <filesystem>

namespace hyper {
    class RedisManager;

    using ExpireTimePoint = std::chrono::system_clock::time_point;

    class AofRewriter {
    public:
        explicit AofRewriter(std::filesystem::path path);

        [[nodiscard]] bool rewrite(RedisManager& manager, ExpireTimePoint now) const;

    private:
        std::filesystem::path path_;
    };
}
