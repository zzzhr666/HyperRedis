#pragma once

#include <filesystem>

#include "database.hpp"
#include "redis_manager.hpp"

namespace hyper {
    class RdbSaver {
    public:
        explicit RdbSaver(const std::filesystem::path& path);

        [[nodiscard]] const std::filesystem::path& path() const noexcept {
            return path_;
        }

        void setPath(const std::filesystem::path& path) {
            path_ = path;
        }

        [[nodiscard]] bool save(RedisManager& manager, ExpireTimePoint now) const;

        [[nodiscard]] bool load(RedisManager& manager, ExpireTimePoint now) const;

    private:
        std::filesystem::path path_;
    };
}
