#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <string_view>

#include "hyper/time.hpp"


namespace hyper {
    enum class AofFsyncPolicy : std::uint8_t {
        No,
        Always,
        EverySecond
    };

    class AofAppender {
    public:
        explicit AofAppender(std::filesystem::path path, AofFsyncPolicy policy = AofFsyncPolicy::No);

        ~AofAppender();

        AofAppender(const AofAppender&) = delete;

        AofAppender& operator=(const AofAppender&) = delete;

        [[nodiscard]] const std::filesystem::path& path() const noexcept {
            return path_;
        }

        [[nodiscard]] bool isBroken() const noexcept {
            return broken_;
        }

        [[nodiscard]] bool appendCommand(std::size_t db_index,
                                         std::span<const std::string_view> args,
                                         ExpireTimePoint now);

    private:
        [[nodiscard]] bool syncIfDue_(ExpireTimePoint now);

        [[nodiscard]] bool appendBytes_(std::string_view bytes);

        void closeFd_();

        std::filesystem::path path_;
        std::size_t selected_db_index_;
        bool broken_;
        AofFsyncPolicy fsync_policy_;
        bool fsync_pending_;
        int fd_;
        std::optional<ExpireTimePoint> last_fsync_time_;
    };
}
