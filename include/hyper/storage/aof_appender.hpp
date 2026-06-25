#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <string_view>
#include <spdlog/common.h>

#include "hyper/time.hpp"


namespace hyper {
    enum class AofFsyncPolicy : std::uint8_t {
        No,
        Always,
        EverySecond
    };

    std::string policyToString(AofFsyncPolicy policy);

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

        void startRewrite() noexcept;

        [[nodiscard]] std::string stopRewrite() noexcept;

        [[nodiscard]] bool appendCommand(std::size_t db_index,
                                         std::span<const std::string_view> args,
                                         ExpireTimePoint now);

        void setSelectedDbIndex(std::size_t db_index) noexcept {
            selected_db_index_ = db_index;
        }


        [[nodiscard]] AofFsyncPolicy fsyncPolicy() const noexcept {
            return fsync_policy_;
        }


        void setFsyncPolicy(AofFsyncPolicy policy) noexcept {
            fsync_policy_ = policy;
        }

        bool flushIfNeeded(ExpireTimePoint now);

        void reload();

    private:
        [[nodiscard]] bool syncIfDue_(ExpireTimePoint now);

        [[nodiscard]] bool appendBytes_(std::string_view bytes);

        void closeFd_();

    private:
        std::filesystem::path path_;
        std::size_t selected_db_index_;
        bool broken_;
        AofFsyncPolicy fsync_policy_;
        bool fsync_pending_;
        int fd_;
        std::optional<ExpireTimePoint> last_fsync_time_;
        bool rewrite_in_progress_;
        std::string rewrite_buffer_;
    };
}
