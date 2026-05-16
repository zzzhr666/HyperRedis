#pragma once

#include <filesystem>
#include <string_view>
#include <span>


namespace hyper {
    class AofAppender {
    public:
        explicit AofAppender(std::filesystem::path path);

        [[nodiscard]] const std::filesystem::path& path() const noexcept {
            return path_;
        }

        [[nodiscard]] bool appendCommand(std::size_t db_index, std::span<const std::string_view> args);

    private:
        std::filesystem::path path_;
        std::size_t selected_db_index_;
    };
}
