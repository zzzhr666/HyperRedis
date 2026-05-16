#include "hyper/storage/aof_appender.hpp"

#include <array>
#include <fstream>
#include <string_view>

#include "hyper/server/resp_codec.hpp"

namespace {
    std::string serializeSelectCommand(std::size_t db_index) {
        const std::string index = std::to_string(db_index);
        const std::array<std::string_view, 2> args{"SELECT", index};
        return hyper::serializeRespCommand(args);
    }
}

hyper::AofAppender::AofAppender(std::filesystem::path path) : path_(std::move(path)), selected_db_index_(0) {}

bool hyper::AofAppender::appendCommand(std::size_t db_index, std::span<const std::string_view> args) {
    std::ofstream out(path_, std::ios::binary | std::ios::app);
    if (!out) {
        return false;
    }
    std::string bytes{};
    const bool index_changed = db_index != selected_db_index_;
    if (index_changed) {
        bytes.append(serializeSelectCommand(db_index));
    }
    bytes.append(serializeRespCommand(args));
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    if (!out.good()) {
        return false;
    }
    if (index_changed) {
        selected_db_index_ = db_index;
    }
    return true;
}
