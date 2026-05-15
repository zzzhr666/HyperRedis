#include "hyper/storage/rdb_saver.hpp"

#include <fstream>
#include <utility>

#include "hyper/storage/snapshot.hpp"


hyper::RdbSaver::RdbSaver(std::filesystem::path  path) : path_(std::move(path)) {}

bool hyper::RdbSaver::save(RedisManager& manager, ExpireTimePoint now) const {
    auto bytes = Snapshot::save(manager, now);
    auto tmp_path = path_;
    tmp_path += ".tmp";

    std::ofstream out_file(tmp_path, std::ios::binary | std::ios::trunc);
    if (!out_file) {
        return false;
    }
    out_file.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    out_file.close();
    if (!out_file.good()) {
        std::filesystem::remove(tmp_path);
        return false;
    }
    std::error_code ec;
    std::filesystem::rename(tmp_path, path_, ec);
    if (ec) {
        std::filesystem::remove(tmp_path);
        return false;
    }
    return true;
}

bool hyper::RdbSaver::load(RedisManager& manager, ExpireTimePoint now) const {
    if (!std::filesystem::exists(path_) || !std::filesystem::is_regular_file(path_)) {
        return false;
    }
    auto file_size = std::filesystem::file_size(path_);
    std::ifstream input_file(path_, std::ios::binary);
    if (!input_file) {
        return false;
    }
    std::vector<std::uint8_t> bytes(file_size);
    input_file.read(reinterpret_cast<char*>(bytes.data()),
                    static_cast<std::streamsize>(bytes.size()));
    if (!input_file.good() && input_file.gcount() != static_cast<std::streamsize>(bytes.size())) {
        return false;
    }
    const bool res = Snapshot::load(bytes, manager, now);
    input_file.close();
    return res;
}
