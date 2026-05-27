#pragma once

#include <string_view>

namespace hyper {
    // Project metadata
    static constexpr std::string_view ProjectVersion = "0.1.0";

    // RDB format metadata
    static constexpr std::string_view RdbMagic = "REDIS";
    static constexpr std::string_view RdbVersion = "0009";
}
