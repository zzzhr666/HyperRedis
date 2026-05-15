#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace hyper {
    struct ChecksumCalculator {
        static std::uint64_t calculate(std::span<const std::uint8_t> bytes);

        static std::uint64_t calculate(const std::vector<std::uint8_t>& bytes) {
            return calculate(std::span<const std::uint8_t>{bytes.data(), bytes.size()});
        }
    };
}
