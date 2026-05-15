#include "hyper/storage/checksum_calculator.hpp"

std::uint64_t hyper::ChecksumCalculator::calculate(std::span<const std::uint8_t> bytes) {
    constexpr std::uint64_t offsetBasis = 14695981039346656037ULL;
    constexpr std::uint64_t prime = 1099511628211ULL;
    std::uint64_t hash = offsetBasis;
    for (auto byte : bytes) {
        hash ^= byte;
        hash *= prime;
    }
    return hash;
}
