#pragma once

#include <chrono>
#include <cstdint>

namespace hyper {
    using ExpireClock = std::chrono::system_clock;
    using ExpireTimePoint = ExpireClock::time_point;
    using Milliseconds = std::chrono::milliseconds;
    using UnixMilliseconds = std::int64_t;
}
