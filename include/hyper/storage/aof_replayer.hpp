#pragma once

#include <chrono>
#include <filesystem>


namespace hyper {
    using ExpireTimePoint = std::chrono::system_clock::time_point;
    class RedisManager;


    class AofReplayer {
    public:
        explicit AofReplayer(std::filesystem::path path);


        bool replay(RedisManager& manager, ExpireTimePoint now);

    private:
        std::filesystem::path path_;
    };
}
