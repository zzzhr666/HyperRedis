#pragma once

#include <filesystem>

#include "hyper/time.hpp"


namespace hyper {
    class RedisManager;


    class AofReplayer {
    public:
        explicit AofReplayer(std::filesystem::path path);


        bool replay(RedisManager& manager, ExpireTimePoint now);

    private:
        std::filesystem::path path_;
    };
}
