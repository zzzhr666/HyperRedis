#include "hyper/storage/aof_replayer.hpp"

#include <fstream>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>


#include "hyper/server/client_context.hpp"
#include "hyper/server/command_executor.hpp"
#include "hyper/server/resp_codec.hpp"
#include "hyper/server/resp_value.hpp"
#include "hyper/storage/redis_manager.hpp"


hyper::AofReplayResult hyper::AofReplayer::replay(const std::filesystem::path& path, RedisManager& manager,
                                                  ExpireTimePoint now) {
    if (!std::filesystem::exists(path) || !std::filesystem::is_regular_file(path)) {
        return {};
    }
    auto data_size = std::filesystem::file_size(path);
    if (data_size > static_cast<std::uintmax_t>(std::numeric_limits<std::size_t>::max())) {
        return {};
    }
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return {};
    }
    std::string data;
    data.resize(data_size);
    in.read(data.data(), static_cast<std::streamsize>(data_size));
    if (in.gcount() != static_cast<std::streamsize>(data_size)) {
        return {};
    }
    RedisManager tmp_manager(manager.dbCount());
    RedisClientContext reply_client;
    CommandExecutor executor;
    std::size_t offset{0};
    while (offset < data.size()) {
        auto [status,command,consumed] = parseRespCommand(data.substr(offset));
        if (status != RespParseStatus::Complete) {
            return {};
        }
        std::vector<std::string_view> views{command.args.begin(), command.args.end()};

        if (const auto reply = executor.execute(tmp_manager, reply_client, views, now); std::holds_alternative<RespError>(reply)) {
            return {};
        }

        offset += consumed;
    }
    manager.swapAll(tmp_manager);
    return {true,reply_client.dbIndex()};
}
