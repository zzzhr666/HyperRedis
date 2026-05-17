#include "hyper/server/command_processor.hpp"

#include <algorithm>

#include "hyper/server/client_context.hpp"
#include "hyper/server/command_registry.hpp"
#include "hyper/storage/aof_appender.hpp"


hyper::CommandProcessor::CommandProcessor(AofAppender* appender) : appender_(appender) {}

hyper::RespValue hyper::CommandProcessor::execute(RedisManager& manager, RedisClientContext& client,
                                                  CommandExecutor::Args args, ExpireTimePoint now) const {
    if (args.empty()) {
        return respError(std::string(ErrEmptyCommand));
    }

    std::string cmd(args[0]);
    std::ranges::transform(cmd, cmd.begin(), [](unsigned char c) {
        return std::toupper(c);
    });

    auto search_res = findCommand(cmd);
    if (search_res == nullptr) {
        return respError(std::string(ErrUnknownCommand));
    }
    std::size_t current_db_index = client.dbIndex();
    if (search_res->write && appender_ != nullptr && appender_->isBroken()) {
        return respError(std::string(ErrAofWriteFailed));
    }
    auto ret = executor_.execute(manager, client, args, now);
    if (!std::holds_alternative<RespError>(ret) && search_res->write && appender_ != nullptr) {
        if (!appender_->appendCommand(current_db_index, args, now)) {
            return respError(std::string(ErrAofWriteFailed));
        }
    }
    return ret;
}
