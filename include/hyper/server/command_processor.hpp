#pragma once


#include "command_executor.hpp"


namespace hyper {
    class AofAppender;

    class CommandProcessor {
    public:
        explicit CommandProcessor(AofAppender* appender = nullptr);

        [[nodiscard]] RespValue execute(RedisManager& manager,
                                        RedisClientContext& client,
                                        CommandExecutor::Args args,
                                        ExpireTimePoint now) const;

    private:
        CommandExecutor executor_;
        AofAppender* appender_;
    };
}
