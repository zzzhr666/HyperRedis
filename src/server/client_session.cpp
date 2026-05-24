#include "hyper/server/client_session.hpp"

#include <algorithm>
#include <string_view>
#include <vector>

#include "hyper/server/redis_server.hpp"
#include "hyper/server/resp_codec.hpp"
#include "hyper/server/resp_value.hpp"

hyper::ClientSession::ClientSession(int fd) : fd_(fd), close_after_reply_(false) {}

void hyper::ClientSession::appendQueryBytes(std::string_view bytes) {
    query_buffer_.append(bytes);
}

std::string hyper::ClientSession::takeReplyBytes() {
    std::string output{};
    output.swap(reply_buffer_);
    return output;
}

void hyper::ClientSession::consumeReplyBytes(std::size_t count) {
    reply_buffer_.erase(0, std::min(count, reply_buffer_.size()));
}

void hyper::ClientSession::processInput(RedisServer& server, ExpireTimePoint now) {
    while (!query_buffer_.empty() && !close_after_reply_) {
        auto [status,command,consumed] = parseRespCommand(query_buffer_);
        if (status == RespParseStatus::Incomplete) {
            break;
        }
        if (status == RespParseStatus::Error) {
            reply_buffer_.append(serializeRespValue(respError("ERR protocol error")));
            close_after_reply_ = true;
            break;
        }
        std::vector<std::string_view> input{command.args.begin(), command.args.end()};
        auto reply = server.execute(context_, input, now);
        reply_buffer_.append(serializeRespValue(reply));
        query_buffer_.erase(0, consumed);
    }
}
