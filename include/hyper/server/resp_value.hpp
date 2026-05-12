#pragma once

#include <cstdint>
#include <utility>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace hyper {
    struct RespSimpleString {
        std::string value;
    };

    struct RespError {
        std::string message;
    };

    struct RespInteger {
        std::int64_t value{};
    };

    struct RespBulkString {
        std::optional<std::string> value;
    };

    struct RespArray;

    using RespValue = std::variant<
        RespSimpleString,
        RespError,
        RespInteger,
        RespBulkString,
        std::shared_ptr<RespArray>
    >;

    struct RespArray {
        std::vector<RespValue> values;
    };

    [[nodiscard]]inline RespValue respOk() {
        return RespSimpleString{"OK"};
    }

    [[nodiscard]]inline RespValue respPong() {
        return RespSimpleString{"PONG"};
    }

    [[nodiscard]]inline RespValue respNullBulk() {
        return RespBulkString{std::nullopt};
    }

    [[nodiscard]]inline RespValue respBulk(std::string value) {
        return RespBulkString{std::move(value)};
    }

    [[nodiscard]]inline RespValue respInteger(std::int64_t value) {
        return RespInteger{value};
    }

    [[nodiscard]]inline RespValue respError(std::string message) {
        return RespError{std::move(message)};
    }
}
