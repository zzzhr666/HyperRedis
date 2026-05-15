#include "hyper/server/resp_codec.hpp"

#include <memory>
#include <string>
#include <type_traits>
#include <variant>

namespace {
    // serialize helper
    std::string serializeSimpleString(const hyper::RespSimpleString& str) {
        return "+" + str.value + "\r\n";
    }

    std::string serializeRespError(const hyper::RespError& err) {
        return "-" + err.message + "\r\n";
    }

    std::string serializeRespInteger(hyper::RespInteger n) {
        return ":" + std::to_string(n.value) + "\r\n";
    }

    std::string serializeRespBulkString(const hyper::RespBulkString& str) {
        if (str.value.has_value()) {
            const auto& value = str.value.value();
            return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
        }
        return "$-1\r\n";
    }

    std::string serializeRespArray(const std::shared_ptr<hyper::RespArray>& array) {
        if (array == nullptr) {
            return "*-1\r\n";
        }
        std::string res = "*" + std::to_string(array->values.size()) + "\r\n";
        for (const auto& item : array->values) {
            res += hyper::serializeRespValue(item);
        }
        return res;
    }


}

std::string hyper::serializeRespValue(const RespValue& value) {
    return std::visit([]<typename T0>(const T0& arg)-> std::string {
        using T = std::remove_cvref_t<T0>;
        if constexpr (std::is_same_v<T, RespSimpleString>) {
            return serializeSimpleString(arg);
        } else if constexpr (std::is_same_v<T, RespError>) {
            return serializeRespError(arg);
        } else if constexpr (std::is_same_v<T, RespInteger>) {
            return serializeRespInteger(arg);
        } else if constexpr (std::is_same_v<T, RespBulkString>) {
            return serializeRespBulkString(arg);
        } else if constexpr (std::is_same_v<T, std::shared_ptr<RespArray>>) {
            return serializeRespArray(arg);
        } else {
            return {};
        }
    }, value);
}
