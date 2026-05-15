#include "hyper/server/resp_codec.hpp"

#include <memory>
#include <string>
#include <type_traits>
#include <variant>
#include <optional>
#include <charconv>
#include <cstdint>

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

    struct LineResult {
        hyper::RespParseStatus status{};
        std::string_view line{};
        std::size_t next{};
    };

    LineResult readRespLine(std::string_view input, std::size_t line_start) {
        for (std::size_t i = line_start; i < input.size(); ++i) {
            if (input[i] == '\n') {
                if (i == line_start || input[i - 1] != '\r') {
                    return {hyper::RespParseStatus::Error, {}, 0};
                }
                return {hyper::RespParseStatus::Complete, input.substr(line_start, i - line_start - 1), i + 1};
            }
            if (input[i] == '\r' && i + 1 < input.size() && input[i + 1] != '\n') {
                return {hyper::RespParseStatus::Error, {}, 0};
            }
        }
        return {hyper::RespParseStatus::Incomplete, {}, 0};
    }

    std::optional<std::int64_t> parseRespInteger(std::string_view text) {
        if (text.empty()) {
            return std::nullopt;
        }
        auto* begin = text.data();
        auto* end = begin + text.size();
        std::int64_t value{};
        auto [ptr , ec] = std::from_chars(begin, end, value);
        if (ptr == end && ec == std::errc()) {
            return value;
        }
        return std::nullopt;
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

hyper::RespParseResult hyper::parseRespCommand(std::string_view input) {
    if (input.empty()) {
        return {RespParseStatus::Incomplete, {}, 0};
    }
    if (input[0] != '*') {
        return {RespParseStatus::Error, {}, 0};
    }
    auto [status,str_view,next] = readRespLine(input, 1);
    if (status != RespParseStatus::Complete) {
        return {status, {}, {}};
    }
    auto len_opt = parseRespInteger(str_view);
    if (!len_opt.has_value()) {
        return {RespParseStatus::Error, {}, 0};
    }
    auto len = len_opt.value();
    if (len < 0) {
        return {RespParseStatus::Error, {}, 0};
    }

    RespParseResult res;
    std::size_t current_pos{next};
    for (std::size_t i = 0; i < static_cast<std::size_t>(len); ++i) {
        if (current_pos >= input.size()) {
            return {RespParseStatus::Incomplete, {}, 0};
        }
        if (input[current_pos] != '$') {
            return {RespParseStatus::Error, {}, 0};
        }
        ++current_pos;
        auto [parse_status,parse_str_view,parse_next] = readRespLine(input, current_pos);
        if (parse_status != RespParseStatus::Complete) {
            return {parse_status, {}, {}};
        }
        auto strlen_opt = parseRespInteger(parse_str_view);
        if (!strlen_opt.has_value()) {
            return {RespParseStatus::Error, {}, 0};
        }
        auto str_len = strlen_opt.value();
        if (str_len < 0) {
            return {RespParseStatus::Error, {}, 0};
        }
        auto bulk_len = static_cast<std::size_t>(str_len);
        current_pos = parse_next;
        if (input.size() - current_pos < bulk_len) {
            return {RespParseStatus::Incomplete, {}, 0};
        }
        std::string_view command(input.substr(current_pos, bulk_len));
        current_pos += bulk_len;

        if (current_pos >= input.size()) {
            return {RespParseStatus::Incomplete, {}, 0};
        }
        if (input[current_pos] != '\r') {
            return {RespParseStatus::Error, {}, 0};
        }
        if (current_pos + 1 >= input.size()) {
            return {RespParseStatus::Incomplete, {}, 0};
        }
        if (input[current_pos + 1] != '\n') {
            return {RespParseStatus::Error, {}, 0};
        }
        current_pos += 2;
        res.command.args.emplace_back(command);
    }
    res.consumed = current_pos;
    res.status = RespParseStatus::Complete;
    return res;
}
