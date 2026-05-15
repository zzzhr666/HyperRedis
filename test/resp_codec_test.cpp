#include <gtest/gtest.h>

#include "hyper/server/resp_codec.hpp"
#include "hyper/server/resp_value.hpp"

#include <memory>
#include <string>
#include <vector>

using namespace hyper;

TEST(RespCodecTest, SerializesSimpleString) {
    EXPECT_EQ(serializeRespValue(RespSimpleString{"OK"}), "+OK\r\n");
}

TEST(RespCodecTest, SerializesError) {
    EXPECT_EQ(serializeRespValue(RespError{"ERR bad command"}), "-ERR bad command\r\n");
}

TEST(RespCodecTest, SerializesInteger) {
    EXPECT_EQ(serializeRespValue(RespInteger{123}), ":123\r\n");
    EXPECT_EQ(serializeRespValue(RespInteger{-42}), ":-42\r\n");
}

TEST(RespCodecTest, SerializesBulkString) {
    EXPECT_EQ(serializeRespValue(RespBulkString{std::string{"hello"}}), "$5\r\nhello\r\n");
}

TEST(RespCodecTest, SerializesEmptyBulkString) {
    EXPECT_EQ(serializeRespValue(RespBulkString{std::string{}}), "$0\r\n\r\n");
}

TEST(RespCodecTest, BulkStringLengthUsesRawBytes) {
    EXPECT_EQ(serializeRespValue(RespBulkString{std::string{"a\r\nb"}}), "$4\r\na\r\nb\r\n");
}

TEST(RespCodecTest, SerializesNullBulkString) {
    EXPECT_EQ(serializeRespValue(respNullBulk()), "$-1\r\n");
}

TEST(RespCodecTest, SerializesArray) {
    auto array = std::make_shared<RespArray>();
    array->values.push_back(RespBulkString{std::string{"GET"}});
    array->values.push_back(RespBulkString{std::string{"key"}});

    EXPECT_EQ(serializeRespValue(array), "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
}

TEST(RespCodecTest, SerializesEmptyArray) {
    auto array = std::make_shared<RespArray>();

    EXPECT_EQ(serializeRespValue(array), "*0\r\n");
}

TEST(RespCodecTest, SerializesNestedArray) {
    auto inner = std::make_shared<RespArray>();
    inner->values.push_back(RespInteger{1});
    inner->values.push_back(RespBulkString{std::string{"two"}});

    auto outer = std::make_shared<RespArray>();
    outer->values.push_back(RespSimpleString{"OK"});
    outer->values.push_back(inner);

    EXPECT_EQ(serializeRespValue(outer), "*2\r\n+OK\r\n*2\r\n:1\r\n$3\r\ntwo\r\n");
}

TEST(RespCodecTest, SerializesNullArray) {
    std::shared_ptr<RespArray> array;

    EXPECT_EQ(serializeRespValue(array), "*-1\r\n");
}

TEST(RespCodecTest, ParsesSingleBulkCommand) {
    const std::string input = "*1\r\n$4\r\nPING\r\n";

    const auto result = parseRespCommand(input);

    EXPECT_EQ(result.status, RespParseStatus::Complete);
    EXPECT_EQ(result.command.args, std::vector<std::string>{"PING"});
    EXPECT_EQ(result.consumed, input.size());
}

TEST(RespCodecTest, ParsesCommandWithMultipleBulkArguments) {
    const std::string input = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";

    const auto result = parseRespCommand(input);

    EXPECT_EQ(result.status, RespParseStatus::Complete);
    EXPECT_EQ(result.command.args, (std::vector<std::string>{"SET", "key", "value"}));
    EXPECT_EQ(result.consumed, input.size());
}

TEST(RespCodecTest, ReportsConsumedForFirstCommandInBuffer) {
    const std::string first = "*1\r\n$4\r\nPING\r\n";
    const std::string input = first + "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";

    const auto result = parseRespCommand(input);

    EXPECT_EQ(result.status, RespParseStatus::Complete);
    EXPECT_EQ(result.command.args, std::vector<std::string>{"PING"});
    EXPECT_EQ(result.consumed, first.size());
}

TEST(RespCodecTest, ParsesBulkArgumentContainingCrLf) {
    const std::string input = "*2\r\n$3\r\nSET\r\n$4\r\na\r\nb\r\n";

    const auto result = parseRespCommand(input);

    EXPECT_EQ(result.status, RespParseStatus::Complete);
    EXPECT_EQ(result.command.args, (std::vector<std::string>{"SET", "a\r\nb"}));
    EXPECT_EQ(result.consumed, input.size());
}

TEST(RespCodecTest, ReportsIncompleteWhenArrayElementIsMissing) {
    const auto result = parseRespCommand("*1\r\n");

    EXPECT_EQ(result.status, RespParseStatus::Incomplete);
    EXPECT_EQ(result.consumed, 0U);
}

TEST(RespCodecTest, ReportsIncompleteWhenBulkPayloadIsMissing) {
    const auto result = parseRespCommand("*1\r\n$4\r\nPI");

    EXPECT_EQ(result.status, RespParseStatus::Incomplete);
    EXPECT_EQ(result.consumed, 0U);
}

TEST(RespCodecTest, RejectsInvalidCommandFrame) {
    EXPECT_EQ(parseRespCommand("+OK\r\n").status, RespParseStatus::Error);
    EXPECT_EQ(parseRespCommand("*1\r\n+OK\r\n").status, RespParseStatus::Error);
    EXPECT_EQ(parseRespCommand("*-1\r\n").status, RespParseStatus::Error);
    EXPECT_EQ(parseRespCommand("*1\r\n$-1\r\n").status, RespParseStatus::Error);
}

TEST(RespCodecTest, RejectsBulkPayloadWithoutTrailingCrLf) {
    const auto result = parseRespCommand("*1\r\n$3\r\nGET\rX");

    EXPECT_EQ(result.status, RespParseStatus::Error);
    EXPECT_EQ(result.consumed, 0U);
}

TEST(RespCodecTest, ParsesEmptyCommandArray) {
    const std::string input = "*0\r\n";

    const auto result = parseRespCommand(input);

    EXPECT_EQ(result.status, RespParseStatus::Complete);
    EXPECT_TRUE(result.command.args.empty());
    EXPECT_EQ(result.consumed, input.size());
}

TEST(RespCodecTest, ParsesEmptyBulkArgument) {
    const std::string input = "*2\r\n$4\r\nECHO\r\n$0\r\n\r\n";

    const auto result = parseRespCommand(input);

    EXPECT_EQ(result.status, RespParseStatus::Complete);
    EXPECT_EQ(result.command.args, (std::vector<std::string>{"ECHO", ""}));
    EXPECT_EQ(result.consumed, input.size());
}

TEST(RespCodecTest, ParsesBinaryBulkArgumentContainingNullByte) {
    std::string input = "*1\r\n$3\r\na";
    input.push_back('\0');
    input += "b\r\n";

    const auto result = parseRespCommand(input);

    ASSERT_EQ(result.status, RespParseStatus::Complete);
    ASSERT_EQ(result.command.args.size(), 1U);
    EXPECT_EQ(result.command.args[0], std::string("a\0b", 3));
    EXPECT_EQ(result.consumed, input.size());
}

TEST(RespCodecTest, ReportsIncompleteWhenLaterArrayElementIsMissing) {
    const auto result = parseRespCommand("*2\r\n$4\r\nPING\r\n");

    EXPECT_EQ(result.status, RespParseStatus::Incomplete);
    EXPECT_EQ(result.consumed, 0U);
}

TEST(RespCodecTest, RejectsNonNumericLengths) {
    EXPECT_EQ(parseRespCommand("*x\r\n").status, RespParseStatus::Error);
    EXPECT_EQ(parseRespCommand("*1\r\n$x\r\n").status, RespParseStatus::Error);
    EXPECT_EQ(parseRespCommand("*1x\r\n$4\r\nPING\r\n").status, RespParseStatus::Error);
    EXPECT_EQ(parseRespCommand("*1\r\n$4x\r\nPING\r\n").status, RespParseStatus::Error);
}

TEST(RespCodecTest, RejectsWrongFirstByteOfBulkTerminator) {
    const auto result = parseRespCommand("*1\r\n$3\r\nGET\n");

    EXPECT_EQ(result.status, RespParseStatus::Error);
    EXPECT_EQ(result.consumed, 0U);
}
