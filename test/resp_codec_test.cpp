#include <gtest/gtest.h>

#include "hyper/server/resp_codec.hpp"
#include "hyper/server/resp_value.hpp"

#include <memory>
#include <string>

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
