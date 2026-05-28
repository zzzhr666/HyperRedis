#include <gtest/gtest.h>

#include "hyper/server/client_context.hpp"
#include "hyper/server/command_executor.hpp"
#include "hyper/server/resp_value.hpp"
#include "hyper/storage/database.hpp"
#include "hyper/storage/redis_manager.hpp"

#include <array>
#include <span>
#include <string>
#include <string_view>
#include <variant>

using namespace hyper;

namespace {
    RespValue execute(CommandExecutor& executor,
                      RedisManager& manager,
                      RedisClientContext& client,
                      std::initializer_list<std::string_view> args) {
        std::vector<std::string_view> v_args(args);
        return executor.execute(manager, client, std::span<const std::string_view>{v_args.data(), v_args.size()}, ExpireTimePoint{});
    }
}

class TimeCommandTest : public ::testing::Test {
protected:
    RedisManager manager;
    RedisClientContext client;
    CommandExecutor executor;
};

TEST_F(TimeCommandTest, TimeReturnsTwoElements) {
    RespValue res = execute(executor, manager, client, {"TIME"});
    
    // Check if it's a RespArray
    const auto* arr_ptr = std::get_if<std::shared_ptr<RespArray>>(&res);
    ASSERT_NE(arr_ptr, nullptr) << "Expected RespArray for TIME command";
    ASSERT_NE(*arr_ptr, nullptr);
    
    const auto& arr = **arr_ptr;
    ASSERT_EQ(arr.values.size(), 2) << "TIME should return 2 elements";
    
    // Both elements should be Bulk Strings
    const auto* sec_ptr = std::get_if<RespBulkString>(&arr.values[0]);
    const auto* usec_ptr = std::get_if<RespBulkString>(&arr.values[1]);
    
    ASSERT_NE(sec_ptr, nullptr);
    ASSERT_NE(usec_ptr, nullptr);
    ASSERT_TRUE(sec_ptr->value.has_value());
    ASSERT_TRUE(usec_ptr->value.has_value());
    
    // They should represent positive integers
    long seconds = std::stol(*sec_ptr->value);
    long microseconds = std::stol(*usec_ptr->value);
    
    EXPECT_GT(seconds, 1600000000L); // Roughly after 2020
    EXPECT_GE(microseconds, 0);
    EXPECT_LT(microseconds, 1000000);
}
