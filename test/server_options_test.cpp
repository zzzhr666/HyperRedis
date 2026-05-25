#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "hyper/server/server_options.hpp"

using namespace hyper;

namespace {
    struct Argv {
        explicit Argv(std::initializer_list<std::string_view> args) {
            storage.reserve(args.size());
            argv.reserve(args.size());
            for (const auto arg : args) {
                storage.emplace_back(arg);
            }
            for (auto& arg : storage) {
                argv.push_back(arg.data());
            }
        }

        [[nodiscard]] int argc() const noexcept {
            return static_cast<int>(argv.size());
        }

        std::vector<std::string> storage;
        std::vector<char*> argv;
    };
}

TEST(ServerOptionsTest, UsageMentionsPersistenceOptions) {
    const auto usage = serverUsage("hyper_redis_server");

    EXPECT_NE(usage.find("--aof PATH"), std::string::npos);
    EXPECT_NE(usage.find("--appendfsync no|always|everysec"), std::string::npos);
    EXPECT_NE(usage.find("--rdb PATH"), std::string::npos);
    EXPECT_NE(usage.find("--load-rdb"), std::string::npos);
    EXPECT_NE(usage.find("--load-aof"), std::string::npos);
    EXPECT_NE(usage.find("--save-rdb-on-stop"), std::string::npos);
}

TEST(ServerOptionsTest, ParsesAofFsyncPolicyCaseInsensitively) {
    AofFsyncPolicy policy{AofFsyncPolicy::No};

    ASSERT_TRUE(parseAofFsyncPolicy("ALWAYS", policy));
    EXPECT_EQ(policy, AofFsyncPolicy::Always);

    ASSERT_TRUE(parseAofFsyncPolicy("EverySec", policy));
    EXPECT_EQ(policy, AofFsyncPolicy::EverySecond);

    ASSERT_TRUE(parseAofFsyncPolicy("no", policy));
    EXPECT_EQ(policy, AofFsyncPolicy::No);
}

TEST(ServerOptionsTest, RejectsInvalidAofFsyncPolicy) {
    AofFsyncPolicy policy{AofFsyncPolicy::Always};

    EXPECT_FALSE(parseAofFsyncPolicy("sometimes", policy));
    EXPECT_EQ(policy, AofFsyncPolicy::Always);
}

TEST(ServerOptionsTest, ParsesValidPortRange) {
    std::uint16_t port{};

    EXPECT_TRUE(parseServerPort("0", port));
    EXPECT_EQ(port, 0);

    EXPECT_TRUE(parseServerPort("65535", port));
    EXPECT_EQ(port, 65535);
}

TEST(ServerOptionsTest, RejectsInvalidPortValues) {
    std::uint16_t port{1234};

    EXPECT_FALSE(parseServerPort("-1", port));
    EXPECT_EQ(port, 1234);

    EXPECT_FALSE(parseServerPort("65536", port));
    EXPECT_EQ(port, 1234);

    EXPECT_FALSE(parseServerPort("12x", port));
    EXPECT_EQ(port, 1234);
}

TEST(ServerOptionsTest, ParsesListenAndPersistenceOptions) {
    Argv argv{
        "hyper_redis_server",
        "--host", "127.0.0.1",
        "--port", "6380",
        "--aof", "/tmp/hyper.aof",
        "--appendfsync", "everysec",
        "--rdb", "/tmp/hyper.rdb",
        "--load-rdb",
        "--load-aof",
        "--save-rdb-on-stop"
    };
    RedisServerRunnerConfig config;

    const auto result = parseServerOptions(argv.argc(), argv.argv.data(), config);

    EXPECT_TRUE(result.ok);
    EXPECT_FALSE(result.help_requested);
    EXPECT_TRUE(result.error.empty());
    EXPECT_EQ(config.listen_options.host, "127.0.0.1");
    EXPECT_EQ(config.listen_options.port, 6380);
    ASSERT_TRUE(config.persistence.aof_path.has_value());
    EXPECT_EQ(config.persistence.aof_path->string(), "/tmp/hyper.aof");
    EXPECT_EQ(config.persistence.append_fsync_policy, AofFsyncPolicy::EverySecond);
    ASSERT_TRUE(config.persistence.rdb_path.has_value());
    EXPECT_EQ(config.persistence.rdb_path->string(), "/tmp/hyper.rdb");
    EXPECT_TRUE(config.persistence.load_rdb_on_start);
    EXPECT_TRUE(config.persistence.load_aof_on_start);
    EXPECT_TRUE(config.persistence.save_rdb_on_stop);
}

TEST(ServerOptionsTest, HelpReturnsHelpRequested) {
    Argv argv{"hyper_redis_server", "--help"};
    RedisServerRunnerConfig config;

    const auto result = parseServerOptions(argv.argc(), argv.argv.data(), config);

    EXPECT_FALSE(result.ok);
    EXPECT_TRUE(result.help_requested);
    EXPECT_TRUE(result.error.empty());
}

TEST(ServerOptionsTest, RejectsMissingOptionValues) {
    const std::array<std::string_view, 5> options{
        "--host", "--port", "--aof", "--appendfsync", "--rdb"
    };

    for (const auto option : options) {
        Argv argv{"hyper_redis_server", option};
        RedisServerRunnerConfig config;

        const auto result = parseServerOptions(argv.argc(), argv.argv.data(), config);

        EXPECT_FALSE(result.ok) << option;
        EXPECT_FALSE(result.help_requested) << option;
        EXPECT_FALSE(result.error.empty()) << option;
    }
}

TEST(ServerOptionsTest, RejectsInvalidOptionValuesAndUnknownArguments) {
    {
        Argv argv{"hyper_redis_server", "--port", "bad"};
        RedisServerRunnerConfig config;

        const auto result = parseServerOptions(argv.argc(), argv.argv.data(), config);

        EXPECT_FALSE(result.ok);
        EXPECT_NE(result.error.find("invalid port"), std::string::npos);
    }
    {
        Argv argv{"hyper_redis_server", "--appendfsync", "sometimes"};
        RedisServerRunnerConfig config;

        const auto result = parseServerOptions(argv.argc(), argv.argv.data(), config);

        EXPECT_FALSE(result.ok);
        EXPECT_NE(result.error.find("invalid appendfsync policy"), std::string::npos);
    }
    {
        Argv argv{"hyper_redis_server", "--unknown"};
        RedisServerRunnerConfig config;

        const auto result = parseServerOptions(argv.argc(), argv.argv.data(), config);

        EXPECT_FALSE(result.ok);
        EXPECT_NE(result.error.find("unknown argument"), std::string::npos);
    }
}
