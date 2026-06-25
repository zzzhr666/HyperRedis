#include <gtest/gtest.h>
#include "hyper/server/redis_server.hpp"
#include "hyper/server/client_session.hpp"

using namespace hyper;

class PubSubStateTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 创建一个简单的 server 实例，不需要配置 RDB/AOF
        server = std::make_unique<RedisServer>(1); 
        // 手动构造两个 dummy fd 用来模拟 Session
        session1 = std::make_unique<ClientSession>(100);
        session2 = std::make_unique<ClientSession>(101);
    }

    std::unique_ptr<RedisServer> server;
    std::unique_ptr<ClientSession> session1;
    std::unique_ptr<ClientSession> session2;
};

// 检查点 1: 基本的订阅和退订
TEST_F(PubSubStateTest, SubscribeAndUnsubscribe) {
    EXPECT_TRUE(server->subscribe(session1.get(), "news"));
    // 重复订阅应返回 false
    EXPECT_FALSE(server->subscribe(session1.get(), "news"));

    // 退订
    EXPECT_TRUE(server->unsubscribe(session1.get(), "news"));
    EXPECT_FALSE(server->unsubscribe(session1.get(), "news")); // 重复退订
}

// 检查点 2: 验证客户端级追踪（用于 ClientContext 或 ClientSession）
TEST_F(PubSubStateTest, ClientContextTracksChannels) {
    server->subscribe(session1.get(), "sports");
    server->subscribe(session1.get(), "music");
    
    // 客户端内部应该记录了这两个频道
    EXPECT_EQ(session1->context().pubsubChannels().size(), 2);
    EXPECT_TRUE(session1->context().pubsubChannels().contains("sports"));
}

// 检查点 3: 客户端断线时的清理机制
TEST_F(PubSubStateTest, UnsubscribeAllOnClientDisconnect) {
    server->subscribe(session1.get(), "tech");
    server->subscribe(session2.get(), "tech");

    // 模拟 session1 退订所有频道
    server->unsubscribeAll(session1.get());

    EXPECT_EQ(session1->context().pubsubChannels().size(), 0);
    // 退订后再次退订tech会失败
    EXPECT_FALSE(server->unsubscribe(session1.get(), "tech"));
    
    // session2 仍然保持订阅 (我们可以通过它退订成功来侧面验证)
    EXPECT_TRUE(server->unsubscribe(session2.get(), "tech"));
}

// 检查点 4: 发布消息并验证缓冲区
TEST_F(PubSubStateTest, PublishMessages) {
    server->subscribe(session1.get(), "news");
    server->subscribe(session2.get(), "news");

    // 向 "news" 发布消息 "hello"
    int receivers = server->publish("news", "hello");
    EXPECT_EQ(receivers, 2);

    // 验证 session1 的缓冲区是否收到了正确的 RESP 数组
    std::string expected = "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n";
    EXPECT_EQ(session1->replyBuffer(), expected);
    EXPECT_EQ(session2->replyBuffer(), expected);

    // 向无人订阅的频道发布
    int no_receivers = server->publish("sports", "goal");
    EXPECT_EQ(no_receivers, 0);
}

// 检查点 5: 验证完整的命令执行 (SUBSCRIBE, PUBLISH, UNSUBSCRIBE)
TEST_F(PubSubStateTest, PubSubCommandsExecution) {
    // 1. 模拟 session1 执行 SUBSCRIBE news sports
    std::vector<std::string_view> sub_vec{"SUBSCRIBE", "news", "sports"};
    CommandExecutor::Args sub_args{sub_vec};
    auto sub_reply = server->execute(session1->context(), sub_args, ExpireClock::now());
    
    if (std::holds_alternative<RespError>(sub_reply)) {
        std::cerr << "sub_reply error: " << std::get<RespError>(sub_reply).message << std::endl;
    }
    // 返回值应该是最后一个频道的响应 (sports)
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<RespArray>>(sub_reply));
    auto sub_arr = std::get<std::shared_ptr<RespArray>>(sub_reply);
    EXPECT_EQ(sub_arr->values.size(), 3);
    EXPECT_EQ(std::get<RespBulkString>(sub_arr->values[1]).value.value(), "sports");
    EXPECT_EQ(std::get<RespInteger>(sub_arr->values[2]).value, 2);

    // 验证 session1 的 buffer 里被手动塞入了前一个频道 (news) 的响应
    std::string expected_news = "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n";
    EXPECT_EQ(session1->takeReplyBytes(), expected_news);

    // 2. 模拟 session2 执行 PUBLISH news "hello"
    std::vector<std::string_view> pub_vec{"PUBLISH", "news", "hello"};
    CommandExecutor::Args pub_args{pub_vec};
    auto pub_reply = server->execute(session2->context(), pub_args, ExpireClock::now());
    
    EXPECT_TRUE(std::holds_alternative<RespInteger>(pub_reply));
    EXPECT_EQ(std::get<RespInteger>(pub_reply).value, 1); // 只有 session1 收到了

    // 验证 session1 收到了推送的 message
    std::string expected_msg = "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n";
    EXPECT_EQ(session1->takeReplyBytes(), expected_msg);

    // 3. 模拟 session1 执行无参数的 UNSUBSCRIBE (退订所有)
    std::vector<std::string_view> unsub_vec{"UNSUBSCRIBE"};
    CommandExecutor::Args unsub_args{unsub_vec};
    auto unsub_reply = server->execute(session1->context(), unsub_args, ExpireClock::now());
    
    // UNSUBSCRIBE 会返回最后退订完毕的响应，此时剩余频道数一定为 0
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<RespArray>>(unsub_reply));
    auto unsub_arr = std::get<std::shared_ptr<RespArray>>(unsub_reply);
    EXPECT_EQ(unsub_arr->values.size(), 3);
    EXPECT_EQ(std::get<RespInteger>(unsub_arr->values[2]).value, 0); 
}
