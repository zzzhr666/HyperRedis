#pragma once
#include <cstddef>
#include <string>
#include <unordered_set>


namespace hyper {
    class RedisManager;
    class RedisDb;
    class ClientSession;

    class RedisClientContext {
    public:
        RedisClientContext() : db_index_(0) {}

        [[nodiscard]] std::size_t dbIndex() const noexcept {
            return db_index_;
        }

        bool selectDb(const RedisManager& manager, std::size_t index) noexcept;

        [[nodiscard]] RedisDb* currentDb(RedisManager& manager) const noexcept;

        [[nodiscard]] const RedisDb* currentDb(const RedisManager& manager) const noexcept;

        bool addPubSubChannel(const std::string& channel);

        bool removePubSubChannel(const std::string& channel);

        void clearChannels() {
            sub_channels_.clear();
        }

        [[nodiscard]] const std::unordered_set<std::string>& pubsubChannels() const noexcept {
            return sub_channels_;
        }

        void setSession(ClientSession* session) noexcept {
            session_ = session;
        }

        [[nodiscard]] ClientSession* session() const noexcept {
            return session_;
        }

    private:
        std::size_t db_index_;
        std::unordered_set<std::string> sub_channels_;
        ClientSession* session_{nullptr};
    };
}
