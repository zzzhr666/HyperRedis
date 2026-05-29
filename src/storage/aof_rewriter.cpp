#include "hyper/storage/aof_rewriter.hpp"

#include <array>
#include <fstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "hyper/server/resp_codec.hpp"
#include "hyper/storage/database.hpp"
#include "hyper/storage/object.hpp"
#include "hyper/storage/redis_manager.hpp"


namespace {
    [[nodiscard]] std::string serializeSelectCommand(std::size_t db_index) {
        const std::string index = std::to_string(db_index);
        const std::array<std::string_view,2> args{"SELECT", index};
        return hyper::serializeRespCommand(args);
    }

    void appendSelectIfNeeded(std::string& command, std::size_t& selected_db, std::size_t db_index) {
        if (selected_db != db_index) {
            command.append(serializeSelectCommand(db_index));
        }
        selected_db = db_index;
    }

    void appendExpireIfNeeded(std::string& command, hyper::RedisDb* db, std::string_view key,
                              hyper::ExpireTimePoint now) {
        if (auto deadline = db->expireTime(key, now)) {
            const std::string deadline_str = std::to_string(deadline.value());
            const std::array<std::string_view,3> args{{"PEXPIREAT", key, deadline_str}};
            command.append(hyper::serializeRespCommand(args));
        }
    }
}
bool hyper::AofRewriter::rewrite(const std::filesystem::path& path,RedisManager& manager, ExpireTimePoint now) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        return false;
    }
    std::size_t selected_db = 0;
    bool ok = true;
    for (std::size_t db_index = 0; db_index < manager.dbCount(); ++db_index) {
        auto db = manager.db(db_index);
        db->forEach(now, [&selected_db, &out, &ok,db,now, db_index](std::string_view key, const RedisObjectPtr& value) {
            if (!ok) {
                return;
            }


            if (value->getType() == ObjectType::String) {
                std::string command{};
                appendSelectIfNeeded(command, selected_db, db_index);
                std::string value_str = value->asString();
                std::array<std::string_view,3> args{{"SET", key, value_str}};
                command.append(serializeRespCommand(args));
                appendExpireIfNeeded(command, db, key, now);
                out.write(command.data(), static_cast<std::streamsize>(command.size()));
                if (!out) {
                    ok = false;
                }
            } else if (value->getType() == ObjectType::List) {
                auto list_data = value->listRangeAsStrings(0, -1);
                if (list_data.empty()) {
                    return;
                }
                std::string command{};
                appendSelectIfNeeded(command, selected_db, db_index);
                std::vector<std::string_view> args(list_data.begin(), list_data.end());
                args.insert(args.begin(), key);
                args.insert(args.begin(), "RPUSH");
                command.append(serializeRespCommand(args));
                appendExpireIfNeeded(command, db, key, now);
                out.write(command.data(), static_cast<std::streamsize>(command.size()));
                if (!out) {
                    ok = false;
                }
            } else if (value->getType() == ObjectType::Hash) {
                if (value->hashSize() == 0) {
                    return;
                }
                std::string command{};
                appendSelectIfNeeded(command, selected_db, db_index);
                value->hashForEach([&key,&command](std::string_view filed, const RedisObjectPtr& v) {
                    std::string value_str = v->asString();
                    std::array<std::string_view,4> args{{"HSET", key, filed, value_str}};
                    command.append(serializeRespCommand(args));
                });
                appendExpireIfNeeded(command, db, key, now);
                out.write(command.data(), static_cast<std::streamsize>(command.size()));
                if (!out) {
                    ok = false;
                }
            } else if (value->getType() == ObjectType::Set) {
                if (value->setSize() == 0) {
                    return;
                }
                std::string command{};
                appendSelectIfNeeded(command, selected_db, db_index);
                std::vector<std::string> members;
                value->setForEach([&members](std::string_view member) {
                    members.emplace_back(member);
                });
                std::vector<std::string_view> args(members.begin(), members.end());
                args.reserve(members.size() + 2);
                args.insert(args.begin(), key);
                args.insert(args.begin(), "SADD");
                command.append(serializeRespCommand(args));
                appendExpireIfNeeded(command, db, key, now);
                out.write(command.data(), static_cast<std::streamsize>(command.size()));
                if (!out) {
                    ok = false;
                }
            } else if (value->getType() == ObjectType::ZSet) {
                if (value->zSetSize() == 0) {
                    return;
                }
                std::string command{};
                appendSelectIfNeeded(command, selected_db, db_index);
                auto zset_data = value->zSetRange(0, -1);
                std::vector<std::string> args;
                args.reserve(2 + 2 * zset_data.size());
                args.emplace_back("ZADD");
                args.emplace_back(key);
                for (auto& [member,score] : zset_data) {
                    args.push_back(std::to_string(score));
                    args.push_back(std::move(member));
                }
                std::vector<std::string_view> args_span{args.begin(), args.end()};
                command.append(serializeRespCommand(args_span));
                appendExpireIfNeeded(command, db, key, now);
                out.write(command.data(), static_cast<std::streamsize>(command.size()));
                if (!out) {
                    ok = false;
                }
            }
        });
    }
    out.close();
    return ok && out.good();
}


