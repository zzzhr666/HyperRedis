#include "hyper/storage/aof_rewriter.hpp"

#include <array>
#include <fstream>
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
        const std::array<std::string_view, 2> args{"SELECT", index};
        return hyper::serializeRespCommand(args);
    }

    void appendSelectIfNeeded(std::string& command, std::size_t& selected_db, std::size_t db_index) {
        if (selected_db != db_index) {
            command.append(serializeSelectCommand(db_index));
        }
        selected_db = db_index;
    }
}

hyper::AofRewriter::AofRewriter(std::filesystem::path path) : path_(std::move(path)) {}

bool hyper::AofRewriter::rewrite(RedisManager& manager, ExpireTimePoint now) const {
    std::ofstream out(path_, std::ios::binary | std::ios::trunc);
    if (!out) {
        return false;
    }
    std::size_t selected_db = 0;
    bool ok = true;
    for (std::size_t db_index = 0; db_index < manager.dbCount(); ++db_index) {
        auto db = manager.db(db_index);
        db->forEach(now, [&selected_db, &out, &ok, db_index](std::string_view key, const RedisObjectPtr& value) {
            if (!ok) {
                return;
            }


            if (value->getType() == ObjectType::String) {
                std::string command{};
                appendSelectIfNeeded(command, selected_db, db_index);
                std::string value_str = value->asString();
                std::array<std::string_view, 3> args{{"SET", key, value_str}};
                command.append(serializeRespCommand(args));
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
                    std::array<std::string_view, 4> args{{"HSET", key, filed, value_str}};
                    command.append(serializeRespCommand(args));
                });
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
