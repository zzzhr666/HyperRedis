#include <charconv>
#include <memory>
#include <cassert>

#include "hyper/storage/object.hpp"
#include "hyper/datastructures/intset.hpp"
#include "hyper/datastructures/ziplist.hpp"
#include "hyper/datastructures/dict.hpp"
#include "hyper/datastructures/linked_list.hpp"
#include "hyper/datastructures/skip_list.hpp"


namespace hyper {
    struct RedisDict {
        dict<std::string, std::shared_ptr<RedisObject>, transparentStringHash, transparentStringEqual> storage;
    };


    struct RedisList {
        list<std::shared_ptr<RedisObject>> storage;
    };

    struct RedisSet {
        dict<std::string, std::monostate, transparentStringHash, transparentStringEqual> storage;
    };

    struct RedisZSet {
        skipList<double, std::string> order;
        dict<std::string, double, transparentStringHash, transparentStringEqual> score;
    };
}


hyper::RedisObject::RedisObject(ObjectType type, ObjectEncoding encoding, ObjectData data, Token token)
    : type_(type), encoding_(encoding), data_(std::move(data)) {}

hyper::RedisObject::~RedisObject() = default;

std::unique_ptr<hyper::RedisObject> hyper::RedisObject::createStringObject(std::string_view val) {
    if (val.empty() ||
        (val.size() > 1 && val[0] == '0') ||
        (val.size() >= 2 && val[0] == '-' && val[1] == '0')) {
        return std::make_unique<RedisObject>(ObjectType::String,
                                             ObjectEncoding::Raw, std::string(val), Token{});
    }

    long value{};
    auto [ptr, ec] = std::from_chars(val.data(), val.data() + val.size(), value);
    if (ptr == val.data() + val.size() && ec == std::errc()) {
        return createLongObject(value);
    }

    return std::make_unique<RedisObject>(ObjectType::String,
                                         ObjectEncoding::Raw, std::string(val), Token{});
}

std::unique_ptr<hyper::RedisObject> hyper::RedisObject::createLongObject(long val) {
    return std::make_unique<RedisObject>(ObjectType::String, ObjectEncoding::Int, val, Token{});
}

std::unique_ptr<hyper::RedisObject> hyper::RedisObject::createHashObject() {
    return std::make_unique<RedisObject>(ObjectType::Hash, ObjectEncoding::HashTable,
                                         ObjectData(std::make_unique<RedisDict>()), Token{});
}

std::unique_ptr<hyper::RedisObject> hyper::RedisObject::createListObject() {
    return std::make_unique<RedisObject>(ObjectType::List, ObjectEncoding::ZipList,
                                         ObjectData(std::make_unique<RedisZiplist>()), Token{});
}

std::unique_ptr<hyper::RedisObject> hyper::RedisObject::createSetObject() {
    return std::make_unique<RedisObject>(ObjectType::Set, ObjectEncoding::IntSet,
                                         ObjectData(std::make_unique<RedisIntset>()), Token{});
}

std::unique_ptr<hyper::RedisObject> hyper::RedisObject::createZSetObject() {
    return std::make_unique<RedisObject>(ObjectType::ZSet, ObjectEncoding::ZipList,
                                         ObjectData(std::make_unique<RedisZiplist>()), Token{});
}

std::string hyper::RedisObject::asString() const {
    assert(type_ == ObjectType::String);
    if (encoding_ == ObjectEncoding::Int) {
        return std::to_string(std::get<long>(data_));
    }
    return std::get<std::string>(data_);
}

void hyper::RedisObject::append(std::string_view str) {
    assert(type_ == ObjectType::String);
    if (encoding_ == ObjectEncoding::Int) {
        data_ = std::to_string(std::get<long>(data_));
        encoding_ = ObjectEncoding::Raw;
    }
    std::get<std::string>(data_).append(str);
}

bool hyper::RedisObject::hashSet(std::string field, std::shared_ptr<RedisObject> value) {
    assert(type_ == ObjectType::Hash);
    assert(value);
    auto& hash_table = std::get<std::unique_ptr<RedisDict>>(data_);
    return hash_table->storage.insertOrAssign(std::move(field), std::move(value));
}

std::shared_ptr<hyper::RedisObject> hyper::RedisObject::hashGet(std::string_view field) const {
    assert(type_ == ObjectType::Hash);
    auto& hash_table = std::get<std::unique_ptr<RedisDict>>(data_);
    if (auto res = hash_table->storage.get(field)) {
        return *res;
    }
    return nullptr;
}

bool hyper::RedisObject::hashRemove(std::string_view field) {
    assert(type_ == ObjectType::Hash);
    return std::get<std::unique_ptr<RedisDict>>(data_)->storage.erase(field);
}

std::size_t hyper::RedisObject::hashSize() const {
    assert(type_ == ObjectType::Hash);
    return std::get<std::unique_ptr<RedisDict>>(data_)->storage.size();
}

bool hyper::RedisObject::hashContains(std::string_view field) const {
    assert(type_ == ObjectType::Hash);
    return std::get<std::unique_ptr<RedisDict>>(data_)->storage.contains(field);
}

void hyper::RedisObject::hashForEach(
    const std::function<void(std::string_view, const std::shared_ptr<RedisObject>&)>& func) const {
    assert(type_ == ObjectType::Hash);
    std::get<std::unique_ptr<RedisDict>>(data_)->storage.forEach(
        [&func](const std::string& field, const std::shared_ptr<RedisObject>& value) {
            func(field, value);
        });
}


void hyper::RedisObject::listLeftPush(const std::shared_ptr<RedisObject>& value) {
    assert(type_ == ObjectType::List);
    assert(value);
    assert(value->getType() == ObjectType::String);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& zip_list = std::get<std::unique_ptr<RedisZiplist>>(data_);
        auto string_value = value->asString();
        if (shouldConvertZiplistToList_(string_value, zip_list->size() + 1)) {
            convertZiplistToList_();
            auto& linked_list = std::get<std::unique_ptr<RedisList>>(data_);
            linked_list->storage.pushFront(value);
        } else {
            zip_list->pushFront(string_value);
        }
    } else if (encoding_ == ObjectEncoding::LinkedList) {
        auto& linked_list = std::get<std::unique_ptr<RedisList>>(data_);
        linked_list->storage.pushFront(value);
    }
}

void hyper::RedisObject::listRightPush(const std::shared_ptr<RedisObject>& value) {
    assert(type_ == ObjectType::List);
    assert(value);
    assert(value->getType() == ObjectType::String);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& zip_list = std::get<std::unique_ptr<RedisZiplist>>(data_);
        auto string_value = value->asString();
        if (shouldConvertZiplistToList_(string_value, zip_list->size() + 1)) {
            convertZiplistToList_();
            auto& linked_list = std::get<std::unique_ptr<RedisList>>(data_);
            linked_list->storage.pushBack(value);
        } else {
            zip_list->pushBack(string_value);
        }
    } else if (encoding_ == ObjectEncoding::LinkedList) {
        auto& linked_list = std::get<std::unique_ptr<RedisList>>(data_);
        linked_list->storage.pushBack(value);
    }
}

std::shared_ptr<hyper::RedisObject> hyper::RedisObject::listLeftPop() {
    assert(type_ == ObjectType::List);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& zip_list = std::get<std::unique_ptr<RedisZiplist>>(data_);
        auto left = zip_list->popFront();
        if (left.has_value()) {
            return createStringObject(left.value());
        }
        return nullptr;
    }
    if (encoding_ == ObjectEncoding::LinkedList) {
        auto& linked_list = std::get<std::unique_ptr<RedisList>>(data_);
        if (linked_list->storage.empty()) {
            return nullptr;
        }
        auto left = std::move(linked_list->storage.front());
        linked_list->storage.popFront();
        return left;
    }
    assert(false);
    return nullptr;
}

std::shared_ptr<hyper::RedisObject> hyper::RedisObject::listRightPop() {
    assert(type_ == ObjectType::List);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& zip_list = std::get<std::unique_ptr<RedisZiplist>>(data_);
        auto left = zip_list->popBack();
        if (left.has_value()) {
            return createStringObject(left.value());
        }
        return nullptr;
    }
    if (encoding_ == ObjectEncoding::LinkedList) {
        auto& linked_list = std::get<std::unique_ptr<RedisList>>(data_);
        if (linked_list->storage.empty()) {
            return nullptr;
        }
        auto left = std::move(linked_list->storage.back());
        linked_list->storage.popBack();
        return left;
    }
    assert(false);
    return nullptr;
}

bool hyper::RedisObject::setAdd(std::string_view member) {
    assert(type_ == ObjectType::Set);
    if (encoding_ == ObjectEncoding::IntSet) {
        auto& intset = std::get<std::unique_ptr<RedisIntset>>(data_);
        auto parse_res = parseSetIntegerMember_(member);
        if (parse_res.has_value()) {
            auto inserted = intset->insert(parse_res.value());
            if (inserted && intset->size() > SetMaxIntSetEntries) {
                convertIntSetToSet_();
            }
            return inserted;
        }
        convertIntSetToSet_();
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        auto& set = std::get<std::unique_ptr<RedisSet>>(data_)->storage;
        return set.insert(std::string(member), {});
    }
    assert(false);
    return false;
}

bool hyper::RedisObject::setRemove(std::string_view member) {
    assert(type_ == ObjectType::Set);
    if (encoding_ == ObjectEncoding::IntSet) {
        auto parse = parseSetIntegerMember_(member);
        if (!parse.has_value()) {
            return false;
        }
        return std::get<std::unique_ptr<RedisIntset>>(data_)->erase(parse.value());
    }

    if (encoding_ == ObjectEncoding::HashTable) {
        return std::get<std::unique_ptr<RedisSet>>(data_)->storage.erase(member);
    }

    assert(false);
    return false;
}

bool hyper::RedisObject::setContains(std::string_view member) const {
    assert(type_ == ObjectType::Set);
    if (encoding_ == ObjectEncoding::IntSet) {
        auto parse_res = parseSetIntegerMember_(member);
        if (parse_res.has_value()) {
            return std::get<std::unique_ptr<RedisIntset>>(data_)->contains(parse_res.value());
        }

        return false;
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        return std::get<std::unique_ptr<RedisSet>>(data_)->storage.contains(member);
    }

    assert(false);
    return false;
}

std::size_t hyper::RedisObject::setSize() const {
    assert(type_ == ObjectType::Set);
    if (encoding_ == ObjectEncoding::IntSet) {
        return std::get<std::unique_ptr<RedisIntset>>(data_)->size();
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        return std::get<std::unique_ptr<RedisSet>>(data_)->storage.size();
    }

    assert(false);
    return 0;
}

void hyper::RedisObject::setForEach(const std::function<void(std::string_view)>& func) const {
    assert(type_ == ObjectType::Set);
    if (encoding_ == ObjectEncoding::IntSet) {
        std::get<std::unique_ptr<RedisIntset>>(data_)->forEach([&func](std::int64_t value) {
            auto member = std::to_string(value);
            func(member);
        });
        return;
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        std::get<std::unique_ptr<RedisSet>>(data_)->storage.forEach(
            [&func](const std::string& member, const std::monostate&) {
                func(member);
            });
        return;
    }
    assert(false);
}

bool hyper::RedisObject::zSetAdd(std::string member, double score) {
    assert(type_ == ObjectType::ZSet);

    if (encoding_ == ObjectEncoding::SkipList) {
        auto& z_set = std::get<std::unique_ptr<RedisZSet>>(data_);
        if (auto old_score = z_set->score.get(member)) {
            if (*old_score == score) {
                return false;
            }
            z_set->order.erase(*old_score, member);
            z_set->order.insert(score, member);
            z_set->score.insertOrAssign(std::move(member), score);
            return false;
        }

        z_set->order.insert(score, member);
        z_set->score.insert(std::move(member), score);
        return true;
    }

    if (encoding_ == ObjectEncoding::ZipList) {
        if (member.size() > ZipListMaxValue) {
            convertZSetToSkiplist_();
            return zSetAdd(std::move(member), score);
        }

        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        bool is_update = false;

        auto idx = ziplist->find(member);
        if (idx.has_value() && idx.value() % 2 == 0) {
            auto m_idx = idx.value();
            auto s_idx = m_idx + 1;

            double old_score = parseScore_(ziplist->at(s_idx).value());
            if (old_score == score) {
                return false;
            }

            ziplist->erase(m_idx);
            ziplist->erase(m_idx);
            is_update = true;
        }

        std::size_t pos = 0;
        const auto size = ziplist->size();

        while (pos < size) {
            double current_score = parseScore_(ziplist->at(pos + 1).value());

            if (current_score < score) {
                pos += 2;
            } else if (current_score == score) {
                if (std::string current_member = getEntryAsString_(ziplist->at(pos).value()); current_member < member) {
                    pos += 2;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        ziplist->insert(pos, formatScore_(score));
        ziplist->insert(pos, std::move(member));

        if (ziplist->size() / 2 > ZSetZipListMaxEntries) {
            convertZSetToSkiplist_();
        }

        return !is_update;
    }

    assert(false);
    return false;
}

bool hyper::RedisObject::shouldConvertZiplistToList_(std::string_view value, std::size_t next_size) noexcept {
    return next_size > ZipListMaxEntries || value.size() > ZipListMaxValue;
}

void hyper::RedisObject::convertZiplistToList_() {
    assert(type_ == ObjectType::List && encoding_ == ObjectEncoding::ZipList);
    auto new_list = std::make_unique<RedisList>();
    auto current_list = std::move(std::get<std::unique_ptr<RedisZiplist>>(data_));
    auto& new_list_storage = new_list->storage;
    current_list->forEach([&new_list_storage](const ziplistEntryView& value) {
        if (value.isInteger()) {
            new_list_storage.pushBack(createLongObject(value.integer()));
        } else if (value.isString()) {
            new_list_storage.pushBack(createStringObject(value.string()));
        }
    });
    encoding_ = ObjectEncoding::LinkedList;
    data_ = std::move(new_list);
}

void hyper::RedisObject::convertIntSetToSet_() {
    assert(type_ == ObjectType::Set && encoding_ == ObjectEncoding::IntSet);
    auto new_set = std::make_unique<RedisSet>();
    auto intset = std::move(std::get<std::unique_ptr<RedisIntset>>(data_));
    intset->forEach([&new_set](std::int64_t value) {
        new_set->storage.insert(std::to_string(value), std::monostate{});
    });
    encoding_ = ObjectEncoding::HashTable;
    data_ = std::move(new_set);
}


std::optional<long> hyper::RedisObject::parseSetIntegerMember_(std::string_view member) {
    if (member.empty()) {
        return std::nullopt;
    }

    if (member.size() > 1) {
        if (member[0] == '0' || (member[0] == '-' && member[1] == '0')) {
            return std::nullopt;
        }
    }
    long value{};
    auto [ptr, ec] = std::from_chars(member.data(), member.data() + member.size(), value);
    if (ptr == member.data() + member.size() && ec == std::errc()) {
        return value;
    }
    return std::nullopt;
}

void hyper::RedisObject::convertZSetToSkiplist_() {
    // TODO: Implement tomorrow
    assert(false && "Not implemented yet");
}

double hyper::RedisObject::parseScore_(const ziplistEntryView& entry) {
    // TODO: Implement tomorrow
    assert(false && "Not implemented yet");
    return 0.0;
}

std::string hyper::RedisObject::getEntryAsString_(const ziplistEntryView& entry) {
    // TODO: Implement tomorrow
    assert(false && "Not implemented yet");
    return "";
}

std::string hyper::RedisObject::formatScore_(double score) {
    // TODO: Implement tomorrow
    assert(false && "Not implemented yet");
    return "";
}
