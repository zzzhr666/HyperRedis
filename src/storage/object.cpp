#include <charconv>
#include <memory>
#include <cassert>

#include "hyper/storage/object.hpp"
#include "hyper/datastructures/intset.hpp"
#include "hyper/datastructures/ziplist.hpp"
#include "hyper/datastructures/dict.hpp"
#include "hyper/datastructures/linked_list.hpp"


namespace hyper {
    struct RedisDict {
        dict<std::string, std::shared_ptr<RedisObject>> storage;
    };


    struct RedisList {
        list<std::shared_ptr<RedisObject>> storage;
    };

    struct RedisSkipList {};
}


hyper::RedisObject::~RedisObject() = default;

std::unique_ptr<hyper::RedisObject> hyper::RedisObject::createStringObject(std::string_view val) {
    if (val.empty() ||
        (val.size() > 1 && val[0] == '0') ||
        (val.size() >= 2 && val[0] == '-' && val[1] == '0')) {
        return std::make_unique<RedisObject>(ObjectType::String,
                                             ObjectEncoding::Raw, std::string(val));
    }

    long value{};
    auto [ptr, ec] = std::from_chars(val.data(), val.data() + val.size(), value);
    if (ptr == val.data() + val.size() && ec == std::errc()) {
        return createLongObject(value);
    }

    return std::make_unique<RedisObject>(ObjectType::String,
                                         ObjectEncoding::Raw, std::string(val));
}

std::unique_ptr<hyper::RedisObject> hyper::RedisObject::createLongObject(long val) {
    return std::make_unique<RedisObject>(ObjectType::String, ObjectEncoding::Int, val);
}

std::unique_ptr<hyper::RedisObject> hyper::RedisObject::createHashObject() {
    return std::make_unique<RedisObject>(ObjectType::Hash, ObjectEncoding::HashTable,
                                         ObjectData(std::make_unique<RedisDict>()));
}

std::unique_ptr<hyper::RedisObject> hyper::RedisObject::createListObject() {
    return std::make_unique<RedisObject>(ObjectType::List, ObjectEncoding::ZipList,
                                         ObjectData(std::make_unique<RedisZiplist>()));
}

std::string hyper::RedisObject::asString() const {
    assert(type_ == ObjectType::String);
    if (encoding_ == ObjectEncoding::Int) {
        return std::to_string(std::get<long>(data_));
    }
    return std::get<std::string>(data_);
}

void hyper::RedisObject::append(std::string_view str) {
    if (type_ != ObjectType::String) {
        return;
    }
    if (encoding_ == ObjectEncoding::Int) {
        data_ = std::to_string(std::get<long>(data_));
        encoding_ = ObjectEncoding::Raw;
    }
    std::get<std::string>(data_).append(str);
}

bool hyper::RedisObject::hashSet(std::string field, std::shared_ptr<RedisObject> value) {
    assert(type_ == ObjectType::Hash);
    auto& hash_table = std::get<std::unique_ptr<RedisDict>>(data_);
    return hash_table->storage.insertOrAssign(std::move(field), std::move(value));
}

std::shared_ptr<hyper::RedisObject> hyper::RedisObject::hashGet(const std::string& filed) const {
    assert(type_ == ObjectType::Hash);
    auto& hash_table = std::get<std::unique_ptr<RedisDict>>(data_);
    if (auto res = hash_table->storage.get(filed)) {
        return *res;
    }
    return nullptr;
}

void hyper::RedisObject::listLeftPush(const std::shared_ptr<RedisObject>& value) {
    assert(type_ == ObjectType::List);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& zip_list = std::get<std::unique_ptr<RedisZiplist>>(data_);
        zip_list->pushFront(std::move(value->asString()));
        if (zip_list->size() > ZipListMaxEntries) {
            convertZiplistToList_();
        }
    } else if (encoding_ == ObjectEncoding::LinkedList) {
        auto& linked_list = std::get<std::unique_ptr<RedisList>>(data_);
        linked_list->storage.pushFront(value);
    }
}

void hyper::RedisObject::listRightPush(const std::shared_ptr<RedisObject>& value) {
    assert(type_ == ObjectType::List);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& zip_list = std::get<std::unique_ptr<RedisZiplist>>(data_);
        zip_list->pushBack(std::move(value->asString()));
        if (zip_list->size() > ZipListMaxEntries) {
            convertZiplistToList_();
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


hyper::RedisObject::RedisObject(ObjectType type, ObjectEncoding encoding, ObjectData data, Token token)
    : type_(type), encoding_(encoding), data_(std::move(data)) {}
