#include <array>
#include <cassert>
#include <charconv>
#include <cmath>
#include <limits>
#include <memory>
#include <random>

#include "hyper/storage/object.hpp"

#include <ranges>

#include "hyper/datastructures/dict.hpp"
#include "hyper/datastructures/intset.hpp"
#include "hyper/datastructures/linked_list.hpp"
#include "hyper/datastructures/skip_list.hpp"
#include "hyper/datastructures/ziplist.hpp"


namespace hyper {
    struct RedisDict {
        dict<std::string, RedisObjectPtr, transparentStringHash, transparentStringEqual> storage;
    };


    struct RedisList {
        list<RedisObjectPtr> storage;
    };

    struct RedisSet {
        dict<std::string, std::monostate, transparentStringHash, transparentStringEqual> storage;
    };

    struct RedisZSet {
        skipList<double, std::string> order;
        dict<std::string, double, transparentStringHash, transparentStringEqual> score;
    };

    namespace {
        [[nodiscard]] bool checkedAddLong(long lhs, long rhs, long& out) noexcept {
            if ((rhs > 0 && lhs > std::numeric_limits<long>::max() - rhs) ||
                (rhs < 0 && lhs < std::numeric_limits<long>::min() - rhs)) {
                return false;
            }
            out = lhs + rhs;
            return true;
        }

        [[nodiscard]] bool isInvalidScore(double score) noexcept {
            return std::isnan(score);
        }

    }
}


hyper::RedisObject::RedisObject(ObjectType type, ObjectEncoding encoding, ObjectData data, Token token)
    : type_(type), encoding_(encoding), data_(std::move(data)) {}

hyper::RedisObject::~RedisObject() = default;

hyper::RedisObjectUPtr hyper::RedisObject::createUniqueStringObject(std::string_view val) {
    if (val.empty() ||
        (val.size() > 1 && val[0] == '0') ||
        (val.size() >= 2 && val[0] == '-' && val[1] == '0')) {
        return std::make_unique<RedisObject>(ObjectType::String,
                                             ObjectEncoding::Raw, std::string(val), Token{});
    }

    long value{};
    auto [ptr, ec] = std::from_chars(val.data(), val.data() + val.size(), value);
    if (ptr == val.data() + val.size() && ec == std::errc()) {
        return createUniqueLongObject(value);
    }

    return std::make_unique<RedisObject>(ObjectType::String,
                                         ObjectEncoding::Raw, std::string(val), Token{});
}

hyper::RedisObjectUPtr hyper::RedisObject::createUniqueLongObject(long val) {
    return std::make_unique<RedisObject>(ObjectType::String, ObjectEncoding::Int, val, Token{});
}

hyper::RedisObjectUPtr hyper::RedisObject::createUniqueHashObject() {
    return std::make_unique<RedisObject>(ObjectType::Hash, ObjectEncoding::ZipList,
                                         ObjectData(std::make_unique<RedisZiplist>()), Token{});
}

hyper::RedisObjectUPtr hyper::RedisObject::createUniqueListObject() {
    return std::make_unique<RedisObject>(ObjectType::List, ObjectEncoding::ZipList,
                                         ObjectData(std::make_unique<RedisZiplist>()), Token{});
}

hyper::RedisObjectUPtr hyper::RedisObject::createUniqueSetObject() {
    return std::make_unique<RedisObject>(ObjectType::Set, ObjectEncoding::IntSet,
                                         ObjectData(std::make_unique<RedisIntset>()), Token{});
}

hyper::RedisObjectUPtr hyper::RedisObject::createUniqueZSetObject() {
    return std::make_unique<RedisObject>(ObjectType::ZSet, ObjectEncoding::ZipList,
                                         ObjectData(std::make_unique<RedisZiplist>()), Token{});
}

hyper::RedisObjectPtr hyper::RedisObject::createSharedStringObject(std::string_view val) {
    if (val.empty() ||
        (val.size() > 1 && val[0] == '0') ||
        (val.size() >= 2 && val[0] == '-' && val[1] == '0')) {
        return std::make_shared<RedisObject>(ObjectType::String,
                                             ObjectEncoding::Raw, std::string(val), Token{});
    }

    long value{};
    auto [ptr, ec] = std::from_chars(val.data(), val.data() + val.size(), value);
    if (ptr == val.data() + val.size() && ec == std::errc()) {
        return createSharedLongObject(value);
    }

    return std::make_shared<RedisObject>(ObjectType::String,
                                         ObjectEncoding::Raw, std::string(val), Token{});
}

hyper::RedisObjectPtr hyper::RedisObject::createSharedLongObject(long val) {
    return std::make_shared<RedisObject>(ObjectType::String, ObjectEncoding::Int, val, Token{});
}

hyper::RedisObjectPtr hyper::RedisObject::createSharedHashObject() {
    return std::make_shared<RedisObject>(ObjectType::Hash, ObjectEncoding::ZipList,
                                         ObjectData(std::make_unique<RedisZiplist>()), Token{});
}

hyper::RedisObjectPtr hyper::RedisObject::createSharedListObject() {
    return std::make_shared<RedisObject>(ObjectType::List, ObjectEncoding::ZipList,
                                         ObjectData(std::make_unique<RedisZiplist>()), Token{});
}

hyper::RedisObjectPtr hyper::RedisObject::createSharedSetObject() {
    return std::make_shared<RedisObject>(ObjectType::Set, ObjectEncoding::IntSet,
                                         ObjectData(std::make_unique<RedisIntset>()), Token{});
}

hyper::RedisObjectPtr hyper::RedisObject::createSharedZSetObject() {
    return std::make_shared<RedisObject>(ObjectType::ZSet, ObjectEncoding::ZipList,
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

std::size_t hyper::RedisObject::stringLen() const {
    assert(type_ == ObjectType::String);
    if (encoding_ == ObjectEncoding::Int) {
        return std::to_string(std::get<long>(data_)).size();
    }
    if (encoding_ == ObjectEncoding::Raw) {
        return std::get<std::string>(data_).size();
    }
    assert(false);
    return 0;
}

std::optional<long> hyper::RedisObject::stringIncrBy(long increment) {
    assert(type_ == ObjectType::String);
    if (encoding_ == ObjectEncoding::Int) {
        auto& res = std::get<long>(data_);
        long updated{};
        if (!checkedAddLong(res, increment, updated)) {
            return std::nullopt;
        }
        res = updated;
        return res;
    }
    if (encoding_ == ObjectEncoding::Raw) {
        long value{};
        auto& origin = std::get<std::string>(data_);
        auto [ptr,ec] = std::from_chars(origin.data(), origin.data() + origin.size(), value);
        if (ptr == origin.data() + origin.size() && ec == std::errc()) {
            long updated{};
            if (!checkedAddLong(value, increment, updated)) {
                return std::nullopt;
            }
            encoding_ = ObjectEncoding::Int;
            data_ = updated;
            return updated;
        }
        return std::nullopt;
    }
    assert(false);
    return std::nullopt;
}

std::optional<double> hyper::RedisObject::stringIncrByFloat(double increment) {
    assert(type_ == ObjectType::String);
    if (isInvalidScore(increment)) {
        return std::nullopt;
    }
    double value{};
    if (encoding_ == ObjectEncoding::Int) {
        value = static_cast<double>(std::get<long>(data_));
    } else if (encoding_ == ObjectEncoding::Raw) {
        auto& str = std::get<std::string>(data_);
        auto [ptr,ec] = std::from_chars(str.data(), str.data() + str.size(), value);
        if (ptr != str.data() + str.size() || ec != std::errc()) {
            return std::nullopt;
        }
    }
    value += increment;
    if (isInvalidScore(value)) {
        return std::nullopt;
    }
    auto fmt_str = formatScore_(value);
    encoding_ = ObjectEncoding::Raw;
    data_ = std::move(fmt_str);
    return value;
}

std::string hyper::RedisObject::stringGetRange(int start, int end) const {
    assert(type_ == ObjectType::String);
    std::string str = asString();
    std::size_t len = str.size();
    if (str.empty()) {
        return {};
    }
    if (start < 0) {
        start += static_cast<int>(len);
    }
    if (end < 0) {
        end += static_cast<int>(len);
    }
    if (start < 0) {
        start = 0;
    }
    if (end < 0) {
        end = 0;
    }

    if (end >= len) {
        end = static_cast<int>(len) - 1;
    }
    if (start > end) {
        return {};
    }
    return str.substr(start, end - start + 1);
}

void hyper::RedisObject::stringSetRange(std::size_t offset, std::string_view value) {
    assert(type_ ==ObjectType::String);
    if (encoding_ == ObjectEncoding::Int) {
        data_ = std::to_string(std::get<long>(data_));
        encoding_ = ObjectEncoding::Raw;
    }
    auto& s = std::get<std::string>(data_);
    if (offset + value.size() > s.size()) {
        s.resize(offset + value.size());
    }
    if (!value.empty()) {
        std::ranges::copy(value, s.begin() + static_cast<int>(offset));
    }
}

bool hyper::RedisObject::hashSet(std::string field, RedisObjectPtr value) {
    assert(type_ == ObjectType::Hash);
    assert(value);
    auto value_str = value->asString();
    if (encoding_ == ObjectEncoding::ZipList) {
        if (field.size() > ZipListMaxValue || value_str.size() > ZipListMaxValue) {
            convertHashZipListToDict_();
            return hashSet(std::move(field), std::move(value));
        }
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (std::size_t i = 0; i < ziplist->size(); i += 2) {
            if (getEntryAsString_((*ziplist)[i]) == field) {
                ziplist->erase(i + 1);
                ziplist->insert(i + 1, value_str);
                return false;
            }
        }

        ziplist->pushBack(field);
        ziplist->pushBack(value_str);
        if (ziplist->size() / 2 > HashZipListMaxEntries) {
            convertHashZipListToDict_();
        }
        return true;
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        auto& hash_table = std::get<std::unique_ptr<RedisDict>>(data_);
        return hash_table->storage.insertOrAssign(std::move(field), std::move(value));
    }
    assert(false);
    return false;
}

hyper::RedisObjectPtr hyper::RedisObject::hashGet(std::string_view field) const {
    assert(type_ == ObjectType::Hash);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (std::size_t i = 0; i < ziplist->size(); i += 2) {
            if (getEntryAsString_((*ziplist)[i]) == field) {
                return createSharedStringObject(getEntryAsString_((*ziplist)[i + 1]));
            }
        }
        return nullptr;
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        auto& hash_table = std::get<std::unique_ptr<RedisDict>>(data_);
        if (auto res = hash_table->storage.get(field)) {
            return *res;
        }
        return nullptr;
    }
    assert(false);
    return nullptr;
}

bool hyper::RedisObject::hashRemove(std::string_view field) {
    assert(type_ == ObjectType::Hash);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (std::size_t i = 0; i < ziplist->size(); i += 2) {
            if (getEntryAsString_((*ziplist)[i]) == field) {
                ziplist->erase(i);
                ziplist->erase(i);
                return true;
            }
        }
        return false;
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        return std::get<std::unique_ptr<RedisDict>>(data_)->storage.erase(field);
    }

    assert(false);
    return false;
}

std::size_t hyper::RedisObject::hashSize() const {
    assert(type_ == ObjectType::Hash);
    if (encoding_ == ObjectEncoding::ZipList) {
        return std::get<std::unique_ptr<RedisZiplist>>(data_)->size() / 2;
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        return std::get<std::unique_ptr<RedisDict>>(data_)->storage.size();
    }
    assert(false);
    return 0;
}

bool hyper::RedisObject::hashContains(std::string_view field) const {
    assert(type_ == ObjectType::Hash);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (std::size_t i = 0; i < ziplist->size(); i += 2) {
            if (getEntryAsString_((*ziplist)[i]) == field) {
                return true;
            }
        }
        return false;
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        return std::get<std::unique_ptr<RedisDict>>(data_)->storage.contains(field);
    }
    assert(false);
    return false;
}

void hyper::RedisObject::hashForEach(
    const std::function<void(std::string_view, const RedisObjectPtr&)>& func) const {
    assert(type_ == ObjectType::Hash);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        bool is_field = true;
        std::string field;
        ziplist->forEach([&is_field,&func,&field](const ziplistEntryView& entry_view) {
            if (is_field) {
                field = getEntryAsString_(entry_view);
                is_field = !is_field;
                return;
            }
            RedisObjectPtr value = createSharedStringObject(getEntryAsString_(entry_view));
            func(field, value);
            is_field = !is_field;
        });
        return;
    }

    if (encoding_ == ObjectEncoding::HashTable) {
        std::get<std::unique_ptr<RedisDict>>(data_)->storage.forEach(
            [&func](const std::string& field, const RedisObjectPtr& value) {
                func(field, value);
            });
        return;
    }
    assert(false);
}

std::vector<std::string> hyper::RedisObject::hashKeys() const {
    assert(type_ == ObjectType::Hash);
    std::vector<std::string> res{};
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (std::size_t i = 0; i < ziplist->size(); i += 2) {
            res.emplace_back(getEntryAsString_((*ziplist)[i]));
        }
        return res;
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        auto& dict = std::get<std::unique_ptr<RedisDict>>(data_)->storage;
        dict.forEach([&res](const std::string& k, const RedisObjectPtr&) {
            res.emplace_back(k);
        });
        return res;
    }

    assert(false);
    return res;
}

std::vector<hyper::RedisObjectPtr> hyper::RedisObject::hashValues() const {
    assert(type_ == ObjectType::Hash);
    std::vector<RedisObjectPtr> res{};
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (std::size_t i = 1; i < ziplist->size(); i += 2) {
            res.emplace_back(createSharedStringObject(getEntryAsString_((*ziplist)[i])));
        }
        return res;
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        auto& dict = std::get<std::unique_ptr<RedisDict>>(data_)->storage;
        dict.forEach([&res](const std::string&, const RedisObjectPtr& v) {
            res.emplace_back(v);
        });
        return res;
    }

    assert(false);
    return res;
}

std::vector<std::pair<std::string, hyper::RedisObjectPtr>> hyper::RedisObject::hashGetAll() const {
    assert(type_ == ObjectType::Hash);
    std::vector<std::pair<std::string, RedisObjectPtr>> res{};
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (std::size_t i = 0; i < ziplist->size(); i += 2) {
            res.emplace_back(getEntryAsString_((*ziplist)[i]),
                             createSharedStringObject(getEntryAsString_((*ziplist)[i + 1])));
        }
        return res;
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        auto& dict = std::get<std::unique_ptr<RedisDict>>(data_)->storage;
        dict.forEach([&res](const std::string& k, const RedisObjectPtr& v) {
            res.emplace_back(k, v);
        });
        return res;
    }

    assert(false);
    return res;
}

std::vector<std::string> hyper::RedisObject::hashValuesAsStrings() const {
    assert(type_ == ObjectType::Hash);
    std::vector<std::string> res{};
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (std::size_t i = 1; i < ziplist->size(); i += 2) {
            res.emplace_back(getEntryAsString_((*ziplist)[i]));
        }
        return res;
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        auto& dict = std::get<std::unique_ptr<RedisDict>>(data_)->storage;
        dict.forEach([&res](const std::string&, const RedisObjectPtr& v) {
            res.emplace_back(v->asString());
        });
        return res;
    }

    assert(false);
    return res;
}

std::vector<std::pair<std::string, std::string>> hyper::RedisObject::hashGetAllAsStrings() const {
    assert(type_ == ObjectType::Hash);
    std::vector<std::pair<std::string, std::string>> res{};
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (std::size_t i = 0; i < ziplist->size(); i += 2) {
            res.emplace_back(getEntryAsString_((*ziplist)[i]), getEntryAsString_((*ziplist)[i + 1]));
        }
        return res;
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        auto& dict = std::get<std::unique_ptr<RedisDict>>(data_)->storage;
        dict.forEach([&res](const std::string& k, const RedisObjectPtr& v) {
            res.emplace_back(k, v->asString());
        });
        return res;
    }

    assert(false);
    return res;
}


void hyper::RedisObject::listLeftPush(const RedisObjectPtr& value) {
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

void hyper::RedisObject::listRightPush(const RedisObjectPtr& value) {
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

hyper::RedisObjectPtr hyper::RedisObject::listLeftPop() {
    assert(type_ == ObjectType::List);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& zip_list = std::get<std::unique_ptr<RedisZiplist>>(data_);
        auto left = zip_list->popFront();
        if (left.has_value()) {
            return createSharedStringObject(left.value());
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

hyper::RedisObjectPtr hyper::RedisObject::listRightPop() {
    assert(type_ == ObjectType::List);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& zip_list = std::get<std::unique_ptr<RedisZiplist>>(data_);
        auto left = zip_list->popBack();
        if (left.has_value()) {
            return createSharedStringObject(left.value());
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

std::size_t hyper::RedisObject::listLen() const {
    assert(type_ == ObjectType::List);
    if (encoding_ == ObjectEncoding::ZipList) {
        return std::get<std::unique_ptr<RedisZiplist>>(data_)->size();
    }
    if (encoding_ == ObjectEncoding::LinkedList) {
        return std::get<std::unique_ptr<RedisList>>(data_)->storage.size();
    }

    assert(false);
    return 0;
}

hyper::RedisObjectPtr hyper::RedisObject::listIndex(int index) const {
    assert(type_ == ObjectType::List);
    std::size_t len = listLen();
    if (len == 0) {
        return nullptr;
    }
    if (index < 0) {
        index += static_cast<int>(len);
    }
    if (index < 0 || index >= len) {
        return nullptr;
    }

    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        return createSharedStringObject(getEntryAsString_((*ziplist)[index]));
    }
    if (encoding_ == ObjectEncoding::LinkedList) {
        auto& list = std::get<std::unique_ptr<RedisList>>(data_)->storage;
        std::size_t current_index{0};
        for (auto it = list.begin(); it != list.end(); ++it, ++current_index) {
            if (current_index == index) {
                return *it;
            }
        }
    }

    assert(false);
    return nullptr;
}

std::optional<std::string> hyper::RedisObject::listIndexAsString(int index) const {
    assert(type_ == ObjectType::List);
    std::size_t len = listLen();
    if (len == 0) {
        return std::nullopt;
    }
    if (index < 0) {
        index += static_cast<int>(len);
    }
    if (index < 0 || index >= static_cast<int>(len)) {
        return std::nullopt;
    }

    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        return getEntryAsString_((*ziplist)[static_cast<std::size_t>(index)]);
    }
    if (encoding_ == ObjectEncoding::LinkedList) {
        auto& list = std::get<std::unique_ptr<RedisList>>(data_)->storage;
        std::size_t current_index{0};
        for (const auto& item : list) {
            if (current_index++ == static_cast<std::size_t>(index)) {
                return item->asString();
            }
        }
    }

    assert(false);
    return std::nullopt;
}

bool hyper::RedisObject::listSet(int index, const RedisObjectPtr& value) {
    assert(type_ == ObjectType::List);
    std::size_t len = listLen();
    if (index < 0) {
        index += static_cast<int>(len);
    }
    if (index < 0 || index >= len) {
        return false;
    }

    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        auto str = value->asString();
        if (shouldConvertZiplistToList_(str, len)) {
            convertZiplistToList_();
            return listSet(index, value);
        }

        ziplist->erase(index);
        return ziplist->insert(index, str);
    }
    if (encoding_ == ObjectEncoding::LinkedList) {
        auto& list = std::get<std::unique_ptr<RedisList>>(data_)->storage;
        std::size_t current_index{0};
        for (auto& item : list) {
            if (current_index++ == static_cast<std::size_t>(index)) {
                item = value;
                return true;
            }
        }
    }
    assert(false);
    return false;
}

std::optional<std::size_t> hyper::RedisObject::listInsert(std::string_view pivot,
                                                          const RedisObjectPtr& value, bool before) {
    assert(type_ == ObjectType::List);
    auto len = listLen();
    if (encoding_ == ObjectEncoding::ZipList) {
        auto str = value->asString();
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        if (shouldConvertZiplistToList_(str, len + 1)) {
            convertZiplistToList_();
            return listInsert(pivot, value, before);
        }

        auto res_op = ziplist->find(pivot);
        if (!res_op.has_value()) {
            return std::nullopt;
        }
        std::size_t index = res_op.value();
        if (before) {
            ziplist->insert(index, str);
            return len + 1;
        }
        ziplist->insert(index + 1, str);
        return len + 1;
    }

    if (encoding_ == ObjectEncoding::LinkedList) {
        auto& list = std::get<std::unique_ptr<RedisList>>(data_)->storage;
        std::size_t index{0};
        for (auto it = list.begin(); it != list.end(); ++it, ++index) {
            if ((*it)->asString() == pivot) {
                if (before) {
                    list.insertBefore(it.get(), value);
                    return len + 1;
                }
                list.insertAfter(it.get(), value);
                return len + 1;
            }
        }
        return std::nullopt;
    }

    assert(false);
    return std::nullopt;
}

std::size_t hyper::RedisObject::listRemove(int count, std::string_view value) {
    assert(type_ == ObjectType::List);
    std::size_t ret{0};
    auto len = listLen();
    if (count == 0) {
        count = static_cast<int>(len);
    }
    bool from_front = count > 0;
    count = std::abs(count);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        if (from_front) {
            std::size_t index{0};
            while (index < len) {
                auto current = getEntryAsString_((*ziplist)[index]);
                if (current == value) {
                    ziplist->erase(index);
                    --len;
                    if (++ret == count) {
                        return ret;
                    }
                } else {
                    ++index;
                }
            }
            return ret;
        }
        for (int i = static_cast<int>(len) - 1; i >= 0; --i) {
            auto current = getEntryAsString_((*ziplist)[i]);
            if (current != value) {
                continue;;
            }
            if (ret++ < count) {
                ziplist->erase(i);
                if (ret == count) {
                    return ret;
                }
            }
        }
        return ret;
    }

    if (encoding_ == ObjectEncoding::LinkedList) {
        auto& list = std::get<std::unique_ptr<RedisList>>(data_)->storage;
        if (from_front) {
            for (auto it = list.begin(); it != list.end();) {
                if ((*it)->asString() == value) {
                    auto current = it++;
                    list.erase(current.get());
                    if (++ret == count) {
                        return ret;
                    }
                } else {
                    ++it;
                }
            }
            return ret;
        }
        for (auto it = list.tail(); it != list.end();) {
            if ((*it)->asString() == value) {
                auto current = it--;
                list.erase(current.get());
                if (++ret == count) {
                    return ret;
                }
            } else {
                --it;
            }
        }
        return ret;
    }
    assert(false);
    return 0;
}

void hyper::RedisObject::listTrim(int start, int end) {
    assert(type_ == ObjectType::List);
    std::size_t len = listLen();
    if (len == 0) {
        return;
    }
    if (start < 0) {
        start += static_cast<int>(len);
    }
    if (end < 0) {
        end += static_cast<int>(len);
    }
    if (start < 0) {
        start = 0;
    }
    if (start > end || static_cast<std::size_t>(start) >= len) {
        encoding_ = ObjectEncoding::ZipList;
        data_ = std::make_unique<RedisZiplist>();
        return;
    }
    if (static_cast<std::size_t>(end) >= len) {
        end = static_cast<int>(len )- 1;
    }
    std::size_t ltrim = start;
    std::size_t rtrim = len - (end + 1);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        while (ltrim--) {
            ziplist->popFront();
        }
        while (rtrim--) {
            ziplist->popBack();
        }
        return;
    }
    if (encoding_ == ObjectEncoding::LinkedList) {
        auto& list = std::get<std::unique_ptr<RedisList>>(data_)->storage;
        while (ltrim--) {
            list.popFront();
        }
        while (rtrim--) {
            list.popBack();
        }
        return;
    }
    assert(false);
}

std::vector<hyper::RedisObjectPtr> hyper::RedisObject::listRange(int start, int end) const {
    assert(type_ == ObjectType::List);
    std::size_t len = listLen();
    if (start < 0) {
        start += static_cast<int>(len);
    }
    if (end < 0) {
        end += static_cast<int>(len);
    }
    if (start < 0) {
        start = 0;
    }
    if (start >= static_cast<int>(len) || start > end) {
        return {};
    }
    if (end >= static_cast<int>(len)) {
        end = static_cast<int>(len) - 1;
    }
    std::vector<RedisObjectPtr> res{};
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (int i = start; i <= end; ++i) {
            res.emplace_back(createSharedStringObject(getEntryAsString_((*ziplist)[i])));
        }
        return res;
    }

    if (encoding_ == ObjectEncoding::LinkedList) {
        auto& list = std::get<std::unique_ptr<RedisList>>(data_)->storage;
        auto it = list.begin();
        for (int i = 0; i < start; ++i) {
            ++it;
        }
        for (int i = start; i <= end; ++i, ++it) {
            res.emplace_back(*it);
        }
        return res;
    }

    assert(false);
    return res;
}

std::vector<std::string> hyper::RedisObject::listRangeAsStrings(int start, int end) const {
    assert(type_ == ObjectType::List);
    std::size_t len = listLen();
    if (start < 0) {
        start += static_cast<int>(len);
    }
    if (end < 0) {
        end += static_cast<int>(len);
    }
    if (start < 0) {
        start = 0;
    }
    if (start >= static_cast<int>(len) || start > end) {
        return {};
    }
    if (end >= static_cast<int>(len)) {
        end = static_cast<int>(len) - 1;
    }

    std::vector<std::string> res{};
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (int i = start; i <= end; ++i) {
            res.emplace_back(getEntryAsString_((*ziplist)[static_cast<std::size_t>(i)]));
        }
        return res;
    }

    if (encoding_ == ObjectEncoding::LinkedList) {
        auto& list = std::get<std::unique_ptr<RedisList>>(data_)->storage;
        auto it = list.begin();
        for (int i = 0; i < start; ++i) {
            ++it;
        }
        for (int i = start; i <= end; ++i, ++it) {
            res.emplace_back((*it)->asString());
        }
        return res;
    }

    assert(false);
    return res;
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

hyper::RedisObjectPtr hyper::RedisObject::setRandomMember() const {
    assert(type_ == ObjectType::Set);
    auto member = setRandomMemberString();
    if (!member.has_value()) {
        return nullptr;
    }
    return createSharedStringObject(*member);
}

std::optional<std::string> hyper::RedisObject::setRandomMemberString() const {
    assert(type_ == ObjectType::Set);
    std::size_t len = setSize();
    if (len == 0) {
        return std::nullopt;
    }
    if (encoding_ == ObjectEncoding::IntSet) {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        std::uniform_int_distribution<std::size_t> dist(0, len - 1);
        std::size_t index = dist(gen);
        return std::to_string(std::get<std::unique_ptr<RedisIntset>>(data_)->getAt(index));
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        auto& hash_set = std::get<std::unique_ptr<RedisSet>>(data_)->storage;
        if (auto res = hash_set.getRandomKey()) {
            return *res;
        }
        return std::nullopt;
    }
    assert(false);
    return std::nullopt;
}

hyper::RedisObjectPtr hyper::RedisObject::setPop() {
    assert(type_ == ObjectType::Set);
    auto member = setPopString();
    if (!member.has_value()) {
        return nullptr;
    }
    return createSharedStringObject(*member);
}

std::optional<std::string> hyper::RedisObject::setPopString() {
    assert(type_ == ObjectType::Set);
    if (setSize() == 0) {
        return std::nullopt;
    }
    if (encoding_ == ObjectEncoding::IntSet) {
        auto member = setRandomMemberString();
        assert(member.has_value());
        setRemove(*member);
        return member;
    }
    if (encoding_ == ObjectEncoding::HashTable) {
        auto& hash_set = std::get<std::unique_ptr<RedisSet>>(data_)->storage;
        auto rand_k = hash_set.getRandomKey();
        if (rand_k == nullptr) {
            return std::nullopt;
        }
        std::string res = *rand_k;
        hash_set.erase(*rand_k);
        return res;
    }
    assert(false);
    return std::nullopt;
}

bool hyper::RedisObject::zSetAdd(std::string member, double score) {
    return zSetAddDetailed(std::move(member), score) == ZSetAddStatus::Added;
}

hyper::ZSetAddStatus hyper::RedisObject::zSetAddDetailed(std::string member, double score) {
    assert(type_ == ObjectType::ZSet);
    if (isInvalidScore(score)) {
        return ZSetAddStatus::InvalidScore;
    }

    if (encoding_ == ObjectEncoding::SkipList) {
        auto& z_set = std::get<std::unique_ptr<RedisZSet>>(data_);
        if (auto old_score = z_set->score.get(member)) {
            if (*old_score == score) {
                return ZSetAddStatus::Unchanged;
            }
            z_set->order.erase(*old_score, member);
            z_set->order.insert(score, member);
            z_set->score.insertOrAssign(std::move(member), score);
            return ZSetAddStatus::Updated;
        }

        z_set->order.insert(score, member);
        z_set->score.insert(std::move(member), score);
        return ZSetAddStatus::Added;
    }

    if (encoding_ == ObjectEncoding::ZipList) {
        if (member.size() > ZipListMaxValue) {
            convertZSetToSkiplist_();
            return zSetAddDetailed(std::move(member), score);
        }

        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        bool is_update = false;

        std::optional<std::size_t> found_idx;
        for (std::size_t i = 0; i < ziplist->size(); i += 2) {
            if (getEntryAsString_((*ziplist)[i]) == member) {
                found_idx = i;
                break;
            }
        }

        if (found_idx.has_value()) {
            auto m_idx = found_idx.value();
            auto s_idx = m_idx + 1;

            double old_score = parseScore_((*ziplist)[s_idx]);
            if (old_score == score) {
                return ZSetAddStatus::Unchanged;
            }

            ziplist->erase(m_idx);
            ziplist->erase(m_idx);
            is_update = true;
        }

        std::size_t pos = 0;
        const auto size = ziplist->size();

        while (pos < size) {
            double current_score = parseScore_((*ziplist)[pos + 1]);

            if (current_score < score) {
                pos += 2;
            } else if (current_score == score) {
                if (std::string current_member = getEntryAsString_((*ziplist)[pos]); current_member < member) {
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

        return is_update ? ZSetAddStatus::Updated : ZSetAddStatus::Added;
    }

    assert(false);
    return ZSetAddStatus::InvalidScore;
}

std::optional<double> hyper::RedisObject::zSetScore(std::string_view member) const {
    assert(type_ == ObjectType::ZSet);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (std::size_t i = 0; i < ziplist->size(); i += 2) {
            if (getEntryAsString_((*ziplist)[i]) == member) {
                return parseScore_((*ziplist)[i + 1]);
            }
        }
        return std::nullopt;
    }
    if (encoding_ == ObjectEncoding::SkipList) {
        if (double* res = std::get<std::unique_ptr<RedisZSet>>(data_)->score.get(member)) {
            return *res;
        }
        return std::nullopt;
    }
    assert(false);
    return std::nullopt;
}

bool hyper::RedisObject::zSetRemove(std::string_view member) {
    assert(type_ == ObjectType::ZSet);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (std::size_t i = 0; i < ziplist->size(); i += 2) {
            if (getEntryAsString_((*ziplist)[i]) == member) {
                ziplist->erase(i);
                ziplist->erase(i);
                return true;
            }
        }
        return false;
    }
    if (encoding_ == ObjectEncoding::SkipList) {
        auto& z_set = std::get<std::unique_ptr<RedisZSet>>(data_);
        if (auto score = z_set->score.get(member)) {
            z_set->order.erase(*score, member);
            z_set->score.erase(member);
            return true;
        }
        return false;
    }

    assert(false);
    return false;
}

std::size_t hyper::RedisObject::zSetSize() const {
    assert(type_ == ObjectType::ZSet);
    if (encoding_ == ObjectEncoding::ZipList) {
        return std::get<std::unique_ptr<RedisZiplist>>(data_)->size() / 2;
    }
    if (encoding_ == ObjectEncoding::SkipList) {
        return std::get<std::unique_ptr<RedisZSet>>(data_)->score.size();
    }
    assert(false);
    return 0;
}

std::optional<std::size_t> hyper::RedisObject::zSetRank(std::string_view member) const {
    assert(type_ == ObjectType::ZSet);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (std::size_t i = 0; i < ziplist->size(); i += 2) {
            if (getEntryAsString_((*ziplist)[i]) == member) {
                return i / 2;
            }
        }
        return std::nullopt;
    }

    if (encoding_ == ObjectEncoding::SkipList) {
        auto& z_set = std::get<std::unique_ptr<RedisZSet>>(data_);
        if (auto res = z_set->score.get(member)) {
            return z_set->order.getRank(*res, std::string(member)) - 1;
        }
        return std::nullopt;
    }

    assert(false);
    return std::nullopt;
}

std::optional<std::size_t> hyper::RedisObject::zSetRevRank(std::string_view member) const {
    assert(type_ == ObjectType::ZSet);
    auto rank = zSetRank(member);
    if (!rank.has_value()) {
        return std::nullopt;
    }
    return zSetSize() - 1 - rank.value();
}

std::size_t hyper::RedisObject::zSetCount(const double min, const double max) const {
    assert(type_ == ObjectType::ZSet);
    if (isInvalidScore(min) || isInvalidScore(max) || min > max) {
        return 0;
    }

    if (encoding_ == ObjectEncoding::ZipList) {
        std::size_t count{0};
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (int i = 0; i < ziplist->size(); i += 2) {
            auto score = parseScore_((*ziplist)[i + 1]);
            if (score >= min && score <= max) {
                ++count;
            } else if (score > max) {
                return count;
            }
        }

        return count;
    }
    if (encoding_ == ObjectEncoding::SkipList) {
        auto& skip_list = std::get<std::unique_ptr<RedisZSet>>(data_)->order;
        ScoreRange<double> range{min, max};
        auto r1 = skip_list.getRankOfFirstInRange(range);
        if (r1 == 0) {
            return 0;
        }
        auto r2 = skip_list.getRankOfLastInRange(range);
        return r2 - r1 + 1;
    }
    assert(false);
    return 0;
}

hyper::RedisObject::ZSetRangeResult hyper::RedisObject::zSetRange(int start, int end) const {
    assert(type_ == ObjectType::ZSet);
    ZSetRangeResult res{};
    std::size_t len = zSetSize();
    if (start < 0) {
        start += static_cast<int>(len);
    }
    if (end < 0) {
        end += static_cast<int>(len);
    }
    if (start < 0) {
        start = 0;
    }
    if (start >= static_cast<int>(len) || start > end) {
        return {};
    }
    if (end >= static_cast<int>(len)) {
        end = static_cast<int>(len) - 1;
    }

    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (std::size_t i = start; i <= end; ++i) {
            std::string member = getEntryAsString_((*ziplist)[2 * i]);
            double score = parseScore_((*ziplist)[2 * i + 1]);
            res.emplace_back(std::move(member), score);
        }
        return res;
    }

    if (encoding_ == ObjectEncoding::SkipList) {
        auto& skip_list = std::get<std::unique_ptr<RedisZSet>>(data_)->order;
        skip_list.forEachByRank(
            static_cast<std::size_t>(start) + 1,
            static_cast<std::size_t>(end - start + 1),
            [&res](const double& score, const std::string& member) {
                res.emplace_back(member, score);
            });
        return res;
    }

    assert(false);
    return res;
}

hyper::RedisObject::ZSetRangeResult hyper::RedisObject::zSetRevRange(int start, int end) const {
    assert(type_ == ObjectType::ZSet);
    std::size_t len = zSetSize();
    if (start < 0) {
        start += static_cast<int>(len);
    }
    if (end < 0) {
        end += static_cast<int>(len);
    }
    if (start < 0) {
        start = 0;
    }
    if (start >= static_cast<int>(len) || start > end) {
        return {};
    }
    if (end >= static_cast<int>(len)) {
        end = static_cast<int>(len) - 1;
    }
    ZSetRangeResult res{};
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        for (std::size_t i = start; i <= end; ++i) {
            auto index = len - 1 - i;
            std::string member = getEntryAsString_((*ziplist)[2 * index]);
            double score = parseScore_((*ziplist)[2 * index + 1]);
            res.emplace_back(std::move(member), score);
        }
        return res;
    }
    if (encoding_ == ObjectEncoding::SkipList) {
        auto& skip_list = std::get<std::unique_ptr<RedisZSet>>(data_)->order;
        skip_list.forEachReverseByRank(
            len - static_cast<std::size_t>(start),
            static_cast<std::size_t>(end - start + 1),
            [&res](const double& score, const std::string& member) {
                res.emplace_back(member, score);
            });
        return res;
    }

    assert(false);
    return res;
}

double hyper::RedisObject::zSetIncrBy(std::string member, double increment) {
    assert(type_ == ObjectType::ZSet);
    auto result = zSetIncrByChecked(std::move(member), increment);
    assert(result.has_value());
    return result.value_or(std::numeric_limits<double>::quiet_NaN());
}

std::optional<double> hyper::RedisObject::zSetIncrByChecked(std::string member, double increment) {
    assert(type_ == ObjectType::ZSet);
    if (isInvalidScore(increment)) {
        return std::nullopt;
    }
    double old_score{};
    auto res = zSetScore(member);
    if (res.has_value()) {
        old_score += res.value();
    }
    double new_score = old_score + increment;
    if (isInvalidScore(new_score)) {
        return std::nullopt;
    }
    if (zSetAddDetailed(std::move(member), new_score) == ZSetAddStatus::InvalidScore) {
        return std::nullopt;
    }
    return new_score;
}

std::size_t hyper::RedisObject::zSetRemoveRangeByRank(int start, int end) {
    assert(type_ == ObjectType::ZSet);
    std::size_t len = zSetSize();
    if (start < 0) {
        start += static_cast<int>(len);
    }
    if (end < 0) {
        end += static_cast<int>(len);
    }
    if (start < 0) {
        start = 0;
    }
    if (start >= static_cast<int>(len) || start > end) {
        return 0;
    }
    if (end >= static_cast<int>(len)) {
        end = static_cast<int>(len) - 1;
    }

    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        const auto removed_count = static_cast<std::size_t>(end - start + 1);
        return ziplist->eraseRange(static_cast<std::size_t>(start) * 2, removed_count * 2) / 2;
    }

    if (encoding_ == ObjectEncoding::SkipList) {
        auto& z_set = std::get<std::unique_ptr<RedisZSet>>(data_);
        return z_set->order.eraseRangeByRank(
            static_cast<std::size_t>(start) + 1,
            static_cast<std::size_t>(end) + 1,
            [&z_set](const double&, const std::string& member) {
                z_set->score.erase(member);
            });
    }

    assert(false);
    return 0;
}

std::size_t hyper::RedisObject::zSetRemoveRangeByScore(double min, double max) {
    assert(type_ == ObjectType::ZSet);
    if (isInvalidScore(min) || isInvalidScore(max) || min > max) {
        return 0;
    }

    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        std::size_t entry_index{0};
        std::size_t first_entry{0};
        std::size_t entries_to_remove{0};
        bool found_range = false;
        bool past_range = false;

        ziplist->forEach([&](const ziplistEntryView& entry) {
            if (past_range) {
                ++entry_index;
                return;
            }

            if (entry_index % 2 == 1) {
                double score = parseScore_(entry);
                if (!found_range) {
                    if (score > max) {
                        past_range = true;
                    } else if (score >= min) {
                        found_range = true;
                        first_entry = entry_index - 1;
                        entries_to_remove = 2;
                    }
                } else if (score > max) {
                    past_range = true;
                } else {
                    entries_to_remove += 2;
                }
            }

            ++entry_index;
        });

        if (!found_range) {
            return 0;
        }
        return ziplist->eraseRange(first_entry, entries_to_remove) / 2;
    }

    if (encoding_ == ObjectEncoding::SkipList) {
        ScoreRange<double> range{min, max};
        auto& z_set = std::get<std::unique_ptr<RedisZSet>>(data_);
        return z_set->order.eraseRangeByScore(
            range,
            [&z_set](const double&, const std::string& member) {
                z_set->score.erase(member);
            });
    }

    assert(false);
    return 0;
}

bool hyper::RedisObject::shouldConvertZiplistToList_(std::string_view value, std::size_t next_size) noexcept {
    return next_size > ZipListMaxEntries || value.size() > ZipListMaxValue;
}


void hyper::RedisObject::convertHashZipListToDict_() {
    assert(type_ == ObjectType::Hash && encoding_ == ObjectEncoding::ZipList);

    auto dict = std::make_unique<RedisDict>();
    auto ziplist = std::move(std::get<std::unique_ptr<RedisZiplist>>(data_));
    for (int i = 0; i < ziplist->size(); i += 2) {
        std::string field = getEntryAsString_((*ziplist)[i]);
        RedisObjectPtr value = createSharedStringObject(getEntryAsString_((*ziplist)[i + 1]));
        dict->storage.insert(std::move(field), std::move(value));
    }

    encoding_ = ObjectEncoding::HashTable;
    data_ = std::move(dict);
}

void hyper::RedisObject::convertZiplistToList_() {
    assert(type_ == ObjectType::List && encoding_ == ObjectEncoding::ZipList);
    auto new_list = std::make_unique<RedisList>();
    auto current_list = std::move(std::get<std::unique_ptr<RedisZiplist>>(data_));
    auto& new_list_storage = new_list->storage;
    current_list->forEach([&new_list_storage](const ziplistEntryView& value) {
        if (value.isInteger()) {
            new_list_storage.pushBack(createSharedLongObject(value.integer()));
        } else if (value.isString()) {
            new_list_storage.pushBack(createSharedStringObject(value.string()));
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
    assert(type_ == ObjectType::ZSet && encoding_ == ObjectEncoding::ZipList);
    auto z_set = std::make_unique<RedisZSet>();
    auto ziplist = std::move(std::get<std::unique_ptr<RedisZiplist>>(data_));
    bool is_member = true;
    std::string member;
    ziplist->forEach([&is_member,&z_set,&member](const ziplistEntryView& entry_view) {
        if (is_member) {
            member = getEntryAsString_(entry_view);
            is_member = !is_member;
            return;
        }
        auto score = parseScore_(entry_view);
        z_set->order.insert(score, member);
        z_set->score.insert(std::move(member), score);
        is_member = !is_member;
    });
    encoding_ = ObjectEncoding::SkipList;
    data_ = std::move(z_set);
}


double hyper::RedisObject::parseScore_(const ziplistEntryView& entry) {
    if (entry.isInteger()) {
        return static_cast<double>(entry.integer());
    }
    if (entry.isString()) {
        double value{};
        auto str = entry.string();
        auto [ptr,ec] = std::from_chars(str.data(), str.data() + str.size(), value);
        if (ptr == str.data() + str.size() && ec == std::errc()) {
            return value;
        }
    }
    assert(false);
    return 0.0;
}

std::string hyper::RedisObject::getEntryAsString_(const ziplistEntryView& entry) {
    if (entry.isInteger()) {
        return std::to_string(entry.integer());
    }
    if (entry.isString()) {
        return std::string(entry.string());
    }
    assert(false);
    return "";
}

std::string hyper::RedisObject::formatScore_(double score) {
    std::array<char, 128> buffer{};
    auto [ptr,ec] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), score);
    if (ec == std::errc()) {
        return {buffer.data(), ptr};
    }
    assert(false);
    return "";
}
