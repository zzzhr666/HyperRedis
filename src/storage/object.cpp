#include <array>
#include <cassert>
#include <charconv>
#include <memory>

#include "hyper/storage/object.hpp"
#include "hyper/datastructures/dict.hpp"
#include "hyper/datastructures/intset.hpp"
#include "hyper/datastructures/linked_list.hpp"
#include "hyper/datastructures/skip_list.hpp"
#include "hyper/datastructures/ziplist.hpp"


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
        res += increment;
        return res;
    }
    if (encoding_ == ObjectEncoding::Raw) {
        long value{};
        auto& origin = std::get<std::string>(data_);
        auto [ptr,ec] = std::from_chars(origin.data(), origin.data() + origin.size(), value);
        if (ptr == origin.data() + origin.size() && ec == std::errc()) {
            value += increment;
            encoding_ = ObjectEncoding::Int;
            data_ = value;
            return value;
        }
        return std::nullopt;
    }
    assert(false);
    return std::nullopt;
}

std::optional<double> hyper::RedisObject::stringIncrByFloat(double increment) {
    assert(type_ == ObjectType::String);
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

std::shared_ptr<hyper::RedisObject> hyper::RedisObject::listIndex(int index) const {
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
        return createStringObject(getEntryAsString_(ziplist->at(index).value()));
    }
    if (encoding_ == ObjectEncoding::LinkedList) {
        auto& list = std::get<std::unique_ptr<RedisList>>(data_)->storage;
        std::size_t current_index{0};
        std::shared_ptr<RedisObject> res;
        for (auto it = list.begin(); it != list.end(); ++it, ++current_index) {
            if (current_index == index) {
                return *it;
            }
        }
    }

    assert(false);
    return nullptr;
}

bool hyper::RedisObject::listSet(int index, const std::shared_ptr<RedisObject>& value) {
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
                                                          const std::shared_ptr<RedisObject>& value, bool before) {
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
        count = len;
    }
    bool from_front = count > 0;
    count = std::abs(count);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        if (from_front) {
            std::size_t index{0};
            while (index < len) {
                auto current = getEntryAsString_(ziplist->at(index).value());
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
        for (int i = len - 1; i >= 0; --i) {
            auto current = getEntryAsString_(ziplist->at(i).value());
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
                if (ret++ == count) {
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
        end = len - 1;
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

std::optional<double> hyper::RedisObject::zSetScore(std::string_view member) const {
    assert(type_ == ObjectType::ZSet);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        auto res_op = ziplist->find(member);
        if (res_op.has_value() && res_op.value() % 2 == 0) {
            return parseScore_(ziplist->at(res_op.value() + 1).value());
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
        auto res = ziplist->find(member);
        if (res.has_value() && res.value() % 2 == 0) {
            ziplist->erase(res.value());
            ziplist->erase(res.value());
            return true;
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

std::optional<size_t> hyper::RedisObject::zSetRank(std::string_view member) const {
    assert(type_ == ObjectType::ZSet);
    if (encoding_ == ObjectEncoding::ZipList) {
        auto& ziplist = std::get<std::unique_ptr<RedisZiplist>>(data_);
        auto res = ziplist->find(member);
        if (res.has_value() && res.value() % 2 == 0) {
            return res.value() / 2;
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
