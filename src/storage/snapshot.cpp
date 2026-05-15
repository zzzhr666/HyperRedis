#include "hyper/storage/snapshot.hpp"

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include "hyper/storage/checksum_calculator.hpp"

namespace {
    struct SaveEntry {
        std::string key;
        hyper::RedisObjectPtr value;
        hyper::UnixMilliseconds ttl;
    };

    hyper::UnixMilliseconds toUnixMilliseconds(hyper::ExpireTimePoint time) {
        return std::chrono::duration_cast<hyper::Milliseconds>(
            time.time_since_epoch()
        ).count();
    }

    hyper::ExpireTimePoint fromUnixMilliseconds(hyper::UnixMilliseconds ms) {
        return hyper::ExpireTimePoint(hyper::Milliseconds(ms));
    }

    class RdbWriter {
    public:
        RdbWriter() = default;

        ~RdbWriter() = default;

        void writeLength(std::uint64_t len) {
            if (len < 64) {
                writeUint8(static_cast<std::uint8_t>(len));
            } else if (len < 16384) {
                std::uint16_t encoded = (static_cast<std::uint16_t>(len) & 0x3FFF) | 0x4000;
                writeUint16BigEndian(encoded);
            } else {
                writeUint8(0x80);
                writeUint32BigEndian(static_cast<std::uint32_t>(len));
            }
        }

        void writeRaw(const void* data, size_t len) {
            auto p = static_cast<const std::uint8_t*>(data);
            buffer_.insert(buffer_.end(), p, p + len);
        }

        void writeRaw(std::string_view str) {
            writeRaw(str.data(), str.size());
        }


        std::vector<std::uint8_t> finish() {
            return std::move(buffer_);
        }

        void writeUint8(std::uint8_t val) {
            buffer_.push_back(val);
        }

        void writeUint16BigEndian(std::uint16_t val) {
            buffer_.push_back(val >> 8);
            buffer_.push_back(val & 0xff);
        }

        void writeUint32BigEndian(std::uint32_t val) {
            buffer_.push_back(val >> 24);
            buffer_.push_back(val >> 16);
            buffer_.push_back(val >> 8);
            buffer_.push_back(val & 0xff);
        }

        void writeCheckSum(std::uint64_t check_sum) {
            buffer_.push_back(check_sum & 0xff);
            buffer_.push_back(check_sum >> 8);
            buffer_.push_back(check_sum >> 16);
            buffer_.push_back(check_sum >> 24);
            buffer_.push_back(check_sum >> 32);
            buffer_.push_back(check_sum >> 40);
            buffer_.push_back(check_sum >> 48);
            buffer_.push_back(check_sum >> 56);
        }

        void writeUint64BigEndian(std::uint64_t val) {
            buffer_.push_back(val >> 56);
            buffer_.push_back(val >> 48);
            buffer_.push_back(val >> 40);
            buffer_.push_back(val >> 32);
            buffer_.push_back(val >> 24);
            buffer_.push_back(val >> 16);
            buffer_.push_back(val >> 8);
            buffer_.push_back(val & 0xff);
        }

        void writeStringObject(std::string_view obj) {
            writeLength(obj.size());
            writeRaw(obj);
        }

        void writeDouble(double value) {
            auto bits = std::bit_cast<std::uint64_t>(value);
            writeUint64BigEndian(bits);
        }

        [[nodiscard]] std::uint64_t calculateChecksum() const {
            return hyper::ChecksumCalculator::calculate(buffer_);
        }

        void writeValue(const hyper::RedisObjectPtr& obj) {
            switch (obj->getType()) {
            case hyper::ObjectType::String: {
                writeStringObject(obj->asString());
                break;
            }
            case hyper::ObjectType::List: {
                writeLength(obj->listLen());
                for (const auto& item : obj->listRangeAsStrings(0, -1)) {
                    writeStringObject(item);
                }
                break;
            }
            case hyper::ObjectType::Set: {
                std::vector<std::string> members;
                members.reserve(obj->setSize());
                obj->setForEach([&members](std::string_view member) {
                    members.emplace_back(member);
                });
                std::ranges::sort(members);
                writeLength(members.size());
                for (const auto& member : members) {
                    writeStringObject(member);
                }
                break;
            }
            case hyper::ObjectType::ZSet: {
                writeLength(obj->zSetSize());
                auto infos = obj->zSetRange(0, -1);
                for (const auto& [member, score] : infos) {
                    writeStringObject(member);
                    writeDouble(score);
                }
                break;
            }
            case hyper::ObjectType::Hash: {
                auto fields = obj->hashGetAllAsStrings();
                std::ranges::sort(fields);
                writeLength(fields.size());
                for (const auto& [field, value] : fields) {
                    writeStringObject(field);
                    writeStringObject(value);
                }
                break;
            }
            default:
                break;
            }
        }

    private:
        std::vector<std::uint8_t> buffer_;
    };

    class RdbReader {
    public:
        explicit RdbReader(const std::vector<std::uint8_t>& data)
            : data_(data), position_(0), success_(true) {}

        [[nodiscard]] bool keepSuccess() const {
            return success_;
        }

        void enFail() noexcept {
            success_ = false;
        }

        std::uint8_t readUint8() {
            if (!success_ || !canRead_(1)) {
                success_ = false;
                return 0;
            }
            return data_[position_++];
        }

        std::uint16_t readUint16BigEndian() {
            if (!success_ || !canRead_(2)) {
                success_ = false;
                return 0;
            }
            std::uint16_t value = static_cast<std::uint16_t>(data_[position_]) << 8 |
                static_cast<std::uint16_t>(data_[position_ + 1]);
            position_ += 2;
            return value;
        }

        std::uint32_t readUint32BigEndian() {
            if (!success_ || !canRead_(4)) {
                success_ = false;
                return 0;
            }
            std::uint32_t value = static_cast<std::uint32_t>(data_[position_]) << 24 |
                static_cast<std::uint32_t>(data_[position_ + 1]) << 16 |
                static_cast<std::uint32_t>(data_[position_ + 2]) << 8 |
                static_cast<std::uint32_t>(data_[position_ + 3]);
            position_ += 4;
            return value;
        }

        std::uint64_t readUint64BigEndian() {
            if (!success_ || !canRead_(8)) {
                success_ = false;
                return 0;
            }
            std::uint64_t value = static_cast<std::uint64_t>(data_[position_]) << 56 |
                static_cast<std::uint64_t>(data_[position_ + 1]) << 48 |
                static_cast<std::uint64_t>(data_[position_ + 2]) << 40 |
                static_cast<std::uint64_t>(data_[position_ + 3]) << 32 |
                static_cast<std::uint64_t>(data_[position_ + 4]) << 24 |
                static_cast<std::uint64_t>(data_[position_ + 5]) << 16 |
                static_cast<std::uint64_t>(data_[position_ + 6]) << 8 |
                static_cast<std::uint64_t>(data_[position_ + 7]);

            position_ += 8;
            return value;
        }

        std::uint64_t readChecksum() {
            if (!success_ || !canRead_(8)) {
                success_ = false;
                return 0;
            }
            std::uint64_t value = static_cast<std::uint64_t>(data_[position_]) |
                static_cast<std::uint64_t>(data_[position_ + 1]) << 8 |
                static_cast<std::uint64_t>(data_[position_ + 2]) << 16 |
                static_cast<std::uint64_t>(data_[position_ + 3]) << 24 |
                static_cast<std::uint64_t>(data_[position_ + 4]) << 32 |
                static_cast<std::uint64_t>(data_[position_ + 5]) << 40 |
                static_cast<std::uint64_t>(data_[position_ + 6]) << 48 |
                static_cast<std::uint64_t>(data_[position_ + 7]) << 56;

            position_ += 8;
            return value;
        }

        std::uint64_t readLength() {
            const auto first = readUint8();
            if (!success_) {
                return 0;
            }
            const auto flag = first & 0xC0;
            if (flag == 0x00) {
                //  < 64
                return first & 0x3F;
            }
            if (flag == 0x40) {
                const auto second = readUint8();
                if (!success_) {
                    return 0;
                }
                return static_cast<std::uint64_t>(first & 0x3F) << 8 |
                    static_cast<std::uint64_t>(second);
            }
            if (first == 0x80) {
                return readUint32BigEndian();
            }
            success_ = false;
            return 0;
        }

        std::string readRawString(std::size_t len) {
            if (!success_ || !canRead_(len)) {
                success_ = false;
                return {};
            }
            std::string res{
                reinterpret_cast<const char*>(data_.data()) + position_,
                reinterpret_cast<const char*>(data_.data()) + position_ + len
            };
            position_ += len;
            return res;
        }

        std::string readStringObject() {
            auto len = readLength();
            return readRawString(len);
        }

        double readDouble() {
            auto bits = readUint64BigEndian();
            if (!success_) {
                return 0;
            }
            return std::bit_cast<double>(bits);
        }

        [[nodiscard]] bool atEnd() const noexcept {
            return position_ == data_.size();
        }

        [[nodiscard]] bool nextIsEof() const noexcept {
            return success_ && canRead_(1) && data_[position_] == hyper::OpCode_EOF;
        }

        hyper::RedisObjectPtr readValue(hyper::ObjectType type) {
            switch (type) {
            case hyper::ObjectType::String: {
                auto value = readStringObject();
                if (!success_) {
                    return nullptr;
                }
                return hyper::RedisObject::createSharedStringObject(value);
            }
            case hyper::ObjectType::List: {
                auto len = readLength();
                if (!success_) {
                    return nullptr;
                }
                auto obj = hyper::RedisObject::createSharedListObject();
                for (std::uint64_t i = 0; i < len; ++i) {
                    auto item = readStringObject();
                    if (!success_) {
                        return nullptr;
                    }
                    obj->listRightPush(hyper::RedisObject::createSharedStringObject(item));
                }
                return obj;
            }
            case hyper::ObjectType::Set: {
                auto len = readLength();
                if (!success_) {
                    return nullptr;
                }
                auto obj = hyper::RedisObject::createSharedSetObject();
                for (std::uint64_t i = 0; i < len; ++i) {
                    auto member = readStringObject();
                    if (!success_) {
                        return nullptr;
                    }
                    obj->setAdd(member);
                }
                return obj;
            }

            case hyper::ObjectType::ZSet: {
                auto len = readLength();
                if (!success_) {
                    return nullptr;
                }
                auto obj = hyper::RedisObject::createSharedZSetObject();
                for (std::uint64_t i = 0; i < len; ++i) {
                    auto member = readStringObject();
                    auto score = readDouble();
                    if (!success_) {
                        return nullptr;
                    }
                    obj->zSetAdd(member, score);
                }
                return obj;
            }

            case hyper::ObjectType::Hash: {
                auto len = readLength();
                if (!success_) {
                    return nullptr;
                }
                auto obj = hyper::RedisObject::createSharedHashObject();
                for (std::uint64_t i = 0; i < len; ++i) {
                    auto field = readStringObject();
                    auto value = readStringObject();
                    if (!success_) {
                        return nullptr;
                    }
                    obj->hashSet(field, hyper::RedisObject::createSharedStringObject(value));
                }
                return obj;
            }
            default:
                success_ = false;
                return nullptr;
            }
        }

    private:
        [[nodiscard]] bool canRead_(std::size_t size) const noexcept {
            return position_ <= data_.size() && size <= data_.size() - position_;
        }

    private:
        const std::vector<std::uint8_t>& data_;
        std::size_t position_;
        bool success_;
    };
}

std::vector<std::uint8_t> hyper::Snapshot::save(RedisManager& manager, ExpireTimePoint now) {
    RdbWriter writer;
    writer.writeRaw(Magic);
    writer.writeRaw(Version);

    for (std::size_t i = 0; i < manager.dbCount(); ++i) {
        auto db = manager.db(i);
        std::vector<SaveEntry> entries;
        db->forEach(now, [&](std::string_view key, const RedisObjectPtr& value) {
            entries.emplace_back(std::string(key), value, db->pttl(key, now));
        });
        std::ranges::sort(entries, {}, &SaveEntry::key);
        if (entries.empty()) {
            continue;
        }
        writer.writeUint8(OpCode_SelectDb);
        writer.writeLength(i);
        for (auto& entry : entries) {
            if (entry.ttl >= 0) {
                writer.writeUint8(OpCode_ExpireTimeMs);
                writer.writeUint64BigEndian(toUnixMilliseconds(now) + entry.ttl);
            }
            writer.writeUint8(static_cast<std::uint8_t>(entry.value->getType()));
            writer.writeStringObject(entry.key);
            writer.writeValue(entry.value);
        }
    }
    writer.writeUint8(OpCode_EOF);
    auto checksum = writer.calculateChecksum();
    writer.writeCheckSum(checksum);
    return writer.finish();
}

bool hyper::Snapshot::load(const std::vector<std::uint8_t>& data, RedisManager& manager, ExpireTimePoint now) {
    RdbReader reader(data);

    auto magic = reader.readRawString(5);
    if (magic != "REDIS") {
        return false;
    }
    auto version = reader.readRawString(4);
    if (version != "0009") {
        return false;
    }
    RedisManager current_manager(manager.dbCount());
    auto current_db = current_manager.db(0);

    while (!reader.nextIsEof()) {
        auto opcode = reader.readUint8();
        if (!reader.keepSuccess()) {
            return false;
        }
        if (opcode == OpCode_SelectDb) {
            auto db_index = reader.readLength();
            if (!reader.keepSuccess() || db_index >= current_manager.dbCount()) {
                return false;
            }
            current_db = current_manager.db(db_index);
            continue;
        }
        ExpireTimePoint deadline{};
        bool has_expired = false;
        if (opcode == OpCode_ExpireTimeMs) {
            auto expire_ms = reader.readUint64BigEndian();
            if (!reader.keepSuccess()) {
                return false;
            }
            deadline = fromUnixMilliseconds(static_cast<UnixMilliseconds>(expire_ms));
            has_expired = true;
            opcode = reader.readUint8();
            if (!reader.keepSuccess()) {
                return false;
            }
        }

        if (opcode > static_cast<std::uint8_t>(ObjectType::Hash)) {
            return false;
        }
        auto type = static_cast<ObjectType>(opcode);
        auto key = reader.readStringObject();
        auto value = reader.readValue(type);
        if (!reader.keepSuccess() || value == nullptr) {
            return false;
        }
        if (has_expired && deadline <= now) {
            continue; //已经过期的直接忽略
        }
        current_db->set(key, value);
        if (has_expired) {
            current_db->expireAt(key, now, deadline);
        }
    }
    reader.readUint8(); // read EOF
    auto checksum = reader.readChecksum();
    auto target = ChecksumCalculator::calculate(std::span{data.data(), data.size() - sizeof(uint64_t)});
    if (!reader.keepSuccess() || checksum != target || !reader.atEnd()) {
        return false;
    }
    manager.swapAll(current_manager);
    return true;
}
