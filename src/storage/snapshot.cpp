#include "hyper/storage/snapshot.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

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
                for (const auto& [member,score] : infos) {
                    writeStringObject(member);
                    writeRaw(&score, sizeof(score));
                }
                break;
            }
            case hyper::ObjectType::Hash: {
                auto fields = obj->hashGetAllAsStrings();
                std::ranges::sort(fields);
                writeLength(fields.size());
                for (const auto& [field,value] : fields) {
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
            : data_(data), position_(0), success_(true) {
        }

        [[nodiscard]] bool success() const {
            return success_;
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
            if (!success()) {
                return 0;
            }
            const auto flag = first & 0xC0;
            if (flag == 0x00) { //  < 64
                return first & 0x3F;
            }
            if (flag == 0x40) {
                const auto second = readUint8();
                if (!success_) {
                    return 0;
                }
                return static_cast<std::uint64_t>(first&0x3F) << 8 |
                    static_cast<std::uint64_t>(second);
            }
            if (first == 0x80) {
                return readUint32BigEndian();
            }
            success_ = false;
            return 0;
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

std::vector<std::uint8_t> hyper::Snapshot::save(RedisManager& manager, ExpireTimePoint now) const {
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
    writer.writeCheckSum(0); //TODO: 添加校验机制
    return writer.finish();
}

bool hyper::Snapshot::load(const std::vector<std::uint8_t>& data, RedisManager& manager, ExpireTimePoint now) const {
    (void)data;
    (void)manager;
    (void)now;
    return false;
}
