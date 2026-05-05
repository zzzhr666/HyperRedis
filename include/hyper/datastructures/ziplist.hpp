#pragma once

#include <algorithm>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <variant>
#include <vector>
#include <charconv>
#include <limits>


// tail   len  entries   end
//   4     2    ....    1(255)
namespace hyper {
    class ziplistEntryView {
    public:
        enum class Type {
            String,
            Integer
        };

        static ziplistEntryView fromInteger(std::int64_t value) {
            return ziplistEntryView(value);
        }

        static ziplistEntryView fromString(std::string_view value) {
            return ziplistEntryView(value);
        }

        [[nodiscard]] Type type() const noexcept {
            return std::holds_alternative<std::string_view>(value_) ? Type::String : Type::Integer;
        }

        [[nodiscard]] bool isString() const noexcept {
            return std::holds_alternative<std::string_view>(value_);
        }

        [[nodiscard]] bool isInteger() const noexcept {
            return std::holds_alternative<std::int64_t>(value_);
        }

        [[nodiscard]] std::string_view string() const {
            assert(isString());
            return std::get<std::string_view>(value_);
        }

        [[nodiscard]] std::int64_t integer() const {
            assert(isInteger());
            return std::get<std::int64_t>(value_);
        }

    private:
        explicit ziplistEntryView(std::string_view value) : value_(value) {}
        explicit ziplistEntryView(std::int64_t value) : value_(value) {}
        std::variant<std::string_view, std::int64_t> value_;
    };

    class ziplist {
    public:
        ziplist() {
            initialize_();
        }

        [[nodiscard]] std::size_t size() const noexcept {
            return getLen_();
        }

        [[nodiscard]] bool empty() const noexcept {
            return size() == 0;
        }

        [[nodiscard]] std::size_t byteSize() const noexcept {
            return getBytes_();
        }

        void clear() {
            initialize_();
        }

        template <typename FUNCTION>
            requires std::invocable<FUNCTION&, const ziplistEntryView&>
        void forEach(FUNCTION&& func) const {
            std::size_t offset = HeaderSize;
            while (entries_[offset] != EndMarker) {
                auto entry_view = parseEntry_(offset);
                func(entry_view.value);
                offset += entry_view.total_len;
            }
        }

        [[nodiscard]] std::optional<ziplistEntryView> at(std::size_t index) const {
            if (index >= getLen_()) {
                return std::nullopt;
            }
            std::size_t offset = HeaderSize;
            std::size_t current_index{0};
            while (entries_[offset] != EndMarker) {
                auto entry_view = parseEntry_(offset);
                if (current_index++ == index) {
                    return entry_view.value;
                }

                offset += entry_view.total_len;
            }
            return std::nullopt;
        }

        ziplistEntryView operator[](std::size_t index) const {
            assert(index < size());
            std::size_t offset = HeaderSize;
            std::size_t current_index{0};
            while (entries_[offset]!= EndMarker) {
                auto entry_view = parseEntry_(offset);
                if (current_index++ == index) {
                    return entry_view.value;
                }
                offset += entry_view.total_len;
            }
            assert(false);
            return ziplistEntryView::fromString("");
        }

        [[nodiscard]] std::optional<std::size_t> find(std::string_view value) const {
            auto offset = HeaderSize;
            std::size_t current_index{0};
            while (entries_[offset] != EndMarker) {
                auto entry_view = parseEntry_(offset);
                if (entry_view.value.isString() && entry_view.value.string() == value) {
                    return current_index;
                }
                ++current_index;
                offset += entry_view.total_len;
            }
            return std::nullopt;
        }

        void pushBack(std::string_view data) {
            auto endmarker_offset = entries_.size() - EndSize;


            std::size_t prev_len_value{};
            if (!empty()) {
                auto tail_offset = getTail_();
                prev_len_value = endmarker_offset - tail_offset;
            }
            auto prev_len_size = prevLenSize_(prev_len_value);
            auto info = determineEncoding_(data);
            auto entry_len = prev_len_size + info.encoding_size + info.payload_size;
            entries_.resize(entries_.size() + entry_len);
            writePrevLen_(endmarker_offset, prev_len_value);

            if (info.is_integer) {
                entries_[endmarker_offset + prev_len_size] = static_cast<std::byte>(info.encoding_byte);
                writeInteger_(endmarker_offset + prev_len_size + info.encoding_size, info.encoding_byte,
                              info.int_value);
            } else {
                writeStringEncoding_(endmarker_offset + prev_len_size, info.payload_size);
                std::memcpy(entries_.data() + endmarker_offset + prev_len_size + info.encoding_size, data.data(),
                            info.payload_size);
            }

            entries_.back() = EndMarker;
            setTail_(static_cast<std::uint32_t>(endmarker_offset));
            setLen_(static_cast<std::uint16_t>(getLen_() + 1));
        }

        void pushFront(std::string_view data) {
            if (empty()) {
                pushBack(data);
                return;
            }
            auto entry_view = parseEntry_(HeaderSize);
            insertNode_(entry_view, data);
        }

        std::optional<std::string> popFront() {
            if (empty()) {
                return std::nullopt;
            }
            auto entry_view = parseEntry_(HeaderSize);

            std::string res;
            if (entry_view.value.isInteger()) {
                res = std::to_string(entry_view.value.integer());
            } else {
                res = std::string{entry_view.value.string()};
            }

            eraseNode_(entry_view);
            return res;
        }

        std::optional<std::string> popBack() {
            if (empty()) {
                return std::nullopt;
            }
            auto entry_view = parseEntry_(getTail_());

            std::string res;
            if (entry_view.value.isInteger()) {
                res = std::to_string(entry_view.value.integer());
            } else {
                res = std::string{std::string{entry_view.value.string()}};
            }
            eraseNode_(entry_view);
            return res;
        }

        bool erase(std::size_t index) {
            auto len = getLen_();
            if (index >= len) {
                return false;
            }
            if (len == 1) {
                initialize_();
                return true;
            }
            if (index == 0) {
                popFront();
                return true;
            }
            if (index == len - 1) {
                popBack();
                return true;
            }
            auto current_index{0};
            auto offset = HeaderSize;
            while (entries_[offset] != EndMarker) {
                auto entry_view = parseEntry_(offset);
                if (current_index++ == index) {
                    eraseNode_(entry_view);
                    return true;
                }
                offset += entry_view.total_len;
            }

            return false;
        }

        std::size_t eraseRange(std::size_t index, std::size_t count) {
            auto len = getLen_();
            if (index >= len || count == 0) {
                return 0;
            }

            auto removed_count = std::min(count, len - index);
            if (removed_count == len) {
                initialize_();
                return removed_count;
            }

            std::size_t current_index{0};
            std::size_t offset = HeaderSize;
            while (entries_[offset] != EndMarker) {
                auto entry_view = parseEntry_(offset);
                if (current_index == index) {
                    eraseRange_(entry_view, removed_count);
                    return removed_count;
                }
                ++current_index;
                offset += entry_view.total_len;
            }

            return 0;
        }

        bool insert(std::size_t index, std::string_view data) {
            if (index > size()) {
                return false;
            }
            if (index == 0) {
                pushFront(data);
                return true;
            }
            if (index == size()) {
                pushBack(data);
                return true;
            }
            auto current_index = 0;
            auto offset = HeaderSize;
            while (entries_[offset] != EndMarker) {
                auto entry_view = parseEntry_(offset);
                if (current_index++ == index) {
                    insertNode_(entry_view, data);
                    return true;
                }
                offset += entry_view.total_len;
            }
            return false;
        }

    private:
        static constexpr std::size_t HeaderSize = sizeof(std::uint32_t) + sizeof(std::uint16_t);
        static constexpr std::size_t EndSize = 1;
        static constexpr std::size_t EmptyByteSize = HeaderSize + EndSize;
        static constexpr std::byte EndMarker{0xFF};
        static constexpr std::byte PrevLen5BitMarker{0xFE};

        static constexpr std::uint8_t String6BitMarker = 0x00;
        static constexpr std::uint8_t String14BitMarker = 0x40;
        static constexpr std::uint8_t String32BitMarker = 0x80;
        static constexpr std::uint8_t EncodingTypeMask = 0xC0;
        static constexpr std::uint8_t StringLengthMask = 0x3F;
        static constexpr std::size_t ShortStringMaxLength = 63;
        static constexpr std::size_t MediumStringMaxLength = 16383;
        static constexpr std::size_t PrevLen1BitMaxLength = 253;
        static constexpr std::uint8_t Int16Marker = 0xC0;
        static constexpr std::uint8_t Int32Marker = 0xD0;
        static constexpr std::uint8_t Int64Marker = 0xE0;
        static constexpr std::uint8_t Int24Marker = 0xF0;
        static constexpr std::uint8_t Int8Marker = 0xFE;
        static constexpr std::uint8_t IntImmMin = 0xF1;
        static constexpr std::uint8_t IntImmMax = 0xFD;
        static constexpr int Int24BitMin = -8388608;
        static constexpr int Int24BitMax = 8388607;

        struct entryView {
            std::size_t offset{};
            std::size_t prev_len_size{};
            std::size_t prev_len_value{};
            std::uint8_t encoding{};
            std::size_t content_offset{};
            std::size_t content_len{};
            std::size_t total_len{};
            ziplistEntryView value;
        };

        static bool tryParseInteger_(std::string_view data, std::int64_t& out_value) {
            if (data.empty()) {
                return false;
            }

            if (data.size() > 1) {
                if (data[0] == '0' || data[0] == '-' && data[1] == '0') {
                    return false;
                }
            }
            const char* first = data.data();
            const char* last = first + data.size();
            auto [ptr , ec] = std::from_chars(first, last, out_value);
            return ec == std::errc() && ptr == last;
        }

        struct EncodingInfo {
            std::uint8_t encoding_byte{};
            std::size_t encoding_size{};
            std::size_t payload_size{};
            bool is_integer{false};
            std::int64_t int_value{};
        };

        [[nodiscard]] static EncodingInfo determineEncoding_(std::string_view data) noexcept {
            EncodingInfo info;
            if (tryParseInteger_(data, info.int_value)) {
                info.is_integer = true;
                info.encoding_size = 1;
                auto value = info.int_value;
                if (value >= 0 && value <= 12) {
                    info.encoding_byte = static_cast<std::uint8_t>(IntImmMin + value);
                    info.payload_size = 0;
                } else if (value >= std::numeric_limits<std::int8_t>::min() && value <= std::numeric_limits<
                    std::int8_t>::max()) {
                    info.encoding_byte = Int8Marker;
                    info.payload_size = 1;
                } else if (value >= std::numeric_limits<std::int16_t>::min() && value <= std::numeric_limits<
                    std::int16_t>::max()) {
                    info.encoding_byte = Int16Marker;
                    info.payload_size = 2;
                } else if (value >= Int24BitMin && value <= Int24BitMax) {
                    info.encoding_byte = Int24Marker;
                    info.payload_size = 3;
                } else if (value >= std::numeric_limits<std::int32_t>::min() && value <= std::numeric_limits<
                    std::int32_t>::max()) {
                    info.encoding_byte = Int32Marker;
                    info.payload_size = 4;
                } else {
                    info.encoding_byte = Int64Marker;
                    info.payload_size = 8;
                }
                return info;
            }

            info.is_integer = false;
            info.payload_size = data.size();
            info.encoding_size = stringEncodingSize_(data.size());

            return info;
        }

        void writeInteger_(std::size_t offset, std::uint8_t encoding_byte, std::int64_t value) {
            if (encoding_byte >= IntImmMin && encoding_byte <= IntImmMax) {
                return;
            }
            if (encoding_byte == Int8Marker) {
                auto val = static_cast<std::int8_t>(value);
                std::memcpy(entries_.data() + offset, &val, sizeof(val));
            } else if (encoding_byte == Int16Marker) {
                auto val = static_cast<std::int16_t>(value);
                std::memcpy(entries_.data() + offset, &val, sizeof(val));
            } else if (encoding_byte == Int24Marker) {
                auto val = static_cast<std::int32_t>(value);
                std::memcpy(entries_.data() + offset, &val, 3);
            } else if (encoding_byte == Int32Marker) {
                auto val = static_cast<std::int32_t>(value);
                std::memcpy(entries_.data() + offset, &val, sizeof(val));
            } else if (encoding_byte == Int64Marker) {
                std::memcpy(entries_.data() + offset, &value, sizeof(value));
            }
        }

        [[nodiscard]] std::int64_t readInteger_(std::size_t offset, std::uint8_t encoding_byte) const {
            if (encoding_byte >= IntImmMin && encoding_byte <= IntImmMax) {
                return encoding_byte - IntImmMin;
            }
            if (encoding_byte == Int8Marker) {
                std::int8_t v{};
                std::memcpy(&v, entries_.data() + offset, sizeof(v));
                return v;
            }
            if (encoding_byte == Int16Marker) {
                std::int16_t v{};
                std::memcpy(&v, entries_.data() + offset, sizeof(v));
                return v;
            }
            if (encoding_byte == Int24Marker) {
                std::int32_t v{0};
                std::memcpy(&v, entries_.data() + offset, 3);
                if (v & 0x800000) {
                    v |= 0xFF000000;
                }
                return v;
            }

            if (encoding_byte == Int32Marker) {
                std::int32_t v{};
                std::memcpy(&v, entries_.data() + offset, sizeof(v));
                return v;
            }

            if (encoding_byte == Int64Marker) {
                std::int64_t v{};
                std::memcpy(&v, entries_.data() + offset, sizeof(v));
                return v;
            }

            return 0;
        }

        void insertNode_(const entryView& entry_view, std::string_view data) {
            auto info = determineEncoding_(data);

            auto new_prev_len_size = prevLenSize_(entry_view.prev_len_value);

            auto new_entry_len = new_prev_len_size + info.encoding_size + info.payload_size;

            auto y_new_prev_len_size = prevLenSize_(new_entry_len);
            auto y_old_prev_len_size = entry_view.prev_len_size;


            auto diff = static_cast<std::ptrdiff_t>(y_new_prev_len_size - y_old_prev_len_size);

            std::size_t old_total_size = entries_.size();

            entries_.resize(old_total_size + diff + new_entry_len);

            std::size_t move_src = entry_view.offset + y_old_prev_len_size;

            std::size_t move_dest = entry_view.offset + new_entry_len + y_new_prev_len_size;

            std::size_t move_len = old_total_size - move_src;

            std::memmove(entries_.data() + move_dest, entries_.data() + move_src, move_len);


            writePrevLen_(entry_view.offset, entry_view.prev_len_value);

            if (info.is_integer) {
                entries_[entry_view.offset + new_prev_len_size] = static_cast<std::byte>(info.encoding_byte);
                writeInteger_(entry_view.offset + new_prev_len_size + info.encoding_size, info.encoding_byte,
                              info.int_value);
            } else {
                writeStringEncoding_(entry_view.offset + new_prev_len_size, info.payload_size);
                std::memcpy(entries_.data() + entry_view.offset + new_prev_len_size + info.encoding_size,
                            data.data(), info.payload_size);
            }


            std::size_t y_new_offset = entry_view.offset + new_entry_len;
            writePrevLen_(y_new_offset, new_entry_len);

            if (getTail_() == entry_view.offset) {
                setTail_(y_new_offset);
            } else {
                setTail_(static_cast<std::uint32_t>(getTail_() + new_entry_len + diff));
            }

            setLen_(static_cast<std::uint16_t>(getLen_() + 1));


            cascadeUpdate_(y_new_offset);
        }

        void eraseNode_(const entryView& entry_view) {
            auto next_offset = entry_view.offset + entry_view.total_len;
            if (entries_[next_offset] == EndMarker) {
                entries_[entry_view.offset] = EndMarker;
                entries_.resize(entries_.size() - entry_view.total_len);
                if (entry_view.offset == HeaderSize) {
                    setTail_(HeaderSize);
                } else {
                    setTail_(entry_view.offset - entry_view.prev_len_value);
                }
                setLen_(getLen_() - 1);
                return;
            }
            auto next_entry = parseEntry_(next_offset);
            auto new_next_prev_size = prevLenSize_(entry_view.prev_len_value);
            bool force_flag = false;
            if (new_next_prev_size < next_entry.prev_len_size) {
                new_next_prev_size = next_entry.prev_len_size;
                force_flag = true;
            }
            auto diff = static_cast<std::ptrdiff_t>(new_next_prev_size - next_entry.prev_len_size);
            std::size_t deleted_bytes = entry_view.total_len - diff;

            std::size_t move_src = next_offset + next_entry.prev_len_size;
            std::size_t move_dest = entry_view.offset + new_next_prev_size;
            std::size_t move_len = entries_.size() - move_src;

            std::memmove(entries_.data() + move_dest, entries_.data() + move_src, move_len);
            entries_.resize(entries_.size() - deleted_bytes);
            writePrevLen_(entry_view.offset, entry_view.prev_len_value, force_flag);
            if (getTail_() == next_offset) {
                setTail_(entry_view.offset);
            } else {
                setTail_(getTail_() - deleted_bytes);
            }


            setLen_(getLen_() - 1);
            cascadeUpdate_(entry_view.offset);
        }

        void eraseRange_(const entryView& first_entry, std::size_t count) {
            std::size_t next_offset = first_entry.offset;
            for (std::size_t i = 0; i < count; ++i) {
                auto entry_view = parseEntry_(next_offset);
                next_offset += entry_view.total_len;
            }

            if (entries_[next_offset] == EndMarker) {
                entries_[first_entry.offset] = EndMarker;
                entries_.resize(entries_.size() - (next_offset - first_entry.offset));
                if (first_entry.offset == HeaderSize) {
                    setTail_(HeaderSize);
                } else {
                    setTail_(first_entry.offset - first_entry.prev_len_value);
                }
                setLen_(static_cast<std::uint16_t>(getLen_() - count));
                return;
            }

            auto next_entry = parseEntry_(next_offset);
            auto new_next_prev_size = prevLenSize_(first_entry.prev_len_value);
            bool force_flag = false;
            if (new_next_prev_size < next_entry.prev_len_size) {
                new_next_prev_size = next_entry.prev_len_size;
                force_flag = true;
            }

            auto diff = static_cast<std::ptrdiff_t>(new_next_prev_size - next_entry.prev_len_size);
            std::size_t deleted_bytes = next_offset - first_entry.offset - diff;
            std::size_t move_src = next_offset + next_entry.prev_len_size;
            std::size_t move_dest = first_entry.offset + new_next_prev_size;
            std::size_t move_len = entries_.size() - move_src;

            std::memmove(entries_.data() + move_dest, entries_.data() + move_src, move_len);
            entries_.resize(entries_.size() - deleted_bytes);
            writePrevLen_(first_entry.offset, first_entry.prev_len_value, force_flag);
            if (getTail_() == next_offset) {
                setTail_(first_entry.offset);
            } else {
                setTail_(getTail_() - deleted_bytes);
            }

            setLen_(static_cast<std::uint16_t>(getLen_() - count));
            cascadeUpdate_(first_entry.offset);
        }

        [[nodiscard]] entryView parseEntry_(std::size_t offset) const {
            auto prev_len_offset = offset;
            auto [prev_len_size,prev_len_value] = readPrevLen_(prev_len_offset);

            auto encoding_offset = offset + prev_len_size;
            auto first_byte = std::to_integer<std::uint8_t>(entries_[encoding_offset]);
            std::size_t encoding_size{0};
            std::size_t content_len{0};
            ziplistEntryView value = ziplistEntryView::fromInteger(0);
            if ((first_byte & EncodingTypeMask) == 0xC0) {
                encoding_size = 1;
                if (first_byte >= IntImmMin && first_byte <= IntImmMax) {
                    content_len = 0;
                } else if (first_byte == Int8Marker) {
                    content_len = 1;
                } else if (first_byte == Int16Marker) {
                    content_len = 2;
                } else if (first_byte == Int24Marker) {
                    content_len = 3;
                } else if (first_byte == Int32Marker) {
                    content_len = 4;
                } else if (first_byte == Int64Marker) {
                    content_len = 8;
                }
                auto content_offset = encoding_offset + encoding_size;
                auto int_val = readInteger_(content_offset, first_byte);
                value = ziplistEntryView::fromInteger(int_val);
            } else {
                auto [str_enc_size, str_len] = readStringEncoding_(encoding_offset);
                encoding_size = str_enc_size;
                content_len = str_len;

                auto content_offset = encoding_offset + encoding_size;

                value = ziplistEntryView::fromString(std::string_view{
                    reinterpret_cast<const char*>(entries_.data() + content_offset),
                    content_len
                });
            }
            auto total_len = prev_len_size + encoding_size + content_len;
            auto content_offset = encoding_offset + encoding_size;

            return {offset, prev_len_size, prev_len_value, first_byte, content_offset, content_len, total_len, value};
        }

        void initialize_() {
            entries_.resize(EmptyByteSize);
            std::size_t offset = 0;
            std::uint32_t tail = HeaderSize;
            std::uint16_t len = 0;
            std::memcpy(entries_.data() + offset, &tail, sizeof(tail));
            offset += sizeof(tail);
            std::memcpy(entries_.data() + offset, &len, sizeof(len));
            offset += sizeof(len);
            std::memcpy(entries_.data() + offset, &EndMarker, sizeof(EndMarker));
        }

        [[nodiscard]] std::size_t getTail_() const noexcept {
            std::uint32_t tail_offset{};
            std::memcpy(&tail_offset, entries_.data(), sizeof(tail_offset));
            return tail_offset;
        }

        [[nodiscard]] std::size_t getBytes_() const noexcept {
            return entries_.size();
        }

        void setTail_(std::uint32_t tail_offset) {
            std::memcpy(entries_.data(), &tail_offset, sizeof(std::uint32_t));
        }

        void setLen_(std::uint16_t len) {
            std::memcpy(entries_.data() + sizeof(std::uint32_t), &len, sizeof(len));
        }

        [[nodiscard]] std::size_t getLen_() const noexcept {
            std::uint16_t len{};
            std::memcpy(&len, entries_.data() + sizeof(std::uint32_t), sizeof(len));
            return len;
        }

        [[nodiscard]] static constexpr std::size_t stringEncodingSize_(std::size_t len) noexcept {
            if (len <= ShortStringMaxLength) {
                return 1;
            }
            if (len <= MediumStringMaxLength) {
                return 2;
            }

            return 5;
        }

        void writeStringEncoding_(std::size_t offset, std::size_t len) {
            if (len <= ShortStringMaxLength) {
                auto encoding = static_cast<std::uint8_t>(len);
                std::memcpy(entries_.data() + offset, &encoding, sizeof(encoding));
                return;
            }
            if (len <= MediumStringMaxLength) {
                auto first = static_cast<std::uint8_t>(String14BitMarker | ((len >> 8) & StringLengthMask));
                auto second = static_cast<std::uint8_t>(len & 0xFF);
                std::memcpy(entries_.data() + offset, &first, sizeof(first));
                std::memcpy(entries_.data() + offset + sizeof(first), &second, sizeof(second));
                return;
            }
            auto marker = String32BitMarker;
            auto encoded_len = static_cast<std::uint32_t>(len);
            std::memcpy(entries_.data() + offset, &marker, sizeof(marker));
            std::memcpy(entries_.data() + offset + sizeof(marker), &encoded_len, sizeof(encoded_len));
        }

        [[nodiscard]] std::tuple<std::size_t, std::size_t> readStringEncoding_(std::size_t offset) const {
            auto first = std::to_integer<std::uint8_t>(entries_[offset]);
            auto type = first & EncodingTypeMask;
            if (type == String6BitMarker) {
                return {1, static_cast<std::size_t>(first & StringLengthMask)};
            }
            if (type == String14BitMarker) {
                auto second = std::to_integer<std::uint8_t>(entries_[offset + 1]);
                auto len = (static_cast<std::size_t>(first & StringLengthMask) << 8)
                    | static_cast<std::size_t>(second);
                return {2, len};
            }
            if (type == String32BitMarker) {
                std::uint32_t len{};
                std::memcpy(&len, entries_.data() + offset + sizeof(std::uint8_t), sizeof(len));
                return {5, len};
            }
            assert(false);
            return {};
        }

        [[nodiscard]] static constexpr std::size_t prevLenSize_(std::size_t len) noexcept {
            return len <= PrevLen1BitMaxLength ? 1 : 5;
        }

        void writePrevLen_(std::size_t offset, std::size_t prev_len, bool force_large = false) {
            if (prev_len <= PrevLen1BitMaxLength && !force_large) {
                auto prev_len_int8 = static_cast<std::uint8_t>(prev_len);
                std::memcpy(entries_.data() + offset, &prev_len_int8, sizeof(prev_len_int8));
            } else {
                entries_[offset] = PrevLen5BitMarker;
                auto prev_len_int32 = static_cast<std::uint32_t>(prev_len);
                std::memcpy(entries_.data() + offset + sizeof(PrevLen5BitMarker), &prev_len_int32,
                            sizeof(std::uint32_t));
            }
        }

        [[nodiscard]] std::tuple<std::size_t, std::size_t> readPrevLen_(std::size_t offset) const {
            std::byte first_byte = entries_[offset];
            if (first_byte == PrevLen5BitMarker) {
                std::uint32_t prev_len_value{};
                std::memcpy(&prev_len_value, entries_.data() + offset + sizeof(first_byte), sizeof(prev_len_value));
                return {5, prev_len_value};
            }

            return {1, std::to_integer<std::uint8_t>(first_byte)};
        }

        void cascadeUpdate_(std::size_t offset) {
            while (entries_[offset] != EndMarker) {
                auto current = parseEntry_(offset);
                auto next_offset = offset + current.total_len;
                if (entries_[next_offset] == EndMarker) {
                    break;
                }
                auto next = parseEntry_(next_offset);
                auto new_next_prev_len_size = prevLenSize_(current.total_len);
                if (new_next_prev_len_size == next.prev_len_size) {
                    writePrevLen_(next_offset, current.total_len);
                    break;
                }
                if (next.prev_len_size < new_next_prev_len_size) {
                    auto diff = static_cast<std::ptrdiff_t>(new_next_prev_len_size - next.prev_len_size);
                    std::size_t old_total_size = entries_.size();
                    entries_.resize(old_total_size + diff);
                    std::size_t move_src = next_offset + next.prev_len_size;
                    std::size_t move_dest = next_offset + new_next_prev_len_size;
                    std::size_t move_len = old_total_size - move_src;

                    std::memmove(entries_.data() + move_dest, entries_.data() + move_src, move_len);

                    writePrevLen_(next_offset, current.total_len);

                    if (getTail_() != next_offset) {
                        setTail_(getTail_() + diff);
                    }


                    offset = next_offset;
                } else {
                    writePrevLen_(next_offset, current.total_len, true);
                    break;
                }
            }
        }

    private:
        std::vector<std::byte> entries_;
    };
}
