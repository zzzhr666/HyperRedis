#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <vector>
#include <cassert>

namespace hyper {
    class intset {
    public:
        enum class Encoding {
            Int16,
            Int32,
            Int64
        };

        intset() : encoding_(Encoding::Int16), length_(0) {}

        [[nodiscard]] std::size_t size() const noexcept {
            return length_;
        }

        [[nodiscard]] bool empty() const noexcept {
            return length_ == 0;
        }

        [[nodiscard]] Encoding encoding() const noexcept {
            return encoding_;
        }

        [[nodiscard]] bool contains(std::int64_t value) const {
            if (encodingGreaterThan_(valueEncoding_(value), encoding_)) {
                return false;
            }
            std::size_t pos{0};
            return search_(value, pos);
        }

        [[nodiscard]] std::int64_t getAt(std::size_t index) const {
            assert(index < length_);
            return get_(index);
        }

        bool insert(std::int64_t value) {
            if (encodingGreaterThan_(valueEncoding_(value), encoding_)) {
                upgradeAndInsert_(value);
                return true;
            }
            std::size_t pos{};
            if (search_(value, pos)) {
                return false;
            }
            std::size_t old_len = length_;
            resize_(old_len + 1);
            for (std::size_t i = old_len; i > pos; --i) {
                set_(i, get_(i - 1));
            }

            set_(pos, value);
            return true;
        }

        bool erase(std::int64_t value) {
            if (encodingGreaterThan_(valueEncoding_(value), encoding_)) {
                return false;
            }

            std::size_t pos{};
            if (!search_(value, pos)) {
                return false;
            }
            for (std::size_t i = pos; i < length_ - 1; ++i) {
                set_(i, get_(i + 1));
            }
            resize_(length_ - 1);
            return true;
        }

        void clear() {
            contents_.clear();
            length_ = 0;
            encoding_ = Encoding::Int16;
        }

        template <typename Func>
            requires std::invocable<Func, std::int64_t>
        void forEach(Func&& func) const {
            for (std::size_t i = 0; i < length_; ++i) {
                func(get_(i));
            }
        }

        [[nodiscard]] std::size_t byteSize() const noexcept {
            return contents_.size();
        }

    private:
        void resize_(std::size_t new_size) {
            contents_.resize(new_size * encodingSize_(encoding_));
            length_ = new_size;
        }

        static constexpr std::size_t encodingSize_(const Encoding encoding) noexcept {
            switch (encoding) {
            case Encoding::Int16:
                return sizeof(std::int16_t);
            case Encoding::Int32:
                return sizeof(std::int32_t);
            case Encoding::Int64:
                return sizeof(std::int64_t);
            }
            return sizeof(std::int64_t);
        }

        [[nodiscard]] std::int64_t get_(std::size_t index) const {
            return readWithEncoding_(index, encoding_);
        }

        void set_(std::size_t index, std::int64_t value) {
            std::size_t offset = index * encodingSize_(encoding_);
            switch (encoding_) {
            case Encoding::Int16: {
                const auto encoded = static_cast<std::int16_t>(value);
                std::memcpy(contents_.data() + offset, &encoded, sizeof(encoded));
                return;
            }
            case Encoding::Int32: {
                const auto encoded = static_cast<std::int32_t>(value);
                std::memcpy(contents_.data() + offset, &encoded, sizeof(encoded));
                return;
            }
            case Encoding::Int64: {
                const auto encoded = static_cast<std::int64_t>(value);
                std::memcpy(contents_.data() + offset, &encoded, sizeof(encoded));
                return;
            }
            }
        }

        bool search_(std::int64_t value, std::size_t& pos) const {
            std::size_t left = 0;
            std::size_t right = length_;
            while (left < right) {
                std::size_t mid = left + (right - left) / 2;
                std::int64_t current_value = get_(mid);
                if (current_value == value) {
                    pos = mid;
                    return true;
                }
                if (current_value < value) {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
            pos = left;
            return false;
        }

        static constexpr Encoding valueEncoding_(const std::int64_t value) noexcept {
            if (value < std::numeric_limits<std::int32_t>::min() || value > std::numeric_limits<std::int32_t>::max()) {
                return Encoding::Int64;
            }
            if (value < std::numeric_limits<std::int16_t>::min() || value > std::numeric_limits<std::int16_t>::max()) {
                return Encoding::Int32;
            }
            return Encoding::Int16;
        }

        static constexpr bool encodingGreaterThan_(const Encoding lhs, const Encoding rhs) noexcept {
            return encodingSize_(lhs) > encodingSize_(rhs);
        }


        [[nodiscard]] std::int64_t readWithEncoding_(std::size_t index, Encoding encoding) const {
            std::size_t offset = encodingSize_(encoding) * index;
            switch (encoding) {
            case Encoding::Int16: {
                std::int16_t value{};
                std::memcpy(&value, contents_.data() + offset, sizeof(value));
                return value;
            }
            case Encoding::Int32: {
                std::int32_t value{};
                std::memcpy(&value, contents_.data() + offset, sizeof(value));
                return value;
            }
            case Encoding::Int64: {
                std::int64_t value{};
                std::memcpy(&value, contents_.data() + offset, sizeof(value));
                return value;
            }
            }
            return 0;
        }

        void upgradeAndInsert_(std::int64_t value) {
            auto old_encoding = encoding_;
            encoding_ = valueEncoding_(value);
            std::size_t old_len = length_;
            resize_(old_len + 1);

            if (value < 0) {
                for (std::size_t i = old_len; i > 0; --i) {
                    set_(i, readWithEncoding_(i - 1, old_encoding));
                }
                set_(0, value);
            } else {
                for (std::size_t i = old_len; i > 0; --i) {
                    set_(i - 1, readWithEncoding_(i - 1, old_encoding));
                }
                set_(old_len, value);
            }
        }

    private:
        Encoding encoding_;
        std::size_t length_;
        std::vector<std::byte> contents_;
    };
}
