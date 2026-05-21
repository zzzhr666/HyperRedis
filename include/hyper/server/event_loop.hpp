#pragma once
#include <chrono>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <utility>
#include <unordered_map>


namespace hyper {
    enum class FileEventMask : std::uint8_t {
        None = 0,
        Readable = 1 << 0,
        Writable = 1 << 1
    };

    [[nodiscard]] constexpr auto toUnderlying(FileEventMask mask) noexcept {
        return static_cast<std::underlying_type_t<FileEventMask>>(mask);
    }

    [[nodiscard]] constexpr FileEventMask operator|(FileEventMask lhs, FileEventMask rhs) noexcept {
        return static_cast<FileEventMask>(toUnderlying(lhs) | toUnderlying(rhs));
    }

    [[nodiscard]] constexpr FileEventMask operator&(FileEventMask lhs, FileEventMask rhs) noexcept {
        return static_cast<FileEventMask>(toUnderlying(lhs) & toUnderlying(rhs));
    }

    constexpr FileEventMask& operator|=(FileEventMask& lhs, FileEventMask rhs) noexcept {
        lhs = lhs | rhs;
        return lhs;
    }

    [[nodiscard]] constexpr bool hasAny(FileEventMask mask, FileEventMask target) noexcept {
        return (mask & target) != FileEventMask::None;
    }

    [[nodiscard]] constexpr FileEventMask without(FileEventMask mask, FileEventMask removed) noexcept {
        return static_cast<FileEventMask>(toUnderlying(mask) & ~toUnderlying(removed));
    }

    class EventLoop {
    public:
        using FileCallback = std::function<void(int, FileEventMask)>;


        EventLoop();
        bool addFileEvent(int fd, FileEventMask mask, const FileCallback& callback);

        void removeFileEvent(int fd, FileEventMask mask);
        int runOnce(std::chrono::milliseconds timeout);


        bool isStopped() const noexcept {
            return stopped_;
        }

        void stop() noexcept {
            stopped_ = true;
        }

        ~EventLoop() = default;

    private:
        struct FileEvent {
            FileEventMask mask{FileEventMask::None};
            FileCallback readable_callback;
            FileCallback writable_callback;
        };

        std::unordered_map<int, FileEvent> file_events_;
        bool stopped_;
    };
}
