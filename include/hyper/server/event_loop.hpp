#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <utility>


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
        using TimeEventId = std::uint64_t;
        using TimeCallback = std::function<std::optional<std::chrono::milliseconds>()>;

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

        std::optional<TimeEventId> addTimeEvent(std::chrono::milliseconds delay, const TimeCallback& callback);

        bool removeTimeEvent(TimeEventId id);

        ~EventLoop() = default;

    private:
        size_t processDueTimeEvents_();

        std::chrono::milliseconds nearestTimeEventTimeout_(std::chrono::milliseconds timeout) const;

    private:
        struct FileEvent {
            FileEventMask mask{FileEventMask::None};
            FileCallback readable_callback;
            FileCallback writable_callback;
        };

        using TimeClock = std::chrono::steady_clock;

        struct TimeEvent {
            TimeClock::time_point deadline;
            TimeCallback callback;
        };

        std::unordered_map<int, FileEvent> file_events_;
        std::unordered_map<TimeEventId, TimeEvent> time_events_;
        TimeEventId next_time_event_id_;
        bool stopped_;
    };
}
