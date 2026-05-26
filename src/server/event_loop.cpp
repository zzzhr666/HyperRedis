#include "hyper/server/event_loop.hpp"

#include <limits>
#include <poll.h>
#include <ranges>
#include <vector>

namespace {
    short toPollEvents(hyper::FileEventMask mask) noexcept {
        short event{};
        if (hyper::hasAny(mask, hyper::FileEventMask::Readable)) {
            event |= POLLIN;
        }
        if (hyper::hasAny(mask, hyper::FileEventMask::Writable)) {
            event |= POLLOUT;
        }

        return event;
    }

    hyper::FileEventMask fromPollRevents(short revents) noexcept {
        hyper::FileEventMask mask = hyper::FileEventMask::None;
        if ((revents & POLLIN) || (revents & POLLHUP) || (revents & POLLERR) || (revents & POLLNVAL)) {
            mask |= hyper::FileEventMask::Readable;
        }
        if (revents & POLLOUT) {
            mask |= hyper::FileEventMask::Writable;
        }
        return mask;
    }

    int toPollTimeout(std::chrono::milliseconds timeout) noexcept {
        const auto count = timeout.count();
        if (count < -1) {
            return -1;
        }
        if (count > std::numeric_limits<int>::max()) {
            return std::numeric_limits<int>::max();
        }
        return static_cast<int>(count);
    }
}

hyper::EventLoop::EventLoop() : next_time_event_id_(1), stopped_(false) {}

bool hyper::EventLoop::addFileEvent(int fd, FileEventMask mask, const FileCallback& callback) {
    if (fd < 0 || mask == FileEventMask::None || !callback) {
        return false;
    }
    auto& event = file_events_[fd];
    event.mask |= mask;
    if (hasAny(mask, FileEventMask::Readable)) {
        event.readable_callback = callback;
    }
    if (hasAny(mask, FileEventMask::Writable)) {
        event.writable_callback = callback;
    }
    return true;
}

void hyper::EventLoop::removeFileEvent(int fd, FileEventMask mask) {
    if (auto it = file_events_.find(fd); it != file_events_.end()) {
        auto& [current_mask,r_cb,w_cb] = it->second;
        if (hasAny(mask, FileEventMask::Readable)) {
            r_cb = nullptr;
        }
        if (hasAny(mask, FileEventMask::Writable)) {
            w_cb = nullptr;
        }
        current_mask = without(current_mask, mask);
        if (current_mask == FileEventMask::None) {
            file_events_.erase(it);
        }
    }
}

int hyper::EventLoop::runOnce(std::chrono::milliseconds timeout) {
    if (isStopped()) {
        return 0;
    }

    std::vector<pollfd> poll_fds;
    poll_fds.reserve(file_events_.size());
    for (const auto& [fd, file_event] : file_events_) {
        pollfd poll_fd{};
        poll_fd.fd = fd;
        poll_fd.events = toPollEvents(file_event.mask);
        poll_fds.push_back(poll_fd);
    }
    auto poll_timeout = nearestTimeEventTimeout_(timeout);
    int ready = poll(poll_fds.data(), poll_fds.size(), toPollTimeout(poll_timeout));
    if (ready < 0) {
        return ready;
    }
    if (ready == 0) {
        return static_cast<int>(processDueTimeEvents_());
    }

    std::size_t count{0};
    for (auto& poll_fd : poll_fds) {
        auto mask = fromPollRevents(poll_fd.revents);
        if (mask == FileEventMask::None) {
            continue;
        }
        auto it = file_events_.find(poll_fd.fd);
        if (it == file_events_.end()) {
            continue;
        }
        mask = mask & it->second.mask;
        if (mask == FileEventMask::None) {
            continue;
        }

        bool fired = false;
        if (hasAny(mask, FileEventMask::Readable)) {
            if (auto& callback = it->second.readable_callback) {
                callback(poll_fd.fd, FileEventMask::Readable);
                fired = true;
            }
        }

        it = file_events_.find(poll_fd.fd);
        if (it != file_events_.end() && hasAny(mask, FileEventMask::Writable)) {
            if (auto& callback = it->second.writable_callback) {
                callback(poll_fd.fd, FileEventMask::Writable);
                fired = true;
            }
        }

        if (fired) {
            ++count;
        }
    }
    count += processDueTimeEvents_();
    return static_cast<int>(count);
}

std::optional<hyper::EventLoop::TimeEventId> hyper::EventLoop::addTimeEvent(std::chrono::milliseconds delay,
                                                                            const TimeCallback& callback) {
    if (!callback) {
        return std::nullopt;
    }
    TimeEventId id = next_time_event_id_++;
    if (delay < std::chrono::milliseconds{0}) {
        delay = std::chrono::milliseconds{0};
    }
    time_events_.emplace(id, TimeEvent{TimeClock::now() + delay, callback});
    return id;
}

bool hyper::EventLoop::removeTimeEvent(TimeEventId id) {
    if (auto it = time_events_.find(id); it != time_events_.end()) {
        time_events_.erase(it);
        return true;
    }
    return false;
}

size_t hyper::EventLoop::processDueTimeEvents_() {
    auto now = TimeClock::now();
    std::vector<TimeEventId> ids;
    for (const auto& [id,time_event] : time_events_) {
        auto& deadline = time_event.deadline;
        if (deadline <= now) {
            ids.emplace_back(id);
        }
    }
    std::size_t fired{0};
    for (const auto id : ids) {
        auto it = time_events_.find(id);
        if (it == time_events_.end()) {
            continue;
        }
        auto callback = it->second.callback;
        auto next_delay_opt = callback();
        ++fired;
        auto after_it = time_events_.find(id);
        if (after_it == time_events_.end()) {
            continue;
        }
        if (next_delay_opt.has_value()) {
            after_it->second.deadline = TimeClock::now() + next_delay_opt.value();
        } else {
            time_events_.erase(after_it);
        }
    }

    return fired;
}

std::chrono::milliseconds hyper::EventLoop::nearestTimeEventTimeout_(std::chrono::milliseconds timeout) const {
    if (time_events_.empty()) {
        return timeout;
    }
    auto now = TimeClock::now();
    TimeClock::time_point nearest_timepoint{time_events_.begin()->second.deadline};
    for (const auto& time_event: time_events_ | std::views::values) {
        const auto ddl = time_event.deadline;
        nearest_timepoint = std::min(nearest_timepoint,ddl);
        if (ddl <= now) {
            return std::chrono::milliseconds{0};
        }
    }

    auto remaining = std::chrono::ceil<std::chrono::milliseconds>(nearest_timepoint - now);
    if (timeout.count() < 0) {
        return remaining;
    }
    return std::min(timeout,remaining);
}
