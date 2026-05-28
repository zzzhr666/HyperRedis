#include "hyper/server/event_loop.hpp"

#include <cassert>
#include <limits>
#include <poll.h>
#include <ranges>
#include <unistd.h>
#include <vector>
#include <sys/epoll.h>

namespace {
    [[maybe_unused]] short toPollEvents(hyper::FileEventMask mask) noexcept {
        short event{};
        if (hyper::hasAny(mask, hyper::FileEventMask::Readable)) {
            event |= POLLIN;
        }
        if (hyper::hasAny(mask, hyper::FileEventMask::Writable)) {
            event |= POLLOUT;
        }

        return event;
    }

    [[maybe_unused]] hyper::FileEventMask fromPollRevents(short revents) noexcept {
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

    std::uint32_t toEpollEvents(hyper::FileEventMask mask) {
        std::uint32_t events{0};
        if (hyper::hasAny(mask, hyper::FileEventMask::Readable)) {
            events |= EPOLLIN;
        }
        if (hyper::hasAny(mask, hyper::FileEventMask::Writable)) {
            events |= EPOLLOUT;
        }
        return events;
    }

    hyper::FileEventMask fromEpollRevent(std::uint32_t revent) {
        auto mask = hyper::FileEventMask::None;
        if (revent & EPOLLIN) {
            mask |= hyper::FileEventMask::Readable;
        }
        if (revent & EPOLLOUT) {
            mask |= hyper::FileEventMask::Writable;
        }
        if (revent & (EPOLLERR | EPOLLHUP)) {
            mask |= hyper::FileEventMask::Readable | hyper::FileEventMask::Writable;
        }

        return mask;
    }
}

hyper::EventLoop::EventLoop()
    : next_time_event_id_(1), epoll_fd_(-1), stopped_(false) {
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ == -1) {
        assert(false);
    }
}

bool hyper::EventLoop::addFileEvent(int fd, FileEventMask mask, const FileCallback& callback) {
    if (fd < 0 || mask == FileEventMask::None || !callback) {
        return false;
    }
    auto it = file_events_.find(fd);
    bool is_new = it == file_events_.end();
    FileEvent old_event = is_new ? FileEvent{} : it->second;
    auto& event = file_events_[fd];
    FileEventMask new_mask = event.mask | mask;
    event.mask |= mask;
    if (hasAny(mask, FileEventMask::Readable)) {
        event.readable_callback = callback;
    }
    if (hasAny(mask, FileEventMask::Writable)) {
        event.writable_callback = callback;
    }
    epoll_event ee{};
    ee.events = toEpollEvents(new_mask);
    ee.data.fd = fd;
    int op = is_new ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
    if (::epoll_ctl(epoll_fd_, op, fd, &ee) < 0) {
        if (is_new) {
            file_events_.erase(fd);
        } else {
            file_events_[fd] = old_event;
        }
        return false;
    }
    return true;
}

void hyper::EventLoop::removeFileEvent(int fd, FileEventMask mask) {
    auto it = file_events_.find(fd);
    if (it == file_events_.end()) {
        return;
    }
    auto event = it->second;
    auto& [current_mask,r_cb,w_cb] = event;
    if (hasAny(mask, FileEventMask::Readable)) {
        r_cb = nullptr;
    }
    if (hasAny(mask, FileEventMask::Writable)) {
        w_cb = nullptr;
    }
    current_mask = without(current_mask, mask);

    epoll_event ee{};
    ee.events = toEpollEvents(current_mask);
    ee.data.fd = fd;
    bool need_del = current_mask == FileEventMask::None;
    int op = need_del ? EPOLL_CTL_DEL : EPOLL_CTL_MOD;
    if (::epoll_ctl(epoll_fd_, op, fd, &ee) < 0) {
        return;
    }
    if (need_del) {
        file_events_.erase(it);
    } else {
        it->second = event;
    }
}

int hyper::EventLoop::runOnce(std::chrono::milliseconds timeout) {
    if (isStopped()) {
        return 0;
    }
    auto epoll_timeout = nearestTimeEventTimeout_(timeout);
    int timeout_ms = toPollTimeout(epoll_timeout);

    constexpr std::size_t MAX_EVENTS = 1024;
    std::array<epoll_event, MAX_EVENTS> events{};
    int ready = ::epoll_wait(epoll_fd_, events.data(), MAX_EVENTS, timeout_ms);
    if (ready < 0) {
        return (errno == EINTR) ? 0 : ready;
    }
    if (ready == 0) {
        return static_cast<int>(processDueTimeEvents_());
    }
    std::size_t processed_count{0};
    for (int i = 0; i < ready; ++i) {
        int fd = events[i].data.fd;
        auto revents = events[i].events;
        auto mask = fromEpollRevent(revents);
        auto it = file_events_.find(fd);
        if (it == file_events_.end()) {
            continue;
        }
        mask = mask & it->second.mask;
        if (mask == FileEventMask::None) {
            continue;
        }
        bool fire{false};
        if (hasAny(mask, FileEventMask::Readable) && it->second.readable_callback) {
            it->second.readable_callback(fd, FileEventMask::Readable);
            fire = true;
        }

        it = file_events_.find(fd);
        if (it != file_events_.end() && hasAny(mask, FileEventMask::Writable) && it->second.writable_callback) {
            it->second.writable_callback(fd, FileEventMask::Writable);
            fire = true;
        }
        if (fire) {
            ++processed_count;
        }
    }

    processed_count += processDueTimeEvents_();
    return static_cast<int>(processed_count);
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

hyper::EventLoop::~EventLoop() {
    if (epoll_fd_ != -1) {
        ::close(epoll_fd_);
    }
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
    for (const auto& time_event : time_events_ | std::views::values) {
        const auto ddl = time_event.deadline;
        nearest_timepoint = std::min(nearest_timepoint, ddl);
        if (ddl <= now) {
            return std::chrono::milliseconds{0};
        }
    }

    auto remaining = std::chrono::ceil<std::chrono::milliseconds>(nearest_timepoint - now);
    if (timeout.count() < 0) {
        return remaining;
    }
    return std::min(timeout, remaining);
}
