#include "hyper/storage/aof_appender.hpp"

#include <array>
#include <cassert>
#include <cerrno>
#include <fcntl.h>
#include <string_view>
#include <unistd.h>
#include <optional>

#include "hyper/server/resp_codec.hpp"

namespace {
    std::string serializeSelectCommand(std::size_t db_index) {
        const std::string index = std::to_string(db_index);
        const std::array<std::string_view, 2> args{"SELECT", index};
        return hyper::serializeRespCommand(args);
    }
}

std::string hyper::policyToString(AofFsyncPolicy policy) {
    switch (policy) {
    case AofFsyncPolicy::No:
        return "no";
    case AofFsyncPolicy::Always:
        return "always";
    case AofFsyncPolicy::EverySecond:
        return "everysec";
    default:
        return "unknown policy";
    }
}

hyper::AofAppender::AofAppender(std::filesystem::path path, AofFsyncPolicy policy)
    : path_(std::move(path)), selected_db_index_(0), broken_(false),
      fsync_policy_(policy), fsync_pending_(false), fd_(-1) {}

hyper::AofAppender::~AofAppender() {
    if (fd_ != -1) {
        ::close(fd_);
    }
}

bool hyper::AofAppender::appendCommand(std::size_t db_index, std::span<const std::string_view> args,
                                       ExpireTimePoint now) {
    if (broken_) {
        return false;
    }

    std::string bytes{};
    const bool index_changed = db_index != selected_db_index_;
    if (index_changed) {
        bytes.append(serializeSelectCommand(db_index));
    }
    bytes.append(serializeRespCommand(args));
    if (!appendBytes_(bytes)) {
        closeFd_();
        broken_ = true;
        return false;
    }
    if (fsync_policy_ == AofFsyncPolicy::Always) {
        if (::fsync(fd_) == -1) {
            closeFd_();
            broken_ = true;
            return false;
        }
    } else if (fsync_policy_ == AofFsyncPolicy::EverySecond) {
        fsync_pending_ = true;
        if (!syncIfDue_(now)) {
            closeFd_();
            broken_ = true;
            return false;
        }
    }

    if (index_changed) {
        selected_db_index_ = db_index;
    }
    return true;
}


bool hyper::AofAppender::flushIfNeeded(ExpireTimePoint now) {
    if (broken_) {
        return false;
    }
    if (!syncIfDue_(now)) {
        closeFd_();
        broken_ = true;
        return false;
    }
    return true;
}

bool hyper::AofAppender::syncIfDue_(ExpireTimePoint now) {
    if (!fsync_pending_) {
        return true;
    }
    if (!last_fsync_time_.has_value()) {
        last_fsync_time_ = now;
        return true;
    }
    if ( now - last_fsync_time_.value() < std::chrono::seconds(1)) {
        return true;
    }
    int ret = ::fsync(fd_);

    if (ret == -1) {
        return false;
    }
    last_fsync_time_ = now;
    fsync_pending_ = false;
    return true;
}

bool hyper::AofAppender::appendBytes_(std::string_view bytes) {
    if (fd_ == -1) {
        fd_ = ::open(path_.c_str(),O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd_ == -1) {
            return false;
        }
    }

    ssize_t count{0};
    while (count < static_cast<ssize_t>(bytes.size())) {
        auto wrote_num = ::write(fd_, bytes.data() + count, bytes.size() - count);
        if (wrote_num == -1) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (wrote_num == 0) {
            return false;
        }
        count += wrote_num;
    }
    return true;
}
void hyper::AofAppender::closeFd_() {
    if (fd_ != -1) {
        ::close(fd_);
        fd_ = -1;
    }
}
