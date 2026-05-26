#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <optional>
#include <unistd.h>

#include "hyper/server/event_loop.hpp"

using namespace hyper;

namespace {
    class Pipe {
    public:
        Pipe() {
            EXPECT_EQ(pipe(fds_.data()), 0);
        }

        Pipe(const Pipe&) = delete;
        Pipe& operator=(const Pipe&) = delete;

        ~Pipe() {
            if (fds_[0] >= 0) {
                close(fds_[0]);
            }
            if (fds_[1] >= 0) {
                close(fds_[1]);
            }
        }

        [[nodiscard]] int readFd() const noexcept {
            return fds_[0];
        }

        [[nodiscard]] int writeFd() const noexcept {
            return fds_[1];
        }

        void closeRead() noexcept {
            if (fds_[0] >= 0) {
                close(fds_[0]);
                fds_[0] = -1;
            }
        }

    private:
        std::array<int, 2> fds_{{-1, -1}};
    };
}

TEST(EventLoopTest, ReadableEventFiresWhenPipeHasData) {
    EventLoop loop;
    Pipe pipe;
    int calls{0};
    int callback_fd{-1};
    FileEventMask callback_mask{FileEventMask::None};

    ASSERT_TRUE(loop.addFileEvent(pipe.readFd(), FileEventMask::Readable,
                                  [&](int fd, FileEventMask mask) {
                                      ++calls;
                                      callback_fd = fd;
                                      callback_mask = mask;
                                  }));

    const char byte = 'x';
    ASSERT_EQ(write(pipe.writeFd(), &byte, 1), 1);

    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 1);
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(callback_fd, pipe.readFd());
    EXPECT_EQ(callback_mask, FileEventMask::Readable);
}

TEST(EventLoopTest, RemovedReadableEventDoesNotFire) {
    EventLoop loop;
    Pipe pipe;
    int calls{0};

    ASSERT_TRUE(loop.addFileEvent(pipe.readFd(), FileEventMask::Readable,
                                  [&](int, FileEventMask) {
                                      ++calls;
                                  }));
    loop.removeFileEvent(pipe.readFd(), FileEventMask::Readable);

    const char byte = 'x';
    ASSERT_EQ(write(pipe.writeFd(), &byte, 1), 1);

    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 0);
    EXPECT_EQ(calls, 0);
}

TEST(EventLoopTest, RemovingReadableKeepsWritableEventForSameFd) {
    EventLoop loop;
    Pipe pipe;
    int readable_calls{0};
    int writable_calls{0};
    FileEventMask writable_mask{FileEventMask::None};

    ASSERT_TRUE(loop.addFileEvent(pipe.writeFd(), FileEventMask::Readable,
                                  [&](int, FileEventMask) {
                                      ++readable_calls;
                                  }));
    ASSERT_TRUE(loop.addFileEvent(pipe.writeFd(), FileEventMask::Writable,
                                  [&](int, FileEventMask mask) {
                                      ++writable_calls;
                                      writable_mask = mask;
                                  }));

    loop.removeFileEvent(pipe.writeFd(), FileEventMask::Readable);

    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 1);
    EXPECT_EQ(readable_calls, 0);
    EXPECT_EQ(writable_calls, 1);
    EXPECT_EQ(writable_mask, FileEventMask::Writable);
}

TEST(EventLoopTest, ReadableCallbackCanRemoveWritableBeforeItFires) {
    EventLoop loop;
    Pipe pipe;
    int readable_calls{0};
    int writable_calls{0};

    ASSERT_TRUE(loop.addFileEvent(pipe.writeFd(), FileEventMask::Readable,
                                  [&](int fd, FileEventMask) {
                                      ++readable_calls;
                                      loop.removeFileEvent(fd, FileEventMask::Writable);
                                  }));
    ASSERT_TRUE(loop.addFileEvent(pipe.writeFd(), FileEventMask::Writable,
                                  [&](int, FileEventMask) {
                                      ++writable_calls;
                                  }));

    pipe.closeRead();

    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 1);
    EXPECT_EQ(readable_calls, 1);
    EXPECT_EQ(writable_calls, 0);
}

TEST(EventLoopTest, TimeEventFiresWhenDelayExpires) {
    EventLoop loop;
    int calls{0};

    const auto id = loop.addTimeEvent(std::chrono::milliseconds{0},
                                      [&]() -> std::optional<std::chrono::milliseconds> {
                                          ++calls;
                                          return std::nullopt;
                                      });

    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 1);
    EXPECT_EQ(calls, 1);

    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 0);
    EXPECT_EQ(calls, 1);
}

TEST(EventLoopTest, TimeEventDoesNotFireBeforeDelay) {
    EventLoop loop;
    int calls{0};

    const auto id = loop.addTimeEvent(std::chrono::milliseconds{20},
                                      [&]() -> std::optional<std::chrono::milliseconds> {
                                          ++calls;
                                          return std::nullopt;
                                      });

    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 0);
    EXPECT_EQ(calls, 0);

    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{100}), 1);
    EXPECT_EQ(calls, 1);
}

TEST(EventLoopTest, TimeEventShortensPollTimeout) {
    EventLoop loop;
    int calls{0};

    const auto id = loop.addTimeEvent(std::chrono::milliseconds{20},
                                      [&]() -> std::optional<std::chrono::milliseconds> {
                                          ++calls;
                                          return std::nullopt;
                                      });

    ASSERT_TRUE(id.has_value());

    const auto start = std::chrono::steady_clock::now();
    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{200}), 1);
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_EQ(calls, 1);
    EXPECT_LT(elapsed, std::chrono::milliseconds{150});
}

TEST(EventLoopTest, RepeatingTimeEventUsesReturnedDelay) {
    EventLoop loop;
    int calls{0};

    const auto id = loop.addTimeEvent(std::chrono::milliseconds{0},
                                      [&]() -> std::optional<std::chrono::milliseconds> {
                                          ++calls;
                                          if (calls < 3) {
                                              return std::chrono::milliseconds{0};
                                          }
                                          return std::nullopt;
                                      });

    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 1);
    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 1);
    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 1);
    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 0);
    EXPECT_EQ(calls, 3);
}

TEST(EventLoopTest, RemovedTimeEventDoesNotFire) {
    EventLoop loop;
    int calls{0};

    const auto id = loop.addTimeEvent(std::chrono::milliseconds{0},
                                      [&]() -> std::optional<std::chrono::milliseconds> {
                                          ++calls;
                                          return std::nullopt;
                                      });

    ASSERT_TRUE(id.has_value());
    loop.removeTimeEvent(*id);

    EXPECT_EQ(loop.runOnce(std::chrono::milliseconds{0}), 0);
    EXPECT_EQ(calls, 0);
}
