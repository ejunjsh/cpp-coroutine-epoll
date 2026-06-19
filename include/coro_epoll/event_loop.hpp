#pragma once

#include "coro_epoll/task.hpp"

#if defined(__linux__)
#include <sys/epoll.h>
#include <sys/eventfd.h>
#elif defined(__APPLE__)
#include <fcntl.h>
#include <sys/event.h>
#else
#include <sys/time.h>
#error "coro_epoll::EventLoop currently supports Linux epoll and macOS kqueue"
#endif
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <coroutine>
#include <cstdint>
#include <string>
#include <functional>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

namespace coro_epoll {

class EventLoop {
public:
    EventLoop() {
#if defined(__linux__)
        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ < 0) {
            throw std::system_error(errno, std::generic_category(), "epoll_create1");
        }

        wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EPOLL_CLOEXEC);
        if (wakeup_fd_ < 0) {
            const int saved_errno = errno;
            ::close(epoll_fd_);
            epoll_fd_ = -1;
            throw std::system_error(saved_errno, std::generic_category(), "eventfd");
        }

        try {
            add_wakeup_fd();
        } catch (...) {
            ::close(wakeup_fd_);
            ::close(epoll_fd_);
            wakeup_fd_ = -1;
            epoll_fd_ = -1;
            throw;
        }

#elif defined(__APPLE__)
        kqueue_fd_ = ::kqueue();
        if (kqueue_fd_ < 0) {
            throw std::system_error(errno, std::generic_category(), "kqueue");
        }

        if (::pipe(wakeup_pipe_) < 0) {
            const int saved_errno = errno;
            ::close(kqueue_fd_);
            kqueue_fd_ = -1;
            throw std::system_error(saved_errno, std::generic_category(), "pipe");
        }

        try {
            set_non_blocking(wakeup_read_fd());
            set_non_blocking(wakeup_write_fd());
            set_close_on_exec(wakeup_read_fd());
            set_close_on_exec(wakeup_write_fd());
            add_wakeup_fd();
        } catch (...) {
            ::close(wakeup_read_fd());
            ::close(wakeup_write_fd());
            wakeup_pipe_[0] = -1;
            wakeup_pipe_[1] = -1;
            kqueue_fd_ = -1;
            throw;
        }
#endif
    }

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    ~EventLoop() {
#if defined(__linux__)
        if (wakeup_fd_ >= 0) {
            ::close(wakeup_fd_);
        }
        if (epoll_fd_ >= 0) {
            ::close(epoll_fd_);
        }
#elif defined(__APPLE__)
        if (wakeup_read_fd() >= 0) {
            ::close(wakeup_read_fd());
        }
        if (wakeup_write_fd() >= 0) {
            ::close(wakeup_write_fd());
        }
        if (kqueue_fd_ >= 0) {
            ::close(kqueue_fd_);
        }
#endif
    }

    void spawn(Task<void>&& task) {
        auto& stored = tasks_.emplace_back(std::move(task));
        stored.resume();
    }

    void post(std::function<void()> task) {
        std::lock_guard lock{pending_mutex_};
        pending_tasks_.push(std::move(task));
        wakeup();
    }

    void run() {
#if defined(__linux__)
        std::vector<epoll_event> events(128);
#elif defined(__APPLE__)
        std::vector<struct kevent> events(128);
#endif
        while (!stopped_) {
            drain_pending_tasks();
            compact_finished_tasks();

#if defined(__linux__)
            const int count = ::epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), -1);
            if (count < 0) {
                if (errno == EINTR) {
                    continue;
                }
                throw std::system_error(errno, std::generic_category(), "epoll_wait");
            }
#elif defined(__APPLE__)
            const int count = ::kevent(kqueue_fd_, nullptr, 0, events.data(), static_cast<int>(events.size()), nullptr);
            if (count < 0) {
                if (errno == EINTR) {
                    continue;
                }
                throw std::system_error(errno, std::generic_category(), "kevent_wait");
            }
#endif

            for (int i = 0; i < count; ++i) {
                dispatch(events[static_cast<std::size_t>(i)]);
            }
            drain_pending_tasks();
            compact_finished_tasks();
        }
    }

    void stop() noexcept {
        stopped_ = true;
        wakeup();
    }

    struct ReadinessAwaiter {
        EventLoop& loop;
        int fd;
        bool edge_trigger;

#if defined(__linux__)
        std::uint32_t event;
#elif defined(__APPLE__)
        int16_t event;
#endif

        bool await_ready() const noexcept {
            return false;
        }

        void await_suspend(std::coroutine_handle<> handle) {
            if (edge_trigger) {
                loop.add_interest_et(fd, event, handle);
            } else {
                loop.add_interest(fd, event, handle);
            }
        }

        void await_resume() const noexcept {}
    };

    ReadinessAwaiter readable(int fd) noexcept {
#if defined(__linux__)
        return ReadinessAwaiter{*this, fd, false, EPOLLIN};
#elif defined(__APPLE__)
        return ReadinessAwaiter{*this, fd, false, EVFILT_READ};
#endif
    }

    ReadinessAwaiter writable(int fd) noexcept {
#if defined(__linux__)
        return ReadinessAwaiter{*this, fd, false, EPOLLOUT};
#elif defined(__APPLE__)
        return ReadinessAwaiter{*this, fd, false, EVFILT_WRITE};
#endif
    }

    ReadinessAwaiter readable_et(int fd) noexcept {
#if defined(__linux__)
        return ReadinessAwaiter{*this, fd, true, EPOLLIN};
#elif defined(__APPLE__)
        return ReadinessAwaiter{*this, fd, true, EVFILT_READ};
#endif
    }

    ReadinessAwaiter writable_et(int fd) noexcept {
#if defined(__linux__)
        return ReadinessAwaiter{*this, fd, true, EPOLLOUT};
#elif defined(__APPLE__)
        return ReadinessAwaiter{*this, fd, true, EVFILT_WRITE};
#endif
    }

    void remove(int fd) noexcept {
        if (fd < 0) {
            return;
        }
#if defined(__linux__)
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
#elif defined(__APPLE__)
        delete_kqueue_filter(fd, EVFILT_READ);
        delete_kqueue_filter(fd, EVFILT_WRITE);
#endif
        states_.erase(fd);
    }

private:
    struct FdState {
        std::coroutine_handle<> read_handle;
        std::coroutine_handle<> write_handle;
        bool edge_trigger = false;
    };

#if defined(__APPLE__)
    static void set_non_blocking(int fd) {
        const int flags = ::fcntl(fd, F_GETFL, 0);
        if (flags < 0) {
            throw std::system_error(errno, std::generic_category(), "fcntl F_GETFL");
        }
        if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
            throw std::system_error(errno, std::generic_category(), "fcntl F_SETFL");
        }
    }

    static void set_close_on_exec(int fd) {
        const int flags = ::fcntl(fd, F_GETFD, 0);
        if (flags < 0) {
            throw std::system_error(errno, std::generic_category(), "fcntl F_GETFD");
        }
        if (::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) < 0) {
            throw std::system_error(errno, std::generic_category(), "fcntl F_SETFD");
        }
    }

    int wakeup_read_fd() const noexcept {
        return wakeup_pipe_[0];
    }

    int wakeup_write_fd() const noexcept {
        return wakeup_pipe_[1];
    }
#endif

    void add_wakeup_fd() {
#if defined(__linux__)
        epoll_event event{};
        event.events = EPOLLIN;
        event.data.fd = wakeup_fd_;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wakeup_fd_, &event) < 0) {
            throw std::system_error(errno, std::generic_category(), "epoll_ctl wakeup fd");
        }
#elif defined(__APPLE__)
        struct kevent event{};
        EV_SET(&event, static_cast<uintptr_t>(wakeup_read_fd()), EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr);
        if (::kevent(kqueue_fd_, &event, 1, nullptr, 0, nullptr) < 0) {
            throw std::system_error(errno, std::generic_category(), "kevent wakeup fd");
        }
#endif
    }

    void wakeup() noexcept {
#if defined(__linux__)
        if (wakeup_fd_ < 0) {
            return;
        }

        std::uint64_t value = 1;
        while (::write(wakeup_fd_, &value, sizeof(value)) < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
#elif defined(__APPLE__)
        if (wakeup_write_fd() < 0) {
            return;
        }

        const char value = 1;
        while (::write(wakeup_write_fd(), &value, sizeof(value)) < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
#endif
    }

    void drain_wakeup_fd() noexcept {
#if defined(__linux__)
        std::uint64_t value = 0;
        while (::read(wakeup_fd_, &value, sizeof(value)) > 0) {}
#elif defined(__APPLE__)
        char buffer[64];
        while (::read(wakeup_read_fd(), buffer, sizeof(buffer)) > 0) {}
#endif
    }

    void drain_pending_tasks() {
        std::queue<std::function<void()>> local_tasks;
        {
            std::lock_guard lock{pending_mutex_};
            local_tasks.swap(pending_tasks_);
        }

        while (!local_tasks.empty()) {
            local_tasks.front()();
            local_tasks.pop();
        }
    }

    void compact_finished_tasks() {
        tasks_.erase(
            std::remove_if(tasks_.begin(), tasks_.end(), [](const Task<void>& task) { return task.done(); }),
            tasks_.end());
    }

#if defined(__linux__)
    void add_interest(int fd, std::uint32_t event, std::coroutine_handle<> handle) {
#elif defined(__APPLE__)
    void add_interest(int fd, int16_t event, std::coroutine_handle<> handle) {
#endif
        add_interest_impl(fd, event, handle, false);
    }

#if defined(__linux__)
    void add_interest_et(int fd, std::uint32_t event, std::coroutine_handle<> handle) {
#elif defined(__APPLE__)
    void add_interest_et(int fd, int16_t event, std::coroutine_handle<> handle) {
#endif
        add_interest_impl(fd, event, handle, true);
    }

private:
#if defined(__linux__)
    void add_interest_impl(int fd, std::uint32_t event, std::coroutine_handle<> handle, bool et) {
#elif defined(__APPLE__)
    void add_interest_impl(int fd, int16_t event, std::coroutine_handle<> handle, bool et) {
#endif
        if (fd < 0) {
            throw std::invalid_argument("cannot wait on an invalid file descriptor");
        }

        auto [it, inserted] = states_.try_emplace(fd);
        auto& state = it->second;

#if defined(__linux__)
        if (event == EPOLLIN) {
            state.read_handle = handle;
        } else if (event == EPOLLOUT) {
            state.write_handle = handle;
        } else {
            throw std::invalid_argument("unsupported epoll event interest");
        }

        epoll_event ep_event{};
        ep_event.events = EPOLLIN | EPOLLOUT | (et ? EPOLLET : 0U);
        ep_event.data.fd = fd;

        const int operation = inserted ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
        if (::epoll_ctl(epoll_fd_, operation, fd, &ep_event) < 0) {
            const int saved_errno = errno;
            states_.erase(fd);
            throw std::system_error(saved_errno, std::generic_category(), "epoll_ctl");
        }

#elif defined(__APPLE__)
        if (event == EVFILT_READ) {
            state.read_handle = handle;
        } else if (event == EVFILT_WRITE) {
            state.write_handle = handle;
        } else {
            throw std::invalid_argument("unsupported kqueue event interest");
        }

        try {
            update_or_remove_impl(fd, state, et);
        } catch (...) {
            if (inserted) {
                states_.erase(fd);
            }
            throw;
        }
#endif
        state.edge_trigger = et;
    }
public:

#if defined(__linux__)
    void dispatch(const epoll_event& event) {
        const int fd = event.data.fd;
        if (fd == wakeup_fd_) {
            drain_wakeup_fd();
            drain_pending_tasks();
            compact_finished_tasks();
            return;
        }

        auto it = states_.find(fd);
        if (it == states_.end()) {
            return;
        }

        auto& state = it->second;
        const bool has_error = (event.events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0;
        std::coroutine_handle<> read_handle;
        std::coroutine_handle<> write_handle;

        if ((event.events & EPOLLIN) || has_error) {
            read_handle = std::exchange(state.read_handle, {});
        }

        if ((event.events & EPOLLOUT) || has_error) {
            write_handle = std::exchange(state.write_handle, {});
        }

        update_or_remove(fd, state);

        if (read_handle) {
            read_handle.resume();
        }
        if (write_handle) {
            write_handle.resume();
        }
    }
#elif defined(__APPLE__)
    void dispatch(const struct kevent& event) {
        const int fd = static_cast<int>(event.ident);
        if (fd == wakeup_read_fd()) {
            drain_wakeup_fd();
            drain_pending_tasks();
            compact_finished_tasks();
            return;
        }

        auto it = states_.find(fd);
        if (it == states_.end()) {
            return;
        }

        auto& state = it->second;
        const bool has_error = (event.flags & (EV_ERROR | EV_EOF)) != 0;
        std::coroutine_handle<> read_handle;
        std::coroutine_handle<> write_handle;

        if (event.filter == EVFILT_READ || has_error) {
            read_handle = std::exchange(state.read_handle, {});
        }
        if (event.filter == EVFILT_WRITE || has_error) {
            write_handle = std::exchange(state.write_handle, {});
        }

        update_or_remove(fd, state);

        if (read_handle) {
            read_handle.resume();
        }
        if (write_handle) {
            write_handle.resume();
        }
    }
#endif

    void update_or_remove(int fd, const FdState& state) {
        update_or_remove_impl(fd, state, state.edge_trigger);
    }

    void update_or_remove_impl(int fd, const FdState& state, bool et) {
#if defined(__linux__)
        const std::uint32_t events = active_events(state);
        if (events == 0) {
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
            states_.erase(fd);
            return;
        }

        epoll_event ep_event{};
        ep_event.events = events | (et ? EPOLLET : 0U);
        ep_event.data.fd = fd;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ep_event) < 0 && errno != EBADF && errno != ENOENT) {
            throw std::system_error(errno, std::generic_category(), "epoll_ctl mod");
        }
#elif defined(__APPLE__)
        if (!state.read_handle && !state.write_handle) {
            delete_kqueue_filter(fd, EVFILT_READ);
            delete_kqueue_filter(fd, EVFILT_WRITE);
            states_.erase(fd);
            return;
        }

        update_kqueue_filter_impl(fd, EVFILT_READ, static_cast<bool>(state.read_handle), et);
        update_kqueue_filter_impl(fd, EVFILT_WRITE, static_cast<bool>(state.write_handle), et);
#endif
    }

#if defined(__linux__)
    static std::uint32_t active_events(const FdState& state) noexcept {
        std::uint32_t events = 0;
        if (state.read_handle) {
            events |= EPOLLIN;
        }
        if (state.write_handle) {
            events |= EPOLLOUT;
        }
        return events == EPOLLRDHUP ? 0 : events;
    }
#elif defined(__APPLE__)
    void update_kqueue_filter_impl(int fd, int16_t filter, bool active, bool et) {
        struct kevent event{};
        EV_SET(
            &event,
            static_cast<uintptr_t>(fd),
            filter,
            static_cast<uint16_t>(active ? (EV_ADD | EV_ENABLE | (et ? EV_CLEAR : 0)) : EV_DELETE),
            0,
            0,
            nullptr);

        if (::kevent(kqueue_fd_, &event, 1, nullptr, 0, nullptr) < 0) {
            if (!active && errno == ENOENT) {
                return;
            }
            throw std::system_error(errno, std::generic_category(), active ? "kevent add" : "kevent delete");
        }
    }

    void delete_kqueue_filter(int fd, int16_t filter) noexcept {
        if (kqueue_fd_ < 0) {
            return;
        }

        struct kevent event{};
        EV_SET(&event, static_cast<uintptr_t>(fd), filter, EV_DELETE, 0, 0, nullptr);
        while (::kevent(kqueue_fd_, &event, 1, nullptr, 0, nullptr) < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
    }
#endif

#if defined(__linux__)
    int epoll_fd_ = -1;
    int wakeup_fd_ = -1;
#elif defined(__APPLE__)
    int kqueue_fd_ = -1;
    int wakeup_pipe_[2]{-1, -1};
#endif

    std::atomic_bool stopped_ = false;
    std::unordered_map<int, FdState> states_;
    std::vector<Task<void>> tasks_;
    std::mutex pending_mutex_;
    std::queue<std::function<void()>> pending_tasks_;
};

} // namespace coro_epoll
