#pragma once

#include <coroutine>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>

namespace coro_epoll {

template <typename T = void>
class Task;

template <typename T>
class Task {
public:
    struct promise_type {
        std::optional<T> value;
        std::exception_ptr exception;
        std::coroutine_handle<> continuation;

        Task get_return_object() noexcept {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() noexcept {
            return {};
        }

        auto final_suspend() noexcept {
            struct FinalAwaiter {
                bool await_ready() noexcept {
                    return false;
                }

                void await_suspend(std::coroutine_handle<promise_type> handle) noexcept {
                    if (handle.promise().continuation) {
                        handle.promise().continuation.resume();
                    }
                }

                void await_resume() noexcept {}
            };

            return FinalAwaiter{};
        }

        template <typename U>
        requires std::is_convertible_v<U, T>
        void return_value(U&& new_value) {
            value.emplace(std::forward<U>(new_value));
        }

        void unhandled_exception() noexcept {
            exception = std::current_exception();
        }
    };

    using handle_type = std::coroutine_handle<promise_type>;

    explicit Task(handle_type handle = nullptr) noexcept : handle_(handle) {}

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, nullptr)) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            destroy();
            handle_ = std::exchange(other.handle_, nullptr);
        }
        return *this;
    }

    ~Task() {
        destroy();
    }

    bool done() const noexcept {
        return !handle_ || handle_.done();
    }

    std::exception_ptr exception() const noexcept {
        if (handle_ && handle_.done()) {
            return handle_.promise().exception;
        }
        return nullptr;
    }

    void resume() {
        if (handle_ && !handle_.done()) {
            handle_.resume();
        }
    }

    struct Awaiter {
        handle_type handle;

        bool await_ready() const noexcept {
            return !handle || handle.done();
        }

        void await_suspend(std::coroutine_handle<> continuation) noexcept {
            handle.promise().continuation = continuation;
            handle.resume();
        }

        T await_resume() {
            auto& promise = handle.promise();
            if (promise.exception) {
                std::rethrow_exception(promise.exception);
            }
            return std::move(*promise.value);
        }
    };

    Awaiter operator co_await() noexcept {
        return Awaiter{handle_};
    }

private:
    void destroy() noexcept {
        if (handle_) {
            handle_.destroy();
            handle_ = nullptr;
        }
    }

    handle_type handle_;
};

template <>
class Task<void> {
public:
    struct promise_type {
        std::exception_ptr exception;
        std::coroutine_handle<> continuation;

        Task get_return_object() noexcept {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() noexcept {
            return {};
        }

        auto final_suspend() noexcept {
            struct FinalAwaiter {
                bool await_ready() noexcept {
                    return false;
                }

                void await_suspend(std::coroutine_handle<promise_type> handle) noexcept {
                    if (handle.promise().continuation) {
                        handle.promise().continuation.resume();
                    }
                }

                void await_resume() noexcept {}
            };

            return FinalAwaiter{};
        }

        void return_void() noexcept {}

        void unhandled_exception() noexcept {
            exception = std::current_exception();
        }
    };

    using handle_type = std::coroutine_handle<promise_type>;

    explicit Task(handle_type handle = nullptr) noexcept : handle_(handle) {}

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, nullptr)) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            destroy();
            handle_ = std::exchange(other.handle_, nullptr);
        }
        return *this;
    }

    ~Task() {
        destroy();
    }

    bool done() const noexcept {
        return !handle_ || handle_.done();
    }

    std::exception_ptr exception() const noexcept {
        if (handle_ && handle_.done()) {
            return handle_.promise().exception;
        }
        return nullptr;
    }

    void resume() {
        if (handle_ && !handle_.done()) {
            handle_.resume();
        }
    }

    struct Awaiter {
        handle_type handle;

        bool await_ready() const noexcept {
            return !handle || handle.done();
        }

        void await_suspend(std::coroutine_handle<> continuation) noexcept {
            handle.promise().continuation = continuation;
            handle.resume();
        }

        void await_resume() {
            auto& promise = handle.promise();
            if (promise.exception) {
                std::rethrow_exception(promise.exception);
            }
        }
    };

    Awaiter operator co_await() noexcept {
        return Awaiter{handle_};
    }

private:
    void destroy() noexcept {
        if (handle_) {
            handle_.destroy();
            handle_ = nullptr;
        }
    }

    handle_type handle_;
};

} // namespace coro_epoll
