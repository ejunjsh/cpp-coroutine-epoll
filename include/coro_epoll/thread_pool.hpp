#pragma once

#include "coro_epoll/event_loop.hpp"

#include <condition_variable>
#include <coroutine>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace coro_epoll {

template <typename T>
struct ThreadPoolState {
    std::optional<T> value;
    std::exception_ptr exception;
    std::coroutine_handle<> continuation;
};

template <>
struct ThreadPoolState<void> {
    std::exception_ptr exception;
    std::coroutine_handle<> continuation;
};

class ThreadPool {
public:
    explicit ThreadPool(std::size_t thread_count) {
        if (thread_count == 0) {
            throw std::invalid_argument("thread_count must be greater than zero");
        }

        threads_.reserve(thread_count);
        for (std::size_t i = 0; i < thread_count; ++i) {
            threads_.emplace_back([this] { worker_loop(); });
        }
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    ~ThreadPool() {
        stop();
        join();
    }

    void stop() noexcept {
        {
            std::lock_guard lock(mutex_);
            stopped_ = true;
        }
        condition_.notify_all();
    }

    void join() noexcept {
        for (auto& thread : threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    template <typename F>
    auto submit(EventLoop& continuation_loop, F&& function) {
        using Function = std::decay_t<F>;
        using Result = std::invoke_result_t<Function&>;

        return Awaiter<Result>{
            *this,
            continuation_loop,
            std::function<Result()>(std::forward<F>(function))};
    }

private:
    template <typename Result>
    class Awaiter {
    public:
        Awaiter(ThreadPool& pool, EventLoop& continuation_loop, std::function<Result()> function)
            : pool_(pool), continuation_loop_(continuation_loop), function_(std::move(function)) {}

        bool await_ready() const noexcept {
            return false;
        }

        void await_suspend(std::coroutine_handle<> continuation) {
            state_->continuation = continuation;

            auto state = state_;
            auto function = std::move(function_);
            EventLoop* continuation_loop = &continuation_loop_;

            pool_.enqueue([state, function = std::move(function), continuation_loop]() mutable {
                try {
                    if constexpr (std::is_void_v<Result>) {
                        function();
                    } else {
                        state->value.emplace(function());
                    }
                } catch (...) {
                    state->exception = std::current_exception();
                }

                continuation_loop->post([state] {
                    state->continuation.resume();
                });
            });
        }

        decltype(auto) await_resume() {
            if (state_->exception) {
                std::rethrow_exception(state_->exception);
            }
            if constexpr (std::is_void_v<Result>) {
                return;
            } else {
                return std::move(*state_->value);
            }
        }

    private:
        ThreadPool& pool_;
        EventLoop& continuation_loop_;
        std::function<Result()> function_;
        std::shared_ptr<ThreadPoolState<Result>> state_ = std::make_shared<ThreadPoolState<Result>>();
    };

    void enqueue(std::function<void()> task) {
        {
            std::lock_guard lock(mutex_);
            if (stopped_) {
                throw std::runtime_error("cannot enqueue task on a stopped ThreadPool");
            }
            tasks_.push(std::move(task));
        }
        condition_.notify_one();
    }

    void worker_loop() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock lock(mutex_);
                condition_.wait(lock, [this] { return stopped_ || !tasks_.empty(); });
                if (stopped_ && tasks_.empty()) {
                    return;
                }
                task = std::move(tasks_.front());
                tasks_.pop();
            }
            task();
        }
    }

    std::mutex mutex_;
    std::condition_variable condition_;
    bool stopped_ = false;
    std::queue<std::function<void()>> tasks_;
    std::vector<std::thread> threads_;
};

} // namespace coro_epoll
