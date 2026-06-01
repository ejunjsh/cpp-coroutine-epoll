#pragma once

#include "coro_epoll/event_loop.hpp"

#include <atomic>
#include <cstddef>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

namespace coro_epoll {

class WorkerGroup {
public:
    explicit WorkerGroup(std::size_t worker_count) {
        if (worker_count == 0) {
            throw std::invalid_argument("worker_count must be greater than zero");
        }

        loops_.reserve(worker_count);
        threads_.reserve(worker_count);

        for (std::size_t i = 0; i < worker_count; ++i) {
            loops_.push_back(std::make_unique<EventLoop>());
        }

        for (auto& loop : loops_) {
            threads_.emplace_back([loop = loop.get()] {
                try {
                    loop->run();
                } catch (const std::exception& error) {
                    std::cerr << "worker loop error: " << error.what() << '\n';
                }
            });
        }
    }

    WorkerGroup(const WorkerGroup&) = delete;
    WorkerGroup& operator=(const WorkerGroup&) = delete;

    ~WorkerGroup() {
        stop();
        join();
    }

    EventLoop& next() noexcept {
        const std::size_t index = next_index_.fetch_add(1, std::memory_order_relaxed) % loops_.size();
        return *loops_[index];
    }

    std::size_t size() const noexcept {
        return loops_.size();
    }

    void stop() {
        for (auto& loop : loops_) {
            loop->stop();
        }
    }

    void join() noexcept {
        for (auto& thread : threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

private:
    std::vector<std::unique_ptr<EventLoop>> loops_;
    std::vector<std::thread> threads_;
    std::atomic<std::size_t> next_index_{0};
};

} // namespace coro_epoll
