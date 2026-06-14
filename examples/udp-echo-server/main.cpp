#include "coro_epoll/event_loop.hpp"
#include "coro_epoll/task.hpp"
#include "coro_epoll/thread_pool.hpp"
#include "coro_epoll/udp.hpp"
#include "coro_epoll/worker_group.hpp"

#include <array>
#include <charconv>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

using coro_epoll::EventLoop;
using coro_epoll::Task;
using coro_epoll::ThreadPool;
using coro_epoll::UdpSocket;
using coro_epoll::WorkerGroup;

std::string process_payload(std::string input) {
    return input;
}

Task<void> udp_echo_loop(EventLoop& loop, UdpSocket& socket, ThreadPool& business_pool) {
    try {
        std::array<char, 4096> buffer{};
        while (true) {
            auto received = co_await socket.async_recv_from(buffer.data(), buffer.size());

            std::string input{buffer.data(), received.size};
            std::string output = co_await business_pool.submit(loop, [payload = std::move(input)]() mutable {
                return process_payload(std::move(payload));
            });

            co_await socket.async_send_to(output.data(), output.size(), received.endpoint);
        }
    } catch (const std::exception& error) {
        std::cerr << "udp echo loop error: " << error.what() << '\n';
        loop.stop();
    }
}

std::uint16_t parse_port(int argc, char** argv) {
    if (argc < 2) {
        return 8080;
    }

    std::uint32_t port = 0;
    const std::string_view value{argv[1]};
    const auto [ptr, error] = std::from_chars(value.data(), value.data() + value.size(), port);
    if (error != std::errc{} || ptr != value.data() + value.size() || port == 0 || port > 65535) {
        throw std::runtime_error("port must be an integer between 1 and 65535");
    }
    return static_cast<std::uint16_t>(port);
}

std::size_t parse_worker_count(int argc, char** argv) {
    const unsigned int hardware_workers = std::thread::hardware_concurrency();
    const std::size_t default_workers = hardware_workers == 0 ? 4 : hardware_workers;
    if (argc < 3) {
        return default_workers;
    }

    std::size_t worker_count = 0;
    const std::string_view value{argv[2]};
    const auto [ptr, error] = std::from_chars(value.data(), value.data() + value.size(), worker_count);
    if (error != std::errc{} || ptr != value.data() + value.size() || worker_count == 0) {
        throw std::runtime_error("worker count must be a positive integer");
    }
    return worker_count;
}

std::size_t parse_business_worker_count(int argc, char** argv, std::size_t default_count) {
    if (argc < 4) {
        return default_count;
    }

    std::size_t worker_count = 0;
    const std::string_view value{argv[3]};
    const auto [ptr, error] = std::from_chars(value.data(), value.data() + value.size(), worker_count);
    if (error != std::errc{} || ptr != value.data() + value.size() || worker_count == 0) {
        throw std::runtime_error("business worker count must be a positive integer");
    }
    return worker_count;
}

int main(int argc, char** argv) {
    try {
        const std::uint16_t port = parse_port(argc, argv);
        const std::size_t worker_count = parse_worker_count(argc, argv);
        const std::size_t business_worker_count = parse_business_worker_count(argc, argv, worker_count);

        WorkerGroup workers{worker_count};
        ThreadPool business_pool{business_worker_count};
        std::vector<std::unique_ptr<UdpSocket>> sockets;
        sockets.reserve(worker_count);

        for (std::size_t i = 0; i < worker_count; ++i) {
            EventLoop& worker = workers.next();
            auto socket = std::make_unique<UdpSocket>(worker);
            socket->bind(port, true);
            UdpSocket* socket_ptr = socket.get();
            sockets.push_back(std::move(socket));

            worker.post([&worker, socket_ptr, &business_pool] {
                worker.spawn(udp_echo_loop(worker, *socket_ptr, business_pool));
            });
        }

        std::cout << "udp echo server listening on 0.0.0.0:" << port
                  << " with " << worker_count << " SO_REUSEPORT worker socket(s) and "
                  << business_worker_count << " business worker thread(s)\n";

        workers.join();
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "fatal: " << error.what() << '\n';
        return 1;
    }
}
