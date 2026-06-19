#include "coro_epoll/event_loop.hpp"
#include "coro_epoll/tcp.hpp"
#include "coro_epoll/thread_pool.hpp"
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
using coro_epoll::TcpServer;
using coro_epoll::TcpSocket;
using coro_epoll::ThreadPool;
using coro_epoll::WorkerGroup;

std::string process_payload(std::string input) {
    // Put CPU-heavy or blocking business work here. The sample keeps echo semantics.
    return input;
}

Task<void> handle_client(EventLoop& loop, TcpSocket socket, ThreadPool& business_pool) {
    try {
        std::array<char, 4096> buffer{};
        while (true) {
            const std::size_t count = co_await socket.async_read(buffer.data(), buffer.size());
            if (count == 0) {
                break;
            }

            std::string input(buffer.data(), count);
            std::string output = co_await business_pool.submit(loop, [input = std::move(input)]() mutable {
                return process_payload(std::move(input));
            });

            co_await socket.async_write(output.data(), output.size());
        }
    } catch (const std::exception& error) {
        std::cerr << "client error: " << error.what() << '\n';
    }
}

Task<void> accept_and_handle(EventLoop& loop, TcpServer& server, ThreadPool& business_pool) {
    try {
        while (true) {
            const int client_fd = co_await server.async_accept_fd();
            loop.spawn(handle_client(loop, TcpSocket(loop, client_fd), business_pool));
        }
    } catch (const std::exception& error) {
        std::cerr << "accept loop error: " << error.what() << '\n';
        loop.stop();
    }
}

std::uint16_t parse_port(int argc, char** argv) {
    if (argc < 2) {
        return 8888;
    }

    std::uint32_t port = 0;
    const std::string_view value(argv[1]);
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
    const std::string_view value(argv[2]);
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
    const std::string_view value(argv[3]);
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
        std::vector<std::unique_ptr<TcpServer>> servers;
        servers.reserve(worker_count);

        for (std::size_t i = 0; i < worker_count; ++i) {
            EventLoop& worker = workers.next();
            auto server = std::make_unique<TcpServer>(worker);
            server->listen(port, SOMAXCONN, true);
            TcpServer* server_ptr = server.get();
            servers.push_back(std::move(server));

            worker.post([&worker, server_ptr, &business_pool] {
                worker.spawn(accept_and_handle(worker, *server_ptr, business_pool));
            });
        }

        std::cout << "echo server listening on 0.0.0.0:" << port
                  << " with " << worker_count << " SO_REUSEPORT worker socket(s) and "
                  << business_worker_count << " business worker thread(s)\n";

        workers.join();
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "fatal: " << error.what() << '\n';
        return 1;
    }
}
