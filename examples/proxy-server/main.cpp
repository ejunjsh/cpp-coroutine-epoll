#include "coro_epoll/event_loop.hpp"
#include "coro_epoll/tcp.hpp"
#include "coro_epoll/worker_group.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <charconv>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

using coro_epoll::EventLoop;
using coro_epoll::Task;
using coro_epoll::TcpServer;
using coro_epoll::TcpSocket;
using coro_epoll::WorkerGroup;

// ---------------------------------------------------------------------------
// Async non-blocking TCP connect
// ---------------------------------------------------------------------------
Task<int> async_connect_to(EventLoop& loop, const char* host, std::uint16_t port) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    if (::inet_pton(AF_INET, host, &address.sin_addr) != 1) {
        throw std::runtime_error(std::string("invalid address: ") + host);
    }

#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
#else
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
#endif
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(), "socket");
    }

    try {
        coro_epoll::disable_sigpipe(fd);
#if !(defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC))
        coro_epoll::set_non_blocking(fd);
        coro_epoll::set_close_on_exec(fd);
#endif
    } catch (...) {
        ::close(fd);
        throw;
    }

    const int ret = ::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
    if (ret < 0 && errno != EINPROGRESS) {
        const int saved = errno;
        ::close(fd);
        throw std::system_error(saved, std::generic_category(), "connect");
    }

    // Immediately connected (possible for localhost)
    if (ret == 0) {
        co_return fd;
    }

    // Wait for the connection to complete
    co_await loop.writable(fd);

    // Verify the connection succeeded
    int error = 0;
    socklen_t len = sizeof(error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0) {
        const int saved = errno;
        ::close(fd);
        throw std::system_error(saved, std::generic_category(), "getsockopt SO_ERROR");
    }
    if (error != 0) {
        ::close(fd);
        throw std::system_error(error, std::generic_category(), "connect");
    }

    co_return fd;
}

// ---------------------------------------------------------------------------
// Bidirectional relay coroutine
// ---------------------------------------------------------------------------
Task<void> relay(std::shared_ptr<TcpSocket> from, std::shared_ptr<TcpSocket> to) {
    try {
        std::array<char, 8192> buffer{};
        while (true) {
            const std::size_t count = co_await from->async_read(buffer.data(), buffer.size());
            if (count == 0) {
                break;
            }
            co_await to->async_write(buffer.data(), count);
        }
    } catch (const std::exception& error) {
        std::cerr << "relay error: " << error.what() << '\n';
    }

    // Tear down both sockets so the sibling relay task wakes up and exits.
    from->close();
    to->close();
}

// ---------------------------------------------------------------------------
// Per-session proxy coroutine
// ---------------------------------------------------------------------------
Task<void> handle_session(EventLoop& loop, TcpSocket client, std::string backend_host, std::uint16_t backend_port) {
    try {
        const int backend_fd = co_await async_connect_to(loop, backend_host.c_str(), backend_port);

        auto client_ptr = std::make_shared<TcpSocket>(std::move(client));
        auto backend_ptr = std::make_shared<TcpSocket>(loop, backend_fd);

        std::cout << "proxy session: client connected → " << backend_host << ":" << backend_port << '\n';

        // Spawn client→backend relay, await backend→client in this coroutine.
        loop.spawn(relay(client_ptr, backend_ptr));
        co_await relay(backend_ptr, client_ptr);
    } catch (const std::exception& error) {
        std::cerr << "proxy session error: " << error.what() << '\n';
    }
}

// ---------------------------------------------------------------------------
// Accept loop — runs on each worker EventLoop
// ---------------------------------------------------------------------------
Task<void> accept_and_handle(EventLoop& loop, TcpServer& server,
                             std::string backend_host, std::uint16_t backend_port) {
    try {
        while (true) {
            const int client_fd = co_await server.async_accept_fd();
            loop.spawn(handle_session(loop, TcpSocket(loop, client_fd), backend_host, backend_port));
        }
    } catch (const std::exception& error) {
        std::cerr << "accept loop error: " << error.what() << '\n';
        loop.stop();
    }
}

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------
std::string usage() {
    return "usage: proxy_server <listen_port> <backend_host> <backend_port> [worker_count]";
}

std::uint16_t parse_listen_port(int argc, char** argv) {
    if (argc < 2) {
        throw std::runtime_error(usage());
    }

    std::uint32_t port = 0;
    const std::string_view value(argv[1]);
    const auto [ptr, error] = std::from_chars(value.data(), value.data() + value.size(), port);
    if (error != std::errc{} || ptr != value.data() + value.size() || port == 0 || port > 65535) {
        throw std::runtime_error("listen port must be an integer between 1 and 65535");
    }
    return static_cast<std::uint16_t>(port);
}

std::string parse_backend_host(int argc, char** argv) {
    if (argc < 3) {
        throw std::runtime_error(usage());
    }
    return argv[2];
}

std::uint16_t parse_backend_port(int argc, char** argv) {
    if (argc < 4) {
        throw std::runtime_error(usage());
    }

    std::uint32_t port = 0;
    const std::string_view value(argv[3]);
    const auto [ptr, error] = std::from_chars(value.data(), value.data() + value.size(), port);
    if (error != std::errc{} || ptr != value.data() + value.size() || port == 0 || port > 65535) {
        throw std::runtime_error("backend port must be an integer between 1 and 65535");
    }
    return static_cast<std::uint16_t>(port);
}

std::size_t parse_worker_count(int argc, char** argv) {
    const unsigned int hardware_workers = std::thread::hardware_concurrency();
    const std::size_t default_workers = hardware_workers == 0 ? 4 : hardware_workers;
    if (argc < 5) {
        return default_workers;
    }

    std::size_t worker_count = 0;
    const std::string_view value(argv[4]);
    const auto [ptr, error] = std::from_chars(value.data(), value.data() + value.size(), worker_count);
    if (error != std::errc{} || ptr != value.data() + value.size() || worker_count == 0) {
        throw std::runtime_error("worker count must be a positive integer");
    }
    return worker_count;
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
int main(int argc, char** argv) {
    try {
        const std::uint16_t listen_port = parse_listen_port(argc, argv);
        const std::string backend_host = parse_backend_host(argc, argv);
        const std::uint16_t backend_port = parse_backend_port(argc, argv);
        const std::size_t worker_count = parse_worker_count(argc, argv);

        WorkerGroup workers{worker_count};
        std::vector<std::unique_ptr<TcpServer>> servers;
        servers.reserve(worker_count);

        for (std::size_t i = 0; i < worker_count; ++i) {
            EventLoop& worker = workers.next();
            auto server = std::make_unique<TcpServer>(worker);
            server->listen(listen_port, SOMAXCONN, true);
            TcpServer* server_ptr = server.get();
            servers.push_back(std::move(server));

            worker.post([&worker, server_ptr, &backend_host, backend_port] {
                worker.spawn(accept_and_handle(worker, *server_ptr, backend_host, backend_port));
            });
        }

        std::cout << "tcp proxy listening on 0.0.0.0:" << listen_port
                  << " → " << backend_host << ":" << backend_port
                  << " with " << worker_count << " SO_REUSEPORT worker socket(s)\n";

        workers.join();
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "fatal: " << error.what() << '\n';
        return 1;
    }
}
