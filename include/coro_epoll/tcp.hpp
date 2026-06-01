#pragma once

#include "coro_epoll/event_loop.hpp"
#include "coro_epoll/task.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <string>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace coro_epoll {

inline void set_non_blocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        throw std::system_error(errno, std::generic_category(), "fcntl F_GETFL");
    }
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        throw std::system_error(errno, std::generic_category(), "fcntl F_SETFL");
    }
}

inline void set_close_on_exec(int fd) {
    const int flags = ::fcntl(fd, F_GETFD, 0);
    if (flags < 0) {
        throw std::system_error(errno, std::generic_category(), "fcntl F_GETFD");
    }
    if (::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) < 0) {
        throw std::system_error(errno, std::generic_category(), "fcntl F_SETFD");
    }
}

inline constexpr int send_no_signal_flag() noexcept {
#ifdef MSG_NOSIGNAL
    return MSG_NOSIGNAL;
#else
    return 0;
#endif
}

inline void disable_sigpipe(int fd) {
#ifdef SO_NOSIGPIPE
    int value = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &value, sizeof(value)) < 0) {
        throw std::system_error(errno, std::generic_category(), "setsockopt SO_NOSIGPIPE");
    }
#else
    static_cast<void>(fd);
#endif
}

class TcpSocket {
public:
    TcpSocket() = default;

    TcpSocket(EventLoop& loop, int fd) : loop_(&loop), fd_(fd) {
        if (fd < 0) {
            throw std::invalid_argument("TcpSocket requires a valid file descriptor");
        }
    }

    TcpSocket(const TcpSocket&) = delete;
    TcpSocket& operator=(const TcpSocket&) = delete;

    TcpSocket(TcpSocket&& other) noexcept
        : loop_(std::exchange(other.loop_, nullptr)), fd_(std::exchange(other.fd_, -1)) {}

    TcpSocket& operator=(TcpSocket&& other) noexcept {
        if (this != &other) {
            close();
            loop_ = std::exchange(other.loop_, nullptr);
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }

    ~TcpSocket() {
        close();
    }

    bool valid() const noexcept {
        return fd_ >= 0;
    }

    int fd() const noexcept {
        return fd_;
    }

    void close() noexcept {
        if (fd_ >= 0) {
            if (loop_) {
                loop_->remove(fd_);
            }
            ::close(fd_);
            fd_ = -1;
            loop_ = nullptr;
        }
    }

    Task<std::size_t> async_read(char* buffer, std::size_t size) {
        ensure_valid();
        while (true) {
            const ssize_t count = ::recv(fd_, buffer, size, 0);
            if (count > 0) {
                co_return static_cast<std::size_t>(count);
            }
            if (count == 0) {
                co_return 0;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                co_await loop_->readable(fd_);
                continue;
            }
            throw std::system_error(errno, std::generic_category(), "recv");
        }
    }

    Task<std::size_t> async_write(const char* buffer, std::size_t size) {
        ensure_valid();
        std::size_t total = 0;
        while (total < size) {
            const ssize_t count = ::send(fd_, buffer + total, size - total, send_no_signal_flag());
            if (count > 0) {
                total += static_cast<std::size_t>(count);
                continue;
            }
            if (count == 0) {
                co_await loop_->writable(fd_);
                continue;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                co_await loop_->writable(fd_);
                continue;
            }
            throw std::system_error(errno, std::generic_category(), "send");
        }
        co_return total;
    }

private:
    void ensure_valid() const {
        if (!loop_ || fd_ < 0) {
            throw std::runtime_error("invalid TcpSocket");
        }
    }

    EventLoop* loop_ = nullptr;
    int fd_ = -1;
};

class TcpServer {
public:
    explicit TcpServer(EventLoop& loop) : loop_(&loop) {}

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    ~TcpServer() {
        close();
    }

    void listen(std::uint16_t port, int backlog = SOMAXCONN) {
        close();

#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
        fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
#else
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
#endif

        if (fd_ < 0) {
            throw std::system_error(errno, std::generic_category(), "socket");
        }
        disable_sigpipe(fd_);

#if !(defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC))
        try {
            set_non_blocking(fd_);
            set_close_on_exec(fd_);
        } catch (...) {
            ::close(fd_);
            fd_ = -1;
            throw;
        }
#endif

        int reuse = 1;
        if (::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
            throw std::system_error(errno, std::generic_category(), "setsockopt SO_REUSEADDR");
        }

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_ANY);
        address.sin_port = htons(port);

        if (::bind(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
            throw std::system_error(errno, std::generic_category(), "bind");
        }

        if (::listen(fd_, backlog) < 0) {
            throw std::system_error(errno, std::generic_category(), "listen");
        }
    }

    Task<int> async_accept_fd() {
        ensure_valid();
        while (true) {
            sockaddr_in client_address{};
            socklen_t client_length = sizeof(client_address);
#if defined(__linux__) && defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
            const int client_fd = ::accept4(
                fd_,
                reinterpret_cast<sockaddr*>(&client_address),
                &client_length,
                SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
            const int client_fd = ::accept(fd_, reinterpret_cast<sockaddr*>(&client_address), &client_length);
#endif

            if (client_fd >= 0) {
                try {
                    disable_sigpipe(client_fd);
#if !(defined(__linux__) && defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC))
                    set_non_blocking(client_fd);
                    set_close_on_exec(client_fd);
#endif
                } catch (...) {
                    ::close(client_fd);
                    throw;
                }
                co_return client_fd;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                co_await loop_->readable(fd_);
                continue;
            }
#if defined(__linux__) && defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
            throw std::system_error(errno, std::generic_category(), "accept4");
#else
            throw std::system_error(errno, std::generic_category(), "accept");
#endif
        }
    }

    Task<TcpSocket> async_accept() {
        const int client_fd = co_await async_accept_fd();
        co_return TcpSocket(*loop_, client_fd);
    }

    void close() noexcept {
        if (fd_ >= 0) {
            ::close(fd_);
            loop_->remove(fd_);
            fd_ = -1;
        }
    }

private:
    void ensure_valid() const {
        if (!loop_ || fd_ < 0) {
            throw std::runtime_error("invalid TcpServer");
        }
    }

    EventLoop* loop_ = nullptr;
    int fd_ = -1;
};

} // namespace coro_epoll
