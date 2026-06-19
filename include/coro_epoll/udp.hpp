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
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace coro_epoll {

class UdpEndpoint {
public:
    UdpEndpoint() noexcept {
        address_.sin_family = AF_INET;
    }

    explicit UdpEndpoint(const sockaddr_in& address) noexcept : address_(address) {}

    UdpEndpoint(std::string_view address, std::uint16_t port) {
        address_.sin_family = AF_INET;
        address_.sin_port = htons(port);
        const std::string address_text(address);
        const int result = ::inet_pton(AF_INET, address_text.c_str(), &address_.sin_addr);
        if (result == 0) {
            throw std::invalid_argument("invalid IPv4 address");
        }
        if (result < 0) {
            throw std::system_error(errno, std::generic_category(), "inet_pton");
        }
    }

    const sockaddr* sockaddr_ptr() const noexcept {
        return reinterpret_cast<const sockaddr*>(&address_);
    }

    sockaddr* sockaddr_ptr() noexcept {
        return reinterpret_cast<sockaddr*>(&address_);
    }

    socklen_t sockaddr_length() const noexcept {
        return sizeof(address_);
    }

    socklen_t* sockaddr_length_ptr() noexcept {
        length_ = sizeof(address_);
        return &length_;
    }

    std::string address() const {
        char buffer[INET_ADDRSTRLEN]{};
        if (::inet_ntop(AF_INET, &address_.sin_addr, buffer, sizeof(buffer)) == nullptr) {
            throw std::system_error(errno, std::generic_category(), "inet_ntop");
        }
        return buffer;
    }

    std::uint16_t port() const noexcept {
        return ntohs(address_.sin_port);
    }

private:
    sockaddr_in address_{};
    socklen_t length_ = sizeof(address_);
};

struct UdpReceiveResult {
    std::size_t size = 0;
    UdpEndpoint endpoint;
};

class UdpSocket {
public:
    explicit UdpSocket(EventLoop& loop) : loop_(&loop) {}

    UdpSocket(const UdpSocket&) = delete;
    UdpSocket& operator=(const UdpSocket&) = delete;

    UdpSocket(UdpSocket&& other) noexcept
        : loop_(std::exchange(other.loop_, nullptr)), fd_(std::exchange(other.fd_, -1)) {}

    UdpSocket& operator=(UdpSocket&& other) noexcept {
        if (this != &other) {
            close();
            loop_ = std::exchange(other.loop_, nullptr);
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }

    ~UdpSocket() {
        close();
    }

    bool valid() const noexcept {
        return fd_ >= 0;
    }

    int fd() const noexcept {
        return fd_;
    }

    void bind(std::uint16_t port, bool reuse_port = false) {
        close();

#if defined(SOCK_NONBLOCK) && defined(SOCK_CLOEXEC)
        fd_ = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
#else
        fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
#endif
        if (fd_ < 0) {
            throw std::system_error(errno, std::generic_category(), "socket");
        }
        try {
#if !defined(SOCK_NONBLOCK) || !defined(SOCK_CLOEXEC)
            set_non_blocking();
            set_close_on_exec();
#endif
            int reuse = 1;
            if (::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
                throw std::system_error(errno, std::generic_category(), "setsockopt SO_REUSEADDR");
            }
#ifdef SO_REUSEPORT
            if (reuse_port) {
                if (::setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) < 0) {
                    throw std::system_error(errno, std::generic_category(), "setsockopt SO_REUSEPORT");
                }
            }
#else
        if (reuse_port) {
            throw std::runtime_error("SO_REUSEPORT is not supported on this platform");
        }
#endif

            sockaddr_in address{};
            address.sin_family = AF_INET;
            address.sin_addr.s_addr = htonl(INADDR_ANY);
            address.sin_port = htons(port);

            if (::bind(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
                throw std::system_error(errno, std::generic_category(), "bind");
            }
        } catch (...) {
            ::close(fd_);
            fd_ = -1;
            throw;
        }
    }

    Task<UdpReceiveResult> async_recv_from(char* buffer, std::size_t size) {
        ensure_valid();
        while (true) {
            UdpEndpoint endpoint;
            const ssize_t count = ::recvfrom(
                fd_,
                buffer,
                size,
                0,
                endpoint.sockaddr_ptr(),
                endpoint.sockaddr_length_ptr());

            if (count >= 0) {
                co_return UdpReceiveResult{static_cast<std::size_t>(count), endpoint};
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                co_await loop_->readable(fd_);
                continue;
            }
            throw std::system_error(errno, std::generic_category(), "recvfrom");
        }
    }

    Task<std::vector<UdpReceiveResult>> async_recv_from_et(char* buffer, std::size_t size) {
        ensure_valid();
        std::vector<UdpReceiveResult> results;
        while (true) {
            UdpEndpoint endpoint;
            const ssize_t count = ::recvfrom(
                fd_,
                buffer,
                size,
                0,
                endpoint.sockaddr_ptr(),
                endpoint.sockaddr_length_ptr());

            if (count >= 0) {
                results.push_back(UdpReceiveResult{static_cast<std::size_t>(count), endpoint});
                continue;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (!results.empty()) {
                    co_return results; // drained all available datagrams
                }
                co_await loop_->readable_et(fd_);
                continue;
            }
            throw std::system_error(errno, std::generic_category(), "recvfrom");
        }
    }

    Task<std::size_t> async_send_to(const char* buffer, std::size_t size, const UdpEndpoint& endpoint) {
        ensure_valid();
        while (true) {
            const ssize_t count = ::sendto(
                fd_,
                buffer,
                size,
                0,
                endpoint.sockaddr_ptr(),
                endpoint.sockaddr_length());

            if (count >= 0) {
                co_return static_cast<std::size_t>(count);
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                co_await loop_->writable(fd_);
                continue;
            }
            throw std::system_error(errno, std::generic_category(), "sendto");
        }
    }

    void close() noexcept {
        if (fd_ >= 0) {
            if (loop_) {
                loop_->remove(fd_);
            }
            ::close(fd_);
            fd_ = -1;
        }
    }

private:
    void ensure_valid() const {
        if (!loop_ || fd_ < 0) {
            throw std::runtime_error("invalid UdpSocket");
        }
    }

    void set_non_blocking() {
        const int flags = ::fcntl(fd_, F_GETFL, 0);
        if (flags < 0) {
            throw std::system_error(errno, std::generic_category(), "fcntl F_GETFL");
        }
        if (::fcntl(fd_, F_SETFL, flags | O_NONBLOCK) < 0) {
            throw std::system_error(errno, std::generic_category(), "fcntl F_SETFL");
        }
    }

    void set_close_on_exec() {
        const int flags = ::fcntl(fd_, F_GETFD, 0);
        if (flags < 0) {
            throw std::system_error(errno, std::generic_category(), "fcntl F_GETFD");
        }
        if (::fcntl(fd_, F_SETFD, flags | FD_CLOEXEC) < 0) {
            throw std::system_error(errno, std::generic_category(), "fcntl F_SETFD");
        }
    }

    EventLoop* loop_ = nullptr;
    int fd_ = -1;
};

} // namespace coro_epoll
