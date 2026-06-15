# C++20 Coroutine Event Loop

![CMake Build](https://github.com/ejunjsh/cpp-coroutine-epoll/actions/workflows/cmake-multi-platform.yml/badge.svg)

Small Linux/macOS networking example that wraps readiness events with C++20 coroutines. Linux uses 'epoll' plus 'eventfd'; macOS uses 'kqueue' plus 'pipe'. Each worker thread owns an `EventLoop` with a listening socket (bound with `SO_REUSEPORT`), and a separate business `ThreadPool` handles CPU-heavy or blocking work.

## Structure

- `coro_epoll::Task<T>`: coroutine return type.
- `coro_epoll::EventLoop`: single-threaded reactor with Linux 'epoll'/'eventfd' or macOS 'kqueue'/'pipe' wakeup and cross-thread 'post()'.
- `coro_epoll::WorkerGroup`: owns one worker `EventLoop` per network worker thread.
- `coro_epoll::ThreadPool`: runs heavy work away from the network loops.
- `coro_epoll::TcpServer`: non-blocking listening socket, supports `SO_REUSEPORT`.
- `coro_epoll::TcpSocket`: wraps fd, registers interest with EventLoop and resumes coroutines on the original `EventLoop`.
- `coro_epoll::UdpSocket`: non-blocking UDP socket with `SO_REUSEPORT` support.
- `coro_epoll::UdpEndpoint`: IPv4 address + port wrapper for UDP send/recv.

## Threading model

```text
worker thread N
    worker EventLoop N
        epoll_wait(eventfd + listen fd + client fds)
            accept()  ← SO_REUSEPORT, kernel distributes connections
                spawn client coroutine
            resume read/write coroutines

business thread pool
    run CPU-heavy/blocking tasks
    post coroutine continuation back to the original worker EventLoop
```

Each worker `EventLoop` owns its own `epoll` fd and a listening socket bound with `SO_REUSEPORT`. The kernel distributes incoming connections across all worker threads. A client socket stays on whichever worker accepted it, and all subsequent read/write events for that socket are handled by the same worker.

The client coroutine can offload expensive logic without blocking the network worker:

```cpp
std::string output = co_await business_pool.submit(loop, [input = std::move(input)]() mutable {
    return process_payload(std::move(input));
});
```

The submitted function runs on the business pool. When it completes, the coroutine is resumed through `loop.post(...)`, so subsequent socket writes still happen on the socket's owning network worker.

## Build

Run on Linux or macOS:

```bash
cmake -S . -B build
cmake --build build
```

## Examples

- [echo_server](examples/echo-server/README.md) — TCP echo server
- [udp_echo_server](examples/udp-echo-server/README.md) — UDP echo server
- [proxy_server](examples/proxy-server/README.md) — TCP proxy
- [http_server](examples/http-server/README.md) — Minimal HTTP/1.1 server
