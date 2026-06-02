# C++20 Coroutine Event Loop

![CMake Build](https://github.com/ejunjsh/cpp-coroutine-epoll/actions/workflows/cmake-multi-platform.yml/badge.svg)

Small Linux/macOS networking example that wraps readiness events with C++20 coroutines. Linux uses 'epoll' plus 'eventfd'; macOS uses 'kqueue' plus 'pipe'. The server uses one acceptor `EventLoop` in the main thread, one `EventLoop` per network worker thread for connected clients, and a separate business `ThreadPool` for CPU-heavy or blocking work.

## Structure

- `coro_epoll::Task<T>`: coroutine return type.
- `coro_epoll::EventLoop`: single-threaded reactor with Linux 'epoll'/'eventfd' or macOS 'kqueue'/'pipe' wakeup and cross-thread 'post()'.
- `coro_epoll::WorkerGroup`: owns one worker `EventLoop` per network worker thread.
- `coro_epoll::ThreadPool`: runs heavy work away from the network loops.
- `coro_epoll::TcpServer`: non-blocking listening socket.
- `coro_epoll::TcpSocket`: wraps fd, registers interest with EventLoop and resumes coroutines on the original `EventLoop`.
- `echo_server`: sample echo server, connected TCP socket.

## Threading model

```text
main thread
    accept EventLoop
        epoll/wait/listen fd
            accept()
                round robin dispatch client fd to worker.post(...)

worker thread N
    worker EventLoop N
        epoll_wait(eventfd + client fds)
            spawn client coroutine
            resume read/write coroutines

business thread pool
    run CPU-heavy/blocking tasks
    post coroutine continuation back to the original worker EventLoop
```

Each `EventLoop` owns its own `epoll` fd. A client socket is assigned to exactly one worker loop, and later read/write readiness for that socket is handled by that worker thread.

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

## Run

```bash
./build/echo_server 8888
nc 127.0.0.1 8888
```

The optional second argument sets the network worker count:

```bash
./build/echo_server 8888 4
```

The optional third argument sets the business worker count:

```bash
./build/echo_server 8888 4 8
```

Every line typed in `nc` is echoed back by a coroutine-managed client handler.
