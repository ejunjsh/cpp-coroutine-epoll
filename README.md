# C++20 Coroutine Event Loop

![CMake Build](https://github.com/ejunjsh/cpp-coroutine-epoll/actions/workflows/cmake-multi-platform.yml/badge.svg)

A C++20 coroutine-based networking library that wraps readiness events with `co_await`. Linux uses `epoll` + `eventfd`; macOS uses `kqueue` + `pipe`. All I/O is non-blocking, and coroutines are suspended/resumed by the event loop without any callback nesting.

## Library

### `coro_epoll::Task<T>`

The coroutine return type. A `Task<T>` bridges the synchronous world (event loop) with `co_await`-based async code.

- `Task<T>` for coroutines that produce a value; `Task<void>` for fire-and-forget or side-effect-only coroutines.
- Move-only: each `Task` uniquely owns a coroutine handle. Destroying a `Task` destroys the coroutine.
- `co_await task` suspends the caller and resumes when the inner coroutine completes, propagating the result (or exception).
- `promise_type::initial_suspend()` returns `suspend_always` — newly created tasks are lazily started via `EventLoop::spawn()`.

### `coro_epoll::EventLoop`

Single-threaded reactor. Owns one `epoll` (Linux) or `kqueue` (macOS) fd.

| Method | Description |
|--------|-------------|
| `spawn(Task<void>&&)` | Start a coroutine on this loop. |
| `post(std::function<void()>)` | Enqueue a callback from another thread. Thread-safe. |
| `run()` | Blocking event loop. Returns when `stop()` is called. |
| `stop()` | Signal the loop to exit. Thread-safe. |
| `readable(int fd)` | Returns an awaiter that suspends until `fd` is readable. |
| `writable(int fd)` | Returns an awaiter that suspends until `fd` is writable. |
| `remove(int fd)` | Deregister `fd` and clean up associated coroutine handles. |

`post()` enables cross-thread scheduling: a business thread can post a continuation back to the owning `EventLoop`, keeping socket I/O on the correct thread.

### `coro_epoll::WorkerGroup`

Owns N `EventLoop` instances, each running on its own thread. Provides round-robin access via `next()`.

| Method | Description |
|--------|-------------|
| `next()` | Returns the next `EventLoop&` in round-robin order. Thread-safe. |
| `stop()` / `join()` | Gracefully shut down all worker loops. |

Combined with `SO_REUSEPORT`, each worker can bind its own listening socket and the kernel distributes incoming connections.

### `coro_epoll::ThreadPool`

Runs CPU-heavy or blocking tasks off the network threads. When a task completes, the coroutine continuation is posted back to the originating `EventLoop`.

```cpp
std::string result = co_await pool.submit(loop, [] {
    return expensive_computation();
});
// Resumed on 'loop' — safe to read/write sockets here.
```

### `coro_epoll::TcpServer`

Non-blocking TCP listening socket, bound to an `EventLoop`.

| Method | Description |
|--------|-------------|
| `listen(port, backlog, reuse_port)` | Create, bind, and listen. `reuse_port` enables `SO_REUSEPORT`. |
| `async_accept_fd()` | `co_await` a new client fd. |
| `async_accept()` | `co_await` a new `TcpSocket`. |

### `coro_epoll::TcpSocket`

Non-blocking connected TCP socket. Move-only; destructor closes the fd and deregisters from the `EventLoop`.

| Method | Description |
|--------|-------------|
| `async_read(buffer, size)` | `co_await` until data arrives. Returns bytes read, or `0` on EOF. |
| `async_write(buffer, size)` | `co_await` until all bytes are written. Partial writes are handled transparently. |

### `coro_epoll::UdpSocket`

Non-blocking UDP socket.

| Method | Description |
|--------|-------------|
| `bind(port, reuse_port)` | Create and bind. Supports `SO_REUSEPORT`. |
| `async_recv_from(buffer, size)` | `co_await` a datagram. Returns `UdpReceiveResult{size, endpoint}`. |
| `async_send_to(buffer, size, endpoint)` | `co_await` until the datagram is sent. |

### `coro_epoll::UdpEndpoint`

Immutable IPv4 address + port. Construct from `(string_view address, uint16_t port)`, or from a raw `sockaddr_in`.

## Threading model

```text
worker thread N
    EventLoop N
        epoll_wait(eventfd + listen fd + client fds)
            accept()  ← SO_REUSEPORT, kernel distributes connections
                spawn client coroutine
            resume read/write coroutines

business thread pool
    run CPU-heavy / blocking tasks
    post continuation back to the originating EventLoop via loop.post()
```

Each `EventLoop` owns its `epoll`/`kqueue` fd and runs on a dedicated thread. A client socket stays on whichever worker accepted it — all subsequent I/O for that socket is handled by the same worker, so no locking is needed for socket state.

## Build

```bash
cmake -S . -B build
cmake --build build
```

## Examples

- [echo_server](examples/echo-server/README.md) — TCP echo server
- [udp_echo_server](examples/udp-echo-server/README.md) — UDP echo server
- [proxy_server](examples/proxy-server/README.md) — TCP proxy
- [http_server](examples/http-server/README.md) — Minimal HTTP/1.1 server
