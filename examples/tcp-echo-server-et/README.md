# tcp_echo_server_et

TCP echo server using edge-triggered (ET) epoll/kqueue with `SO_REUSEPORT` across worker event loops:

```bash
./build/tcp_echo_server_et [port=8888] [worker_count] [business_worker_count]
```

```bash
./build/tcp_echo_server_et 8888
nc 127.0.0.1 8888
```

Key differences from `tcp_echo_server` (LT):
- **`async_accept_et()`** — drains the accept queue in one batch, returns `std::vector<TcpSocket>`
- **`async_read_et()`** — edge-triggered read, loops `recv` until `EAGAIN` to drain the socket buffer
- Write stays LT (`async_write`) to avoid the classic ET write race
