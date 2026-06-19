# udp_echo_server_et

UDP echo server using edge-triggered (ET) epoll/kqueue with `SO_REUSEPORT` across worker event loops:

```bash
./build/udp_echo_server_et [port=8080] [worker_count] [business_worker_count]
```

```bash
./build/udp_echo_server_et 8080
nc -u 127.0.0.1 8080
```

Key differences from `udp_echo_server` (LT):
- **`async_recv_from_et()`** — drains the socket buffer in one batch, returns `std::vector<UdpReceiveResult>`
- Each datagram is spawned as an independent coroutine (`handle_datagram`) for concurrent processing and echo
