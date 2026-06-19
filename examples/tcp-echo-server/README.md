# echo_server

TCP echo server using `SO_REUSEPORT` across worker event loops:

```bash
./build/tcp_echo_server [port=8888] [worker_count] [business_worker_count]
```

```bash
./build/tcp_echo_server 8888
nc 127.0.0.1 8888
```

Every line typed in `nc` is echoed back by a coroutine-managed client handler. CPU-heavy or blocking business logic can be offloaded to a separate `ThreadPool`.
