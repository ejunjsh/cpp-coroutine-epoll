# udp_echo_server

UDP echo server using `SO_REUSEPORT` across worker event loops:

```bash
./build/udp_echo_server [port=8080] [worker_count] [business_worker_count]
```

```bash
./build/udp_echo_server 8080
nc -u 127.0.0.1 8080
```

Each UDP datagram received is echoed back to the sender from the same worker loop.
