# proxy_server

TCP proxy using `SO_REUSEPORT` across worker event loops, forwarding connections to a backend:

```bash
./build/proxy_server <listen_port> <backend_host> <backend_port> [worker_count]
```

```bash
# Proxy localhost:8888 → 127.0.0.1:3306 (MySQL)
./build/proxy_server 8888 127.0.0.1 3306
```

Uses `spawn` + `co_await` for concurrent bidirectional relay between client and backend.
