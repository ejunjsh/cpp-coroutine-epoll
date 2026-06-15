# echo_server

```bash
./build/echo_server [port=8888] [worker_count] [business_worker_count]
```

```bash
./build/echo_server 8888
nc 127.0.0.1 8888
```

Every line typed in `nc` is echoed back by a coroutine-managed client handler.
