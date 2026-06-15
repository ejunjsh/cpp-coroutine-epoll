# http_server

Minimal HTTP/1.1 server with keep-alive support:

```bash
./build/http_server [port=8080] [worker_count]
```

```bash
./build/http_server 8080
```

Open `http://localhost:8080` in a browser. Routes:

| Path | Content |
|------|---------|
| `/` | HTML home page |
| `/hello` | Plain text greeting |
| `/json` | JSON status |
| Others | 404 Not Found |

Response content is loaded from `examples/http-server/static/` at startup — edit the HTML or JSON files without recompiling.
