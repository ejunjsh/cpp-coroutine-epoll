#include "coro_epoll/event_loop.hpp"
#include "coro_epoll/tcp.hpp"
#include "coro_epoll/worker_group.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <exception>
#include <iostream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>

using coro_epoll::EventLoop;
using coro_epoll::Task;
using coro_epoll::TcpServer;
using coro_epoll::TcpSocket;
using coro_epoll::WorkerGroup;

// ---------------------------------------------------------------------------
// Minimal HTTP request
// ---------------------------------------------------------------------------
struct HttpRequest {
    std::string method;
    std::string path;
    std::string version;
    std::unordered_map<std::string, std::string> headers;
};

// ---------------------------------------------------------------------------
// Minimal HTTP response builder
// ---------------------------------------------------------------------------
struct HttpResponse {
    int status = 200;
    std::string status_text = "OK";
    std::string content_type = "text/plain";
    std::string body;
    std::unordered_map<std::string, std::string> headers;

    std::string to_string() const {
        std::string result;
        result += "HTTP/1.1 ";
        result += std::to_string(status);
        result += ' ';
        result += status_text;
        result += "\r\n";
        result += "Content-Type: ";
        result += content_type;
        result += "\r\n";
        result += "Content-Length: ";
        result += std::to_string(body.size());
        result += "\r\n";
        result += "Connection: keep-alive\r\n";
        for (const auto& [key, value] : headers) {
            result += key;
            result += ": ";
            result += value;
            result += "\r\n";
        }
        result += "\r\n";
        result += body;
        return result;
    }
};

// ---------------------------------------------------------------------------
// Line-based reader for HTTP parsing
// ---------------------------------------------------------------------------
Task<std::string> read_line(TcpSocket& socket) {
    std::string line;
    std::array<char, 1> ch{};
    while (true) {
        const std::size_t count = co_await socket.async_read(ch.data(), 1);
        if (count == 0) {
            co_return line;
        }
        line += ch[0];
        if (line.size() >= 2 && line[line.size() - 2] == '\r' && line[line.size() - 1] == '\n') {
            line.resize(line.size() - 2); // strip \r\n
            co_return line;
        }
    }
}

// ---------------------------------------------------------------------------
// Parse the HTTP request line and headers
// ---------------------------------------------------------------------------
Task<HttpRequest> parse_request(TcpSocket& socket) {
    HttpRequest request;

    // Request line: METHOD SP PATH SP VERSION
    const std::string request_line = co_await read_line(socket);
    if (request_line.empty()) {
        throw std::runtime_error("empty request line");
    }

    {
        std::size_t pos = 0;
        const std::size_t sp1 = request_line.find(' ');
        if (sp1 == std::string::npos) {
            throw std::runtime_error("malformed request line");
        }
        request.method = request_line.substr(0, sp1);
        pos = sp1 + 1;

        const std::size_t sp2 = request_line.find(' ', pos);
        if (sp2 == std::string::npos) {
            request.path = request_line.substr(pos);
            request.version = "HTTP/1.0";
        } else {
            request.path = request_line.substr(pos, sp2 - pos);
            request.version = request_line.substr(sp2 + 1);
        }
    }

    // Headers
    while (true) {
        const std::string header_line = co_await read_line(socket);
        if (header_line.empty()) {
            break; // end of headers
        }
        const std::size_t colon = header_line.find(':');
        if (colon == std::string::npos) {
            continue; // skip malformed headers
        }
        std::string key = header_line.substr(0, colon);
        std::string value = header_line.substr(colon + 1);
        // Trim leading whitespace from value
        const auto start = value.find_first_not_of(" \t");
        if (start != std::string::npos) {
            value = value.substr(start);
        }
        // Store lowercase key
        std::transform(key.begin(), key.end(), key.begin(), ::tolower);
        request.headers[std::move(key)] = std::move(value);
    }

    co_return request;
}

// ---------------------------------------------------------------------------
// Route handler: build response for a given request
// ---------------------------------------------------------------------------
HttpResponse handle_route(const HttpRequest& request) {
    HttpResponse response;

    static const std::string home_html = R"(<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>cpp-coroutine-epoll HTTP Server</title>
    <style>
        body { font-family: -apple-system, sans-serif; max-width: 600px; margin: 80px auto; padding: 0 20px; }
        h1 { color: #333; }
        p { color: #666; line-height: 1.6; }
        code { background: #f0f0f0; padding: 2px 6px; border-radius: 3px; }
        ul { color: #666; }
        a { color: #0366d6; }
    </style>
</head>
<body>
    <h1>cpp-coroutine-epoll HTTP Server</h1>
    <p>A C++20 coroutine HTTP server running on <code>epoll</code>/<code>kqueue</code>.</p>
    <p>Try these paths:</p>
    <ul>
        <li><a href="/hello">/hello</a> — plain text greeting</li>
        <li><a href="/json">/json</a> — JSON response</li>
    </ul>
    <p><em>Powered by C++20 coroutines</em></p>
</body>
</html>)";

    if (request.method != "GET") {
        response.status = 405;
        response.status_text = "Method Not Allowed";
        response.content_type = "text/plain";
        response.body = "405 Method Not Allowed\n";
        return response;
    }

    if (request.path == "/") {
        response.status = 200;
        response.status_text = "OK";
        response.content_type = "text/html; charset=utf-8";
        response.body = home_html;
    } else if (request.path == "/hello") {
        response.status = 200;
        response.status_text = "OK";
        response.content_type = "text/plain; charset=utf-8";
        response.body = "Hello, World!\n";
    } else if (request.path == "/json") {
        response.status = 200;
        response.status_text = "OK";
        response.content_type = "application/json";
        response.body = R"({"status":"ok","server":"cpp-coroutine-epoll"})";
        response.body += '\n';
    } else {
        response.status = 404;
        response.status_text = "Not Found";
        response.content_type = "text/plain; charset=utf-8";
        response.body = "404 Not Found\n";
    }

    return response;
}

// ---------------------------------------------------------------------------
// Per-client HTTP coroutine
// ---------------------------------------------------------------------------
Task<void> handle_client(EventLoop& /*loop*/, TcpSocket socket) {
    try {
        while (true) {
            // Parse the HTTP request
            HttpRequest request = co_await parse_request(socket);
            std::cout << "  " << request.method << ' ' << request.path << '\n';

            // Build and send the response
            HttpResponse response = handle_route(request);
            const std::string raw = response.to_string();
            co_await socket.async_write(raw.data(), raw.size());

            // Keep the connection alive, loop back for the next request.
            // The socket is closed when the client disconnects or an error occurs.
        }
    } catch (const std::exception& error) {
        std::cerr << "http client error: " << error.what() << '\n';
    }
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------
Task<void> accept_loop(EventLoop& accept_loop, TcpServer& server, WorkerGroup& workers) {
    try {
        while (true) {
            const int client_fd = co_await server.async_accept_fd();
            EventLoop& worker = workers.next();
            worker.post([client_fd, &worker] {
                worker.spawn(handle_client(worker, TcpSocket(worker, client_fd)));
            });
        }
    } catch (const std::exception& error) {
        std::cerr << "accept loop error: " << error.what() << '\n';
        workers.stop();
        accept_loop.stop();
    }
}

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------
std::uint16_t parse_port(int argc, char** argv) {
    if (argc < 2) {
        return 8080;
    }

    std::uint32_t port = 0;
    const std::string_view value(argv[1]);
    const auto [ptr, error] = std::from_chars(value.data(), value.data() + value.size(), port);
    if (error != std::errc{} || ptr != value.data() + value.size() || port == 0 || port > 65535) {
        throw std::runtime_error("port must be an integer between 1 and 65535");
    }
    return static_cast<std::uint16_t>(port);
}

std::size_t parse_worker_count(int argc, char** argv) {
    const unsigned int hardware_workers = std::thread::hardware_concurrency();
    const std::size_t default_workers = hardware_workers == 0 ? 4 : hardware_workers;
    if (argc < 3) {
        return default_workers;
    }

    std::size_t worker_count = 0;
    const std::string_view value(argv[2]);
    const auto [ptr, error] = std::from_chars(value.data(), value.data() + value.size(), worker_count);
    if (error != std::errc{} || ptr != value.data() + value.size() || worker_count == 0) {
        throw std::runtime_error("worker count must be a positive integer");
    }
    return worker_count;
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
int main(int argc, char** argv) {
    try {
        const std::uint16_t port = parse_port(argc, argv);
        const std::size_t worker_count = parse_worker_count(argc, argv);

        EventLoop accept_event_loop;
        WorkerGroup workers{worker_count};
        TcpServer server(accept_event_loop);
        server.listen(port);

        std::cout << "http server listening on http://0.0.0.0:" << port
                  << " with " << worker_count << " worker(s)\n";

        accept_event_loop.spawn(accept_loop(accept_event_loop, server, workers));
        accept_event_loop.run();
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "fatal: " << error.what() << '\n';
        return 1;
    }
}
