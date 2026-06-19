cmake_minimum_required(VERSION 3.20)

project(cpp_coroutine_epoll LANGUAGES CXX)

if(NOT (CMAKE_SYSTEM_NAME STREQUAL "Linux" OR APPLE))
    message(FATAL_ERROR "cpp-coroutine-epoll requires Linux epoll or macOS kqueue support")
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

set(EXAMPLES
    tcp_echo_server:examples/tcp-echo-server/main.cpp
    proxy_server:examples/proxy-server/main.cpp
    http_server:examples/http-server/main.cpp
    udp_echo_server:examples/udp-echo-server/main.cpp
    tcp_echo_server_et:examples/tcp-echo-server-et/main.cpp
    udp_echo_server_et:examples/udp-echo-server-et/main.cpp
)

foreach(example IN LISTS EXAMPLES)
    string(REPLACE ":" ";" example_parts ${example})
    list(GET example_parts 0 example_name)
    list(GET example_parts 1 example_source)

    add_executable(${example_name} ${example_source})

    target_include_directories(${example_name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    )

    target_link_libraries(${example_name} PRIVATE Threads::Threads)

    target_compile_options(${example_name} PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )
endforeach()
