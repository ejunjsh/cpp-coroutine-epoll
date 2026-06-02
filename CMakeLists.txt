cmake_minimum_required(VERSION 3.20)

project(cpp_coroutine_epoll LANGUAGES CXX)

if(NOT (CMAKE_SYSTEM_NAME STREQUAL "Linux" OR APPLE))
    message(FATAL_ERROR "cpp-coroutine-epoll requires Linux epoll or macOS kqueue support")
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_executable(echo_server
    examples/echo-server/main.cpp
)

target_include_directories(echo_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(echo_server PRIVATE Threads::Threads)

target_compile_options(echo_server PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)

add_executable(proxy_server
    examples/proxy-server/main.cpp
)

target_include_directories(proxy_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(proxy_server PRIVATE Threads::Threads)

target_compile_options(proxy_server PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)

add_executable(http_server
    examples/http-server/main.cpp
)

target_include_directories(http_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(http_server PRIVATE Threads::Threads)

target_compile_options(http_server PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)
