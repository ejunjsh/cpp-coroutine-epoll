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
    src/main.cpp
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
