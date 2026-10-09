cmake_minimum_required(VERSION 3.16)
project(redis_clone CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(MSVC)
    add_compile_options(/W4 /O2)
else()
    add_compile_options(-Wall -Wextra -O3)
endif()

include_directories(include)

set(CORE_SOURCES
    src/util.cpp
    src/skiplist.cpp
    src/dict.cpp
    src/resp.cpp
    src/eviction.cpp
    src/expire.cpp
    src/aof.cpp
    src/net.cpp
    src/server.cpp
)

add_executable(redis-server src/main.cpp ${CORE_SOURCES})

if(WIN32)
    target_link_libraries(redis-server ws2_32)
else()
    target_link_libraries(redis-server pthread)
endif()

add_executable(unit_tests
    tests/test_unit.cpp
    ${CORE_SOURCES}
)

if(WIN32)
    target_link_libraries(unit_tests ws2_32)
else()
    target_link_libraries(unit_tests pthread)
endif()
