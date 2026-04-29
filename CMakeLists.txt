cmake_minimum_required(VERSION 3.20)

project(HyperRedis LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(spdlog REQUIRED)
find_package(GTest REQUIRED)


set(STORAGE_ENGINE_SOURCES
    # src/storage/database.cpp
    # src/storage/object.cpp
)

set(SERVER_SOURCES
    # src/server/server.cpp
    # src/server/connection.cpp
)

set(HYPERREDIS_CORE_SOURCES
    ${STORAGE_ENGINE_SOURCES}
)

if (HYPERREDIS_CORE_SOURCES)
    add_library(HyperRedisCore STATIC ${HYPERREDIS_CORE_SOURCES})
    target_include_directories(HyperRedisCore
        PUBLIC
            ${PROJECT_SOURCE_DIR}/include
    )
    target_link_libraries(HyperRedisCore
        PUBLIC
            spdlog::spdlog
    )
else ()
    add_library(HyperRedisCore INTERFACE)
    target_include_directories(HyperRedisCore
        INTERFACE
            ${PROJECT_SOURCE_DIR}/include
    )
    target_link_libraries(HyperRedisCore
        INTERFACE
            spdlog::spdlog
    )
    message(STATUS "HyperRedisCore is configured as an interface placeholder until core source files are added.")
endif ()

enable_testing()

add_executable(linked_list_test
    test/linked_list_test.cpp
)

target_link_libraries(linked_list_test
    PRIVATE
        GTest::gtest
        HyperRedisCore
        spdlog::spdlog
)

add_executable(dict_test
    test/dict_test.cpp
)

target_link_libraries(dict_test
    PRIVATE
        GTest::gtest
        HyperRedisCore
        spdlog::spdlog
)

add_executable(skip_list_test
    test/skip_list_test.cpp
)

target_link_libraries(skip_list_test
    PRIVATE
        GTest::gtest
        HyperRedisCore
        spdlog::spdlog
)

add_executable(intset_test
    test/intset_test.cpp
)

target_link_libraries(intset_test
    PRIVATE
        GTest::gtest
        HyperRedisCore
        spdlog::spdlog
)

include(GoogleTest)
gtest_discover_tests(linked_list_test)
gtest_discover_tests(dict_test)
gtest_discover_tests(skip_list_test)
gtest_discover_tests(intset_test)
