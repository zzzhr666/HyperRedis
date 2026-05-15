cmake_minimum_required(VERSION 3.20)

project(HyperRedis LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(spdlog REQUIRED)
find_package(GTest REQUIRED)

set(STORAGE_ENGINE_SOURCES
        src/storage/object.cpp
        src/storage/database.cpp
        src/storage/redis_manager.cpp
        src/storage/snapshot.cpp
        src/storage/rdb_saver.cpp
        src/storage/checksum_calculator.cpp
)

set(SERVER_CORE_SOURCES
        src/server/client_context.cpp
        src/server/command_executor.cpp
        src/server/resp_codec.cpp
)

set(HYPERREDIS_CORE_SOURCES
        ${STORAGE_ENGINE_SOURCES}
        ${SERVER_CORE_SOURCES}
)

add_library(HyperRedisCore STATIC
        ${HYPERREDIS_CORE_SOURCES}
)

target_include_directories(HyperRedisCore
        PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(HyperRedisCore
        PUBLIC
        spdlog::spdlog
)

enable_testing()
include(GoogleTest)

function(add_hyperredis_test target)
    add_executable(${target}
            test/${target}.cpp
    )

    target_link_libraries(${target}
            PRIVATE
            HyperRedisCore
            GTest::gtest_main
            spdlog::spdlog
    )

    gtest_discover_tests(${target})
endfunction()

set(HYPERREDIS_TESTS
        linked_list_test
        dict_test
        skip_list_test
        intset_test
        ziplist_test
        object_test
        zset_test
        object_stress_test
        redis_object_redis_behavior_test
        database_test
        redis_manager_test
        snapshot_test
        rdb_saver_test
        client_context_test
        command_executor_test
        resp_codec_test
)

foreach (test_target IN LISTS HYPERREDIS_TESTS)
    add_hyperredis_test(${test_target})
endforeach ()
