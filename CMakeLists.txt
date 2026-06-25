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
        src/storage/aof_appender.cpp
        src/storage/aof_replayer.cpp
        src/storage/aof_rewriter.cpp
)

set(SERVER_CORE_SOURCES
        src/server/client_context.cpp
        src/server/command_executor.cpp
        src/server/resp_codec.cpp
        src/server/command_registry.cpp
        src/server/command_processor.cpp
        src/server/event_loop.cpp
        src/server/client_session.cpp
        src/server/client_socket_io.cpp
        src/server/redis_server.cpp
        src/server/tcp_listener.cpp
        src/server/redis_server_runner.cpp
        src/server/server_options.cpp
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

add_executable(hyper_redis_server
        apps/hyperredis_server.cpp
)

target_include_directories(hyper_redis_server
        PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(hyper_redis_server
        PRIVATE
        HyperRedisCore
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
        object_command_test
        time_command_test
        zset_test
        object_stress_test
        redis_object_redis_behavior_test
        database_test
        redis_manager_test
        snapshot_test
        rdb_saver_test
        aof_appender_test
        aof_replayer_test
        aof_rewriter_test
        client_context_test
        client_test
        client_socket_io_test
        command_processor_test
        command_executor_test
        resp_codec_test
        event_loop_test
        tcp_listener_test
        redis_server_runner_test
        redis_server_test
        server_options_test
        pubsub_state_test
)

foreach (test_target IN LISTS HYPERREDIS_TESTS)
    add_hyperredis_test(${test_target})
endforeach ()
