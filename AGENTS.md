# Repository Guidelines

## Project Structure & Module Organization

This repository is organized around `HyperRedisCore`, the non-server core of the project. Public headers belong under `include/hyper/` and implementation files belong under `src/`. Keep modules separated by responsibility:

- `include/hyper/datastructures`, `src/datastructures`: low-level data structures
- `include/hyper/storage`, `src/storage`: storage engine and core object model
- `include/hyper/server`, `src/server`: future server-side code
- `test`: GoogleTest-based unit, stress, and Redis-behavior tests

`CMakeLists.txt` is the source of truth for build inputs. Add new `.cpp` files to the correct `set(...)` block and keep the lists explicit. Do not use `file(GLOB ...)`. Do not create placeholder `.cpp` or `.hpp` files before implementation starts.

## Build, Test, and Development Commands

- `cmake -S . -B build`: configure a local build directory
- `cmake --build build`: build the current targets
- `ctest --test-dir build`: run all registered GoogleTest/CTest tests
- `cmake -S . -B /tmp/hyperredis-check`: quick clean configure check without reusing IDE output

`HyperRedisCore` is currently a static core library built from the storage/object implementation and header-only data structures. Server targets are intentionally not wired yet.

## Collaboration Mode

This project is also a learning project. When working on Redis data structures or storage components, prefer the following teaching-oriented workflow unless the user explicitly asks for direct implementation:

- Explain the design idea, invariants, byte layout, and edge cases before code is written.
- Split work into small steps that can be finished and tested in one sitting.
- Let the user implement production code in headers or sources.
- Review the user's implementation for correctness, undefined behavior, layout mistakes, naming/style issues, and future extensibility.
- Write or adjust GoogleTest test files and CMake test wiring when needed.
- Use tests as red/green checkpoints: add focused failing tests first, then let the user implement, then verify.
- Do not rewrite the user's production implementation just to impose style; suggest fixes first unless the user asks for cleanup.
- It is acceptable for the assistant to edit documentation, tests, and narrow mechanical style issues after implementation is working.

For larger Redis-inspired structures such as `ziplist`, preserve the learning sequence: first teach the simplified model, then add Redis-like encoding details, then add edge cases such as cascading updates.

## Coding Style & Naming Conventions

Use modern C++ with C++20 as the default build standard. Keep code compatible with C++17/20 design where practical.

- Namespace: `hyper`
- Headers: `.hpp`
- Sources: `.cpp`
- File and directory names: `snake_case`
- Keep indentation consistent at 4 spaces
- Type names should stay close to Redis/book concepts already used in the codebase, e.g. `dict`, `skipList`, `intset`
- Public APIs use `lowerCamelCase` without a trailing underscore, e.g. `insertOrAssign`, `forEach`, `byteSize`
- Private helper methods use `lowerCamelCase_` with a trailing underscore, e.g. `rehashStep_`, `needRehash_`
- Private ordinary data members use `lower_snake_case_` with a trailing underscore, e.g. `hash_tables_`, `rehash_index_`
- Struct data fields do not use trailing underscores, e.g. `entry.key`, `ht.used`
- `static constexpr` compile-time constants use `PascalCase`, e.g. `ExpandFactor`, `MinBucketSize`

Prefer small, focused headers and keep dependencies local to each module.

## Testing Guidelines

GoogleTest is wired through CMake and discovered by CTest. Place tests under `test/` and register new test executables explicitly in `CMakeLists.txt`. Name files like `skip_list_test.cpp`, `redis_object_redis_behavior_test.cpp`, or `database_test.cpp`. Cover normal cases, boundary cases, encoding transitions, and invariants for each data structure or storage component.

Current coverage includes low-level data structures, RedisObject behavior, stress tests, and Redis-visible behavior checks. When changing object-layer behavior, run the full suite with `ctest --test-dir build` or a clean `/tmp/hyperredis-check` build.

## Commit & Pull Request Guidelines

Use short, imperative commit messages with a bracketed type prefix such as `[feature]`, `[fix]`, or `[chore]`. Examples:

- `[feature] add skip list skeleton`
- `[fix] correct object lifetime handling`
- `[chore] update CMake source lists`

Pull requests should describe the behavior change, list touched modules, mention any `CMakeLists.txt` source-list updates, and include build or test evidence when available.
