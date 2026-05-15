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

This project is also a learning project. When working on Redis data structures, storage components,
RESP/server code, persistence, or command execution, prefer a training-oriented workflow unless the
user explicitly asks for direct implementation.

Default stance: the user should own production-code implementation as much as possible. The assistant
should teach the Redis idea, invariants, byte/layout model, state transitions, and edge cases first,
then use tests and review to help the user tighten the implementation details.

Use this loop for learning-heavy work:

- Define the small slice to build and the Redis concept it teaches.
- Explain the core invariants before code is written.
- Add or propose focused GoogleTest red/green checkpoints when useful.
- Let the user implement production code in headers or sources first.
- Review the user's implementation for correctness, undefined behavior, layout mistakes, parser state
  mistakes, naming/style issues, and future extensibility.
- Prefer hints, failing tests, and invariant-based explanations before directly patching production code.
- Ask the user to fix the first one or two production-code iterations when the remaining issue is
  educational and reasonably small.
- Directly edit production code only when the user asks, when the user is blocked after iterative review,
  or when the change is narrow mechanical cleanup after the implementation is working.

The assistant may still directly edit documentation, tests, CMake wiring, and small mechanical style
issues. Do not rewrite the user's working production implementation just to impose style.

For larger Redis-inspired structures such as `ziplist`, preserve the learning sequence: first teach the
simplified model, then add Redis-like encoding details, then add edge cases such as cascading updates.

For AOF and future server work, keep the Redis lifecycle boundaries explicit: command parsing, command
execution, write-command classification, append-only logging, replay, rewrite, client query buffers,
reply buffers, and event-loop dispatch should be discussed as separate responsibilities before code is
introduced.

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

When the user explicitly asks to commit and push, use the direct release path:

- Run the relevant build/test command immediately, usually `cmake --build build` followed by `ctest --test-dir build --output-on-failure`.
- If tests pass, stage the current requested worktree changes, commit with the project message style, and push to the current branch's upstream without extra review loops.
- Do not make opportunistic cleanup, formatting, refactors, newline normalization, or unrelated fixes during a commit/push request unless the user specifically asks for them.
- If tests fail or git/network permissions block the operation, report the concrete blocker and stop instead of expanding the task.

Pull requests should describe the behavior change, list touched modules, mention any `CMakeLists.txt` source-list updates, and include build or test evidence when available.
