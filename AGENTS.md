# Repository Guidelines

## Project Structure & Module Organization

This repository is organized around `HyperRedisCore`, the non-server core of the project. Public headers belong under `include/hyper/` and implementation files belong under `src/`. Keep modules separated by responsibility:

- `include/hyper/datastructures`, `src/datastructures`: low-level data structures
- `include/hyper/storage`, `src/storage`: storage engine and core object model
- `include/hyper/server`, `src/server`: future server-side code

`CMakeLists.txt` is the source of truth for build inputs. Add new `.cpp` files to the correct `set(...)` block and keep the lists explicit. Do not use `file(GLOB ...)`. Do not create placeholder `.cpp` or `.hpp` files before implementation starts.

## Build, Test, and Development Commands

- `cmake -S . -B build`: configure a local build directory
- `cmake --build build`: build the current targets
- `ctest --test-dir build`: run tests after CTest entries are added
- `cmake -S . -B /tmp/hyperredis-check`: quick clean configure check without reusing IDE output

At the current stage, `HyperRedisCore` may be configured as an `INTERFACE` placeholder until real core source files are added.

## Coding Style & Naming Conventions

Use modern C++ with C++20 as the default build standard. Keep code compatible with C++17/20 design where practical.

- Namespace: `hyper`
- Headers: `.hpp`
- Sources: `.cpp`
- File and directory names: `snake_case`
- Type names: `PascalCase`
- Keep indentation consistent at 4 spaces

Prefer small, focused headers and keep dependencies local to each module.

## Testing Guidelines

No test framework is wired in yet. When tests are introduced, place them under `tests/` and register them with CTest. Name files like `skip_list_test.cpp` or `database_test.cpp`. Cover normal cases, boundary cases, and invariants for each data structure or storage component.

## Commit & Pull Request Guidelines

Use short, imperative commit messages with a type prefix such as `feature`, `fix`, or `chore`. Examples:

- `feature: add skip list skeleton`
- `fix: correct object lifetime handling`
- `chore: update CMake source lists`

Pull requests should describe the behavior change, list touched modules, mention any `CMakeLists.txt` source-list updates, and include build or test evidence when available.
