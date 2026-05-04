# HyperRedis

`HyperRedis` 是一个为了配合阅读《Redis设计与实现（第二版）》而手写搭建的简易版 Redis 项目。

当前阶段先聚焦不带服务器的核心部分，核心库命名为 `HyperRedisCore`；未来在接入服务器实现后，再以 `HyperRedis` 作为完整系统的名称。

项目的第一目标是学习和理解 Redis 的整体设计、对象模型与关键数据结构，而不是在当前阶段追求工业级可用性。

## 当前约定

- 命名空间统一使用 `hyper`
- 当前默认编译标准为 C++20
- 设计与编码风格以现代 C++ 为主，尽量保持对 C++17/20 思路的兼容
- 当前阶段不单独实现 Redis 的 SDS；拥有型字符串使用 `std::string`，只读访问、参数传递和解析场景优先使用 `std::string_view`
- 正式日志统一使用 `spdlog`，不使用 `iostream` 作为长期日志方案
- 头文件统一使用 `.hpp`
- 源文件统一使用 `.cpp`
- `CMakeLists.txt` 中显式列出源文件，不使用 `GLOB`

### 命名约定

当前代码命名以“能从名字直接看出访问属性”为目标：

- 类型名优先贴近 Redis/书中概念；当前已有 `dict`、`skipList`、`intset` 等命名可继续保持
- public API 使用 `lowerCamelCase`，不加尾随下划线，例如 `insertOrAssign`、`forEach`、`byteSize`
- private 方法使用 `lowerCamelCase_`，加尾随下划线，例如 `rehashStep_`、`needRehash_`
- private 普通成员变量使用 `lower_snake_case_`，加尾随下划线，例如 `hash_tables_`、`rehash_index_`
- struct 的数据字段不加尾随下划线，例如 `dictEntry::key`、`dictHt::used`
- `static constexpr` 编译期常量使用 `PascalCase`，例如 `ExpandFactor`、`MinBucketSize`
- 文件 and 目录名继续使用 `snake_case`

## 实现原则

为了尽量贴近《Redis设计与实现》的主线，本项目遵循下面的原则：

- 保留 Redis 的整体框架、对象模型和关键算法
- 优先学习 Redis 的设计思想，而不是追求标准库容器的直接替换
- 只在不破坏核心结构理解的前提下，用现代 C++ 替代 C 语言时代为弥补语言短板而手写的部分
- 资源管理、字符串表达、日志接口等基础设施优先使用现代 C++ 和成熟库

一句话概括：保留 Redis 的“结构与算法”，替换 C 的“表达与资源管理方式”。

## Redis 到 HyperRedis 的映射

当前阶段计划按下面的策略推进：

| Redis/书中模块         | HyperRedis 策略 | 说明                                                 |
|--------------------|---------------|----------------------------------------------------|
| SDS                | 用标准库替代        | 使用 `std::string` 和 `std::string_view`，不单独实现 SDS    |
| `dict`             | 自己实现          | 这是 Redis 核心设计之一，不直接用 `std::unordered_map` 作为最终学习方案 |
| `skiplist`         | 自己实现          | 保留 Redis 有序集合相关核心结构的学习价值                           |
| `intset`           | 自己实现简化版       | 这是 Redis 小整数集合优化的重要思路                              |
| `ziplist` / 紧凑线性结构 | 自己实现简化版       | 书中的关键内容，后续可结合实际版本差异理解                              |
| `redisObject`      | 自己实现          | 保留“类型 + 编码 + 数据”的对象模型                              |
| `redisDb`          | 自己实现          | 保留“主字典 + 过期字典”的总体结构                                |
| 内存管理               | 用现代 C++ 替代    | 优先使用 RAII、值语义和智能指针，而不是 C 风格手工释放                    |
| 日志系统               | 用成熟库替代        | 使用 `spdlog`，不手写日志基础设施                              |
| 通用工具代码             | 用标准库替代        | 优先使用标准库算法、容器和类型工具，减少样板代码                           |

这里的“自己实现”并不意味着照搬 C 写法，而是保留相同的结构职责和算法思想，再用现代 C++ 表达。

## 目录规划

```text
include/
  hyper/
    datastructures/
    storage/
    server/
src/
  datastructures/
  storage/
  server/
```

目录职责如下：

- `datastructures`：底层数据结构
- `storage`：存储引擎与核心对象模型
- `server`：服务器相关实现

## 服务器计划

服务器部分后续计划基于 Muduo 搭建，但当前阶段还不会引入 Muduo，也不会在构建系统中接入服务器目标。

## 日志方案

当前项目日志系统选用 `spdlog`，依赖系统已经安装的库版本，并通过 CMake 直接链接到 `HyperRedisCore`。

当前日志设计约定如下：

- 正式日志输出统一走 `spdlog`
- `std::cout` / `std::cerr` 只保留给极早期启动失败或临时调试，不作为正式日志接口
- 当前阶段优先使用同步日志
- 在核心模块阶段优先使用简单、统一的默认 logger
- 等后续接入 Muduo 和多线程服务器后，再评估是否切换到多线程 sink 或异步日志

这一选择的目标是先保证日志接口统一、易于维护，并避免过早引入复杂的异步日志语义。

## 提交约定

提交信息使用简短的祈使句，并使用方括号类型前缀：

- `[feature] add intset core`
- `[fix] correct object lifetime handling`
- `[chore] update CMake source lists`

常用类型包括：

- `[feature]`：新增功能或新的数据结构能力
- `[fix]`：修复 bug、边界行为或资源管理问题
- `[chore]`：构建、文档、格式、依赖等维护性改动

## 依赖

- C++20 编译器
- CMake 3.20 及以上
- 系统已安装的 `spdlog`
- 系统已安装的 `GTest`

当前 CMake 通过 `find_package(spdlog REQUIRED)` 和 `find_package(GTest REQUIRED)` 查找系统库，并将
`spdlog::spdlog` 链接到 `HyperRedisCore`；各测试目标通过 `GTest::gtest` 接入 GoogleTest。

## 当前状态

仓库已经不再只是工程初始化状态，当前已经进入核心数据结构的第一阶段实现。

目前已完成的内容包括：

- **底层数据结构**：
    - `linked_list`：基础双向链表，支持双端操作、查找与遍历。
    - `dict`：支持渐进式 rehash 的哈希字典，支持透明字符串查找。
    - `skipList`：支持分值/排名双维度查询的高效跳表，已实现 C++20 透明查找优化。
    - `intset`：支持自动编码升级（Int16/32/64）的紧凑整数集合。
    - `ziplist`：初步实现紧凑字节布局，支持变长字符串 entry 编码。
- **核心对象模型 (RedisObject)**：
    - **String**：支持 `Raw` (std::string) 与 `Int` (long) 编码。实现了 `append`、`stringLen`、`incrBy` (支持自动转 Int)、`incrByFloat`、`getRange` (支持负数索引)、`setRange` (支持 \0 填充)。
    - **Hash**：基于 `dict` 实现，支持 `hset`/`hget` 及递归对象嵌套。
    - **List**：支持 `ZipList` 到 `LinkedList` 的透明自动升级，实现了双端推拉操作。
    - **Set**：支持 `IntSet` 到 `HashTable` 的透明自动升级，支持数值与字符串成员混合存储。
    - **ZSet**：支持 `ZipList` 到 `SkipList` 的透明自动升级。实现了 `zadd`、`zscore`、`zrem`、`zcard`、`zrank` 等核心语义。
- **工程化**：
    - 采用 **Passkey Pattern** 保证对象工厂的安全调用。
    - 针对 `std::unique_ptr` 配合 `std::variant` 处理不完整类型的挑战，设计了高效的解耦方案。
    - **测试保障**：已接入 GoogleTest，累计覆盖 **100+** 个测试用例，涵盖基础边界、编码升级、数据一致性等。

### ZSet 已具备的能力

- **双编码切换**：成员数量超过 16 个或成员长度超过 64 字节时，自动从 `ZipList` 升级到 `dict + skipList` 结构。
- **高效查询**：跳表支持 `getRank` 和 `erase` 等操作的 **透明查找 (Transparent Lookup)**，查找 `std::string_view` 无需构造临时字符串。
- **排序语义**：严格遵循 Redis 标准，分值相同时按成员名字典序排列。
- **核心辅助**：基于 `std::from_chars` 和 `std::to_chars` 实现了极高性能的 score 解析与格式化。

### String 已具备的能力

- **智能编码**：`incrBy` 操作时如果字符串是合法整数，会自动将编码由 `Raw` 优化为 `Int` 以节省内存并加速后续运算。
- **健壮性**：`incrBy` 系列操作在解析失败时能保持原数据不被破坏（通过 `std::optional` 错误处理机制）。
- **完全兼容**：`getRange` 严格遵循 Redis 风格的负数索引转换与边界截断逻辑；`setRange` 完美处理越界填充。

## 接下来的重点

- `redisDb`：实现多数据库管理、主字典与过期字典逻辑。
- 过期策略：实现惰性删除与定期删除思路。
- 持久化：初步探索 RDB 快照的 save/load 流程。
- 服务器接入：基于 Muduo 的网络层集成（后续阶段）。
