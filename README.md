# HyperRedis

`HyperRedis` 是一个为了配合阅读《Redis设计与实现（第二版）》而手写搭建的简易版 Redis 项目。

当前阶段仍以核心库 `HyperRedisCore` 为主；核心库现在包含底层数据结构、存储对象模型、数据库管理和不依赖网络层的命令执行核心。未来接入 Muduo 网络层和完整服务器目标后，再以 `HyperRedis` 作为完整系统的名称。

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

服务器目录当前已经放入不依赖网络层的核心组件，包括 `RespValue`、`RedisClientContext` 和 `CommandExecutor`，并已随 `HyperRedisCore` 一起构建。当前仍没有独立的 Redis 网络服务器目标，也没有引入 Muduo。

后续服务器部分计划基于 Muduo 搭建，届时再补齐 RESP 解析/序列化、连接管理、事件循环和完整的可执行服务器目标。

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

仓库已经完成对象层、底层数据结构、数据库管理和第一轮命令执行核心。当前重点已经从“为 DB 和命令层打地基”推进到“补齐更多 Redis 命令语义，并为后续网络服务器接入做准备”。

目前已经完成的内容：

- **底层数据结构**：
  - `dict`：支持渐进式 rehash、透明字符串查找，并补强了随机 key 选择逻辑。
  - `skipList`：支持按 score / rank 的查询和区间删除，新增按 rank 的安全遍历接口。
  - `intset`：支持自动编码升级（Int16/32/64）的紧凑整数集合。
  - `ziplist`：支持连续区间删除，并用于 list/hash/zset 的紧凑编码。
  - `linked_list`：作为 list 的展开编码，支持双端操作与遍历。
- **核心对象层**：
  - `RedisObject` 已覆盖 String / Hash / List / Set / ZSet 的主要行为。
  - String 增加了溢出检查和浮点非法值处理。
  - Hash/List/Set/ZSet 都提供了更适合 DB 层调用的结果接口。
  - ZSet 现在区分新增、更新、无变化和非法 score，且 ziplist / skiplist 两种编码的行为已对齐。
- **数据库层**：
  - `RedisDb` 已实现主字典与过期字典，支持 `set/get/del/exists/type` 等基础访问。
  - 已实现 TTL/PTTL、PERSIST、惰性过期、主动过期周期、随机 key、rename 和清库能力。
  - `EXPIRE` / `PEXPIRE` 支持 `NX`、`XX`、`GT`、`LT` 条件选项。
  - `RedisManager` 已支持多 DB 管理、按索引访问、单库清空和全库清空。
- **命令执行核心**：
  - 已引入 `RespValue` 作为命令返回值模型。
  - 已引入 `RedisClientContext` 保存客户端当前 DB 选择状态。
  - `CommandExecutor` 已支持命令大小写归一、参数数量校验、命令分发表和 Redis 风格错误返回。
  - 已实现 `PING`、`SELECT`、`SET`、`GET`、`DEL`、`EXISTS`、`TYPE`、`TTL`、`PTTL`、`PERSIST`、`EXPIRE`、`PEXPIRE`。
  - 已实现 `DBSIZE`、`FLUSHDB`、`FLUSHALL`、`RANDOMKEY`、`RENAME`、`RENAMENX`。
  - 已实现 String 相关的 `MGET`、`MSET`、`STRLEN`、`APPEND`、`INCR`、`DECR`、`INCRBY`、`INCRBYFLOAT`。
  - List/Hash/Set/ZSet 的命令分发入口已经预留，后续继续补齐具体命令语义。
- **测试**：
  - 已接入 GoogleTest，测试目录位于 `test/`。
  - 当前测试覆盖基础结构、编码切换、对象行为、DB 行为、命令执行器和 Redis 可见语义。
  - 新增了 `redis_object_redis_behavior_test.cpp`，专门对照 Redis 的外部行为做回归验证。
  - 新增了 `database_test.cpp`、`redis_manager_test.cpp`、`client_context_test.cpp` 和 `command_executor_test.cpp`，覆盖 DB、客户端上下文和命令层行为。

## 接下来的重点

- 命令层：继续补齐 List/Hash/Set/ZSet 的 Redis 命令语义。
- RESP：补齐请求解析和响应序列化，让命令执行器可以接入真实连接。
- 过期策略：继续打磨惰性删除与定期删除的调度策略。
- 持久化：初步探索 RDB 快照的 save/load。
- 服务器接入：后续接入 Muduo 和网络层，形成完整 Redis-like 服务。
