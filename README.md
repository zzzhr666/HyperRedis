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

## 依赖

- C++20 编译器
- CMake 4.1 及以上
- 系统已安装的 `spdlog`

当前 CMake 通过 `find_package(spdlog REQUIRED)` 查找系统库，并将 `spdlog::spdlog` 链接到 `HyperRedisCore`。

## 当前状态

仓库已经不再只是工程初始化状态，当前已经进入核心数据结构的第一阶段实现。

目前已完成的内容包括：

- `linked_list`：完成基础双向链表实现，并接入 GoogleTest
- `dict`：完成基础版哈希字典实现，并保留 Redis 风格的双表与渐进式 rehash 思路
- 测试基础设施：已接入 `GTest`，当前为 `linked_list` 和 `dict` 提供测试可执行文件

当前 `dict` 已具备的能力包括：

- 链地址法冲突处理
- `insert` / `insertOrAssign` / `erase` / `contains` / `get`
- 自动扩容与自动缩容
- 渐进式 rehash
- 基于 `forEach` 的遍历接口

目前 `HyperRedisCore` 仍然以头文件为主，因此 CMake 中核心库目标仍可以保持轻量；随着后续 `redisObject`、`redisDb` 和持久化相关 `.cpp` 文件加入，再逐步演进为更完整的核心库实现。

接下来的重点将从底层数据结构转向：

- `redisObject`
- `redisDb`
- 简化版持久化流程（如 RDB save/load）

在字符串实现上，本项目当前不会以 1:1 复刻 Redis SDS 为阶段目标，而是优先复用标准库字符串抽象；但在 `dict`、`skiplist`、`redisObject`、`redisDb` 等真正承载 Redis 核心设计的部分，仍然会优先保留书中的结构和思路。
