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
- 文件和目录名继续使用 `snake_case`

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

- `linked_list`：完成基础双向链表实现，支持头尾插入删除、查找、删除、遍历和移动语义
- `dict`：完成基础版哈希字典实现，并保留 Redis 风格的双表与渐进式 rehash 思路
- `skipList`：完成基础跳表实现，支持有序插入、删除、排名查询、按分值范围查询与范围删除
- `intset`：完成简化版整数集合实现，支持有序去重、二分查找、编码升级和紧凑字节存储
- `redisObject`：完成核心对象模型实现，采用现代 C++ 的 `std::variant` + `std::unique_ptr` 架构，支持类型与编码分离。
- 测试基础设施：已接入 `GTest`，为所有核心组件提供测试，累计覆盖 84 个用例。

当前 `redisObject` 已具备的能力包括：

- **String 类型**：支持 `Raw` (std::string) 和 `Int` (long) 编码，支持 `append` 操作时的自动解码（Int -> Raw）。
- **Hash 类型**：支持基于 `dict` 的对象存储，实现了 `hset` / `hget` 接口，支持对象递归嵌套。
- **List 类型**：支持 `ZipList` 和 `LinkedList` 双编码，实现了 `lpush`/`rpush`/`lpop`/`rpop`，支持从 `ZipList` 到 `LinkedList` 的透明自动升级。
- **封装性**：采用 Passkey Pattern (Token 模式) 保证工厂方法的唯一性，并解决了 `std::unique_ptr` 处理不完整类型的架构难题。

当前 `dict` 已具备的能力包括：

- 链地址法冲突处理
- `insert` / `insertOrAssign` / `erase` / `contains` / `get`
- 自动扩容与自动缩容
- 渐进式 rehash
- 基于 `forEach` 的遍历接口

当前 `skipList` 已具备的能力包括：

- 按 `(score, value)` 排序并拒绝完全重复元素
- 维护 span 并支持 `getRank` / `getElementByRank`
- 支持分值范围判断、首尾范围查找
- 支持按 score 范围删除和按 rank 范围删除

当前 `intset` 已具备的能力包括：

- 使用 `std::vector<std::byte>` 保存连续紧凑整数数组
- 根据元素范围在 `Int16` / `Int32` / `Int64` 之间升级编码
- 保持元素有序且不允许重复
- 支持 `insert` / `erase` / `contains` / `clear` / `forEach`
- 通过 `byteSize` 观察当前紧凑存储占用

当前 `ziplist` 已具备的能力包括：

- 使用 `std::vector<std::byte>` 保存连续紧凑字节布局
- 采用 C++ 简化 header：`zltail(4) + zllen(2) + zlend(1)`，总字节数由 `std::vector::size()` 管理
- 支持字符串 entry 的 6-bit、14-bit、32-bit encoding 读写 helper
- 当前 `prevlen` 仍为 1 byte 简化版，尚未支持 Redis 的 5 byte prevlen
- 支持 `pushBack` / `pushFront` / `insert` / `erase` / `popFront` / `popBack`
- 支持 `forEach` / `at` / `find`
- 已引入 `ziplistEntryView`，当前只解析字符串，但接口已为后续整数 entry 留出空间

### ziplist 下一步任务：变长 prevlen 与连锁更新

下一次继续实现 `ziplist` 时，优先处理 `prevlen`，不要直接上连锁更新。建议按下面顺序推进。

#### 1. 抽出 prevlen helper

先新增并只使用这三个 helper：

```cpp
[[nodiscard]] static constexpr std::size_t prevLenSize_(std::size_t len) noexcept;
void writePrevLen_(std::size_t offset, std::size_t prev_len);
[[nodiscard]] std::pair<std::size_t, std::size_t> readPrevLen_(std::size_t offset) const;
```

目标语义：

- `prev_len < 254`：占 1 byte，直接保存长度
- `prev_len >= 254`：占 5 bytes，第 1 byte 写 `0xFE`，后 4 bytes 写 `std::uint32_t` 长度
- `readPrevLen_()` 返回 `{prev_len_size, prev_len_value}`

建议常量：

```cpp
static constexpr std::uint8_t BigPrevLenMarker = 0xFE;
static constexpr std::size_t SmallPrevLenMax = 253;
```

#### 2. 改 parseEntry_

当前 `parseEntry_()` 默认 `prevlen` 只有 1 byte。下一步要改成：

```text
prevlen_offset = offset
{prev_len_size, prev_len_value} = readPrevLen_(prevlen_offset)
encoding_offset = offset + prev_len_size
{encoding_size, content_len} = readStringEncoding_(encoding_offset)
content_offset = encoding_offset + encoding_size
total_len = prev_len_size + encoding_size + content_len
```

`entryView` 中建议新增或替换字段：

- `prev_len_size`
- `prev_len`
- `encoding_size`
- `content_len`
- `total_len`

这样后续 `insert`、`erase`、`popBack` 不需要重复解析。

#### 3. 改写入 entry 的路径

所有写 entry 的地方都要从固定 1 byte prevlen 改成 helper：

- `pushBack`
- `pushFront`
- `insertIntoMid_`

新 entry 长度计算应变成：

```text
entry_len = prevLenSize_(prev_len) + stringEncodingSize_(data.size()) + data.size()
```

当前阶段可以先限制：

```cpp
assert(entry_len <= std::numeric_limits<std::uint32_t>::max());
```

但要注意：只有完成后继节点 prevlen 更新后，才真正允许大 entry 混入多个节点列表。

#### 4. 抽后继 prevlen 更新函数

先不要写完整 cascade，先写一个只更新直接后继的 helper：

```cpp
void updateNextPrevLen_(std::size_t next_offset, std::size_t new_prev_len);
```

它负责：

- 如果 `next_offset` 指向 `EndMarker`，什么也不做
- 比较旧 prevlen 编码大小和新 prevlen 编码大小
- 编码大小相同：原地覆盖
- 编码大小不同：先调整 buffer 空间，再写入新 prevlen

这一步完成后，再接入：

- `pushFront` 插入后更新旧 first
- `insertIntoMid_` 插入后更新原 index 节点
- `eraseFromMid_` 删除后更新后继节点
- `popFront` 删除后更新新 first 为 `prev_len = 0`

#### 5. 最后再做 cascadeUpdate_

连锁更新的触发条件：

```text
某个 entry 的 prevlen 编码大小变化，导致这个 entry 自身 total_len 变化。
这个 total_len 变化又可能让它的后继 entry 的 prevlen 编码大小变化。
```

建议接口：

```cpp
void cascadeUpdate_(std::size_t offset);
```

其中 `offset` 是第一个可能需要更新 prevlen 的节点位置。循环逻辑：

```text
while offset 不是 EndMarker:
    解析当前 entry
    根据前一个 entry 的实际 total_len 计算当前 entry 应写的 prevlen
    如果 prevlen 编码大小不变且值也正确：可以停止
    否则更新当前 entry 的 prevlen
    如果当前 entry 的 total_len 因 prevlen 编码大小变化而变化：
        继续处理下一个 entry
    否则可以停止
```

#### 6. 明天建议的测试顺序

不要一开始就测复杂连锁更新。按下面顺序补测试：

1. `parseEntry_` 间接测试：插入一个长度让 entry 总长 `< 254` 的节点，行为保持不变。
2. `pushBack` 测试：前一个 entry 长度 `>= 254` 时，后一个 entry 仍能正常遍历和 `popBack`。
3. `pushFront` 测试：插入长 entry 后，旧 first 的 prevlen 从 1 byte 扩到 5 byte。
4. `erase` 测试：删除长 entry 后，后继 prevlen 能恢复或保持正确。
5. 最后才构造连续多个临界节点，测试 cascade update。

#### 7. 验证命令

每完成一个小步骤都跑：

```bash
cmake --build build --target ziplist_test
ctest --test-dir build -R ZiplistTest --output-on-failure
```

如果某个测试疑似卡住，单独用：

```bash
timeout 5 build/ziplist_test --gtest_filter='ZiplistTest.具体测试名'
```

当前测试覆盖 `74` 个 GoogleTest 用例，覆盖基础操作、边界行为、渐进式 rehash、跳表范围操作和 intset 编码升级。

目前 `HyperRedisCore` 仍然以头文件为主，因此 CMake 中核心库目标仍可以保持轻量；随着后续 `redisObject`、`redisDb` 和持久化相关 `.cpp` 文件加入，再逐步演进为更完整的核心库实现。

接下来的重点将从底层数据结构转向：

- `Set` / `ZSet` 的对象层封装
- `redisDb`：实现多数据库管理、主字典与过期字典
- 简化版持久化流程（如 RDB save/load）

在字符串实现上，本项目当前不会以 1:1 复刻 Redis SDS 为阶段目标，而是优先复用标准库字符串抽象；但在 `dict`、`skiplist`、`redisObject`、`redisDb` 等真正承载 Redis 核心设计的部分，仍然会优先保留书中的结构和思路。
