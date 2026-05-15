# HyperRedis

一个用于学习与实践 Redis 内部设计的现代 C++ 项目。

这个仓库主要记录我在阅读《Redis 设计与实现》以及理解 Redis 源码过程中，对核心数据结构、对象系统、数据库和命令执行流程的手写实现与思考。目标不是做一个生产级 Redis 替代品，而是在保留 Redis 经典设计思想的前提下，用现代 C++ 重新表达原版 C 语言实现中的核心能力。

当前项目重点是 `HyperRedisCore`：一个不依赖网络层的 Redis-like 静态核心库。它已经包含底层数据结构、Redis 对象模型、多 DB 存储管理、过期机制、RDB-like 快照持久化、RESP 响应值模型和命令执行器。完整 TCP 服务器、RESP 请求解析和性能基准仍在后续计划中。

## 项目目标

- 以 Redis 的核心数据结构和对象模型为参考，实现一个可测试的现代 C++ Redis core
- 用工程化方式沉淀 Redis 学习过程，而不是只停留在读书和笔记
- 对照原版 C 实现理解设计取舍：紧凑编码、渐进式 rehash、对象编码转换、跳表、有序集合、过期字典等
- 用 RAII、类型安全、智能指针和标准库容器降低手写内存管理成本
- 所有核心功能尽量配备 GoogleTest 单元测试和 Redis 可见行为测试

## 技术取向

- 使用 C++20 作为默认构建标准
- 不复刻 Redis 的全局式 C API，而是用类、模板、枚举和 `std::variant` 表达对象边界
- 默认不使用裸指针表达所有权，优先使用 `std::unique_ptr` / `std::shared_ptr`
- 使用 `std::string_view`、透明哈希和 `std::span` 减少不必要的字符串拷贝
- 使用 `std::chrono` 表达过期时间和 TTL 语义
- 使用 GoogleTest 做行为回归，用 `RespValue` 统一命令执行结果

---

## Quick Start

### 1. 安装依赖

HyperRedis 当前依赖以下开发组件：

| 依赖 | 用途 |
| --- | --- |
| **CMake 3.20+** | 项目构建 |
| **C++20 编译器** | 编译核心库和测试 |
| **spdlog** | 日志库依赖 |
| **GoogleTest** | 单元测试框架 |

Ubuntu/Debian 示例：

```bash
sudo apt install build-essential cmake libspdlog-dev libgtest-dev
```

### 2. 编译项目

```bash
cmake -S . -B build
cmake --build build
```

### 3. 运行单元测试

```bash
ctest --test-dir build --output-on-failure
```

也可以做一次干净构建检查，避免复用 IDE 的构建目录：

```bash
cmake -S . -B /tmp/hyperredis-check
cmake --build /tmp/hyperredis-check
ctest --test-dir /tmp/hyperredis-check --output-on-failure
```

### 4. 通过命令执行器使用核心能力

当前还没有独立 Redis 服务器可执行目标，推荐通过 `RedisManager`、`RedisClientContext` 和 `CommandExecutor` 直接使用核心能力：

```cpp
#include "hyper/server/client_context.hpp"
#include "hyper/server/command_executor.hpp"
#include "hyper/storage/redis_manager.hpp"

#include <chrono>
#include <string_view>
#include <vector>

int main() {
    hyper::RedisManager manager;
    hyper::RedisClientContext client;
    hyper::CommandExecutor executor;
    const auto now = hyper::ExpireClock::now();

    std::vector<std::string_view> set_args{"SET", "name", "hyper"};
    executor.execute(manager, client, set_args, now);

    std::vector<std::string_view> get_args{"GET", "name"};
    auto reply = executor.execute(manager, client, get_args, now);
}
```

---

## 当前项目结构

```text
HyperRedis/
├── include/hyper/
│   ├── datastructures/             # Redis 风格底层数据结构
│   │   ├── dict.hpp                # 哈希表 + 渐进式 rehash
│   │   ├── intset.hpp              # 紧凑整数集合
│   │   ├── linked_list.hpp         # 双向链表
│   │   ├── skip_list.hpp           # 跳表
│   │   └── ziplist.hpp             # 紧凑列表编码
│   ├── storage/                    # 对象系统、数据库、快照
│   │   ├── object.hpp              # RedisObject 与编码转换
│   │   ├── database.hpp            # RedisDb、过期字典、TTL
│   │   ├── redis_manager.hpp       # 多 DB 管理
│   │   ├── snapshot.hpp            # RDB-like 字节编解码
│   │   └── rdb_saver.hpp           # RDB-like 文件落盘包装
│   └── server/                     # 未来服务器层之前的命令核心
│       ├── resp_value.hpp          # RESP 响应值模型
│       ├── client_context.hpp      # 客户端当前 DB 上下文
│       └── command_executor.hpp    # Redis 命令执行器
├── src/
│   ├── storage/                    # 存储与对象实现
│   └── server/                     # 命令执行核心实现
├── test/                           # GoogleTest 测试
├── CMakeLists.txt                  # 构建配置，显式维护源文件和测试目标
└── README.md
```

---

## 架构概览

```text
┌──────────────────────────────────────────────────────────────┐
│                         Command Layer                        │
│  CommandExecutor                                             │
│  - 参数校验                                                   │
│  - 命令分发                                                   │
│  - Redis 风格错误与 RespValue 响应                             │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│                       Client / DB Layer                      │
│  RedisClientContext       RedisManager                       │
│  - 当前 DB 选择                多 DB 生命周期管理                │
│                              - FLUSHDB / FLUSHALL            │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│                         Storage Layer                        │
│  RedisDb                                                     │
│  - main_dict                                                 │
│  - expire_dict                                               │
│  - 惰性过期 / 主动过期                                          │
│  - TTL / PTTL / rename / random key                          │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│                         Object Layer                         │
│  RedisObject                                                 │
│  - String / List / Hash / Set / ZSet                         │
│  - Raw / Int / ZipList / IntSet / HashTable / SkipList 编码   │
│  - 编码升级与行为保持                                           │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│                      Data Structure Layer                    │
│  dict / intset / ziplist / linked_list / skipList            │
└──────────────────────────────────────────────────────────────┘
```

### 组件说明

| 组件                     | 职责                                               |
|------------------------|--------------------------------------------------|
| **dict**               | Redis 字典思想的 C++ 模板实现，支持渐进式 rehash、透明字符串查找、随机 key |
| **intset**             | 紧凑整数集合，按元素范围自动升级编码                               |
| **ziplist**            | 紧凑连续内存结构，支持字符串/整数编码、插入删除和级联更新                    |
| **skipList**           | 有序集合底层结构，支持 rank、score range 和区间删除               |
| **RedisObject**        | 统一对象模型，封装 Redis 五大类型及内部编码转换                      |
| **RedisDb**            | 单个数据库，管理主字典、过期字典和键空间行为                           |
| **RedisManager**       | 多 DB 管理，默认 16 个 DB                               |
| **RedisClientContext** | 保存客户端当前 DB 选择                                    |
| **CommandExecutor**    | 命令入口，返回 RESP 风格响应模型                              |
| **Snapshot**           | RDB-like 字节编解码，支持多 DB、过期元数据和五种对象类型 round-trip       |
| **RdbSaver**           | 文件落盘包装，基于临时文件和 rename 写入 RDB-like 快照                  |

---

## 现代 C++ 对 Redis C 实现的重新表达

Redis 原版使用 C 语言实现，很多能力依赖结构体、宏、函数指针、手动内存管理和约定式类型检查。HyperRedis 保留这些设计背后的核心思想，但尽量用现代 C++ 的类型系统和 RAII 表达边界。

### 概览：核心差异速查

| Redis C 实现思路 | HyperRedis 现代 C++ 表达 | 优势 |
| --- | --- | --- |
| `robj` 中用 `type + encoding + void* ptr` 表示对象 | `RedisObject` 使用 `ObjectType`、`ObjectEncoding` 和 `std::variant` | 类型边界更清晰，减少错误转换 |
| SDS 动态字符串 | `std::string` / `std::string_view` | 自动管理内存，接口更安全 |
| `dict` 依赖 C 结构体和函数指针 | 模板 `dict<K, V, Hash, KeyEqual>` | 可复用、类型安全，编译期约束更明确 |
| `dictFind` 常需要构造 key 字符串 | 透明哈希支持 `std::string_view` 查找 | 减少临时对象和拷贝 |
| 对象释放依赖手动引用计数和析构函数约定 | `std::shared_ptr` / `std::unique_ptr` + 析构函数 | 所有权语义明确，异常路径更安全 |
| 过期时间使用整数毫秒时间戳 | `std::chrono::time_point` / `milliseconds` | 时间单位强类型化，减少单位混淆 |
| 命令响应写入客户端输出缓冲 | `RespValue` 使用 `std::variant` 建模 | 命令层可脱离网络层独立测试 |
| ziplist 直接操作裸字节数组 | `std::vector<std::byte>` + entry view | 保留紧凑布局，同时封装解析细节 |
| 返回码和错误通过字符串约定传播 | `std::optional`、枚举状态和 `RespError` | 调用方更容易区分缺失、错误和正常值 |

### 1. 对象系统：从 `void*` 到 `std::variant`

Redis 原版对象大致由 `type`、`encoding` 和底层指针组成。不同命令需要根据类型和编码把 `void* ptr` 转回对应结构，例如字符串、ziplist、dict、skiplist 等。

HyperRedis 保留这一模型，但用更显式的类型表达：

```cpp
enum class ObjectType : std::uint8_t {
    String,
    List,
    Set,
    ZSet,
    Hash
};

enum class ObjectEncoding : std::uint8_t {
    Raw,
    Int,
    HashTable,
    ZipList,
    IntSet,
    SkipList,
    LinkedList
};
```

底层数据通过 `ObjectData` 统一承载：

```cpp
using ObjectData = std::variant<
    long,
    std::string,
    std::unique_ptr<RedisDict>,
    std::unique_ptr<RedisIntset>,
    std::unique_ptr<RedisZiplist>,
    std::unique_ptr<RedisZSet>,
    std::unique_ptr<RedisList>,
    std::unique_ptr<RedisSet>
>;
```

这样仍能学习 Redis 的“同一类型多种编码”思想，例如：

- String 可在整数编码和 raw string 编码之间转换
- List 可从 ziplist 升级到 linked list
- Hash 可从 ziplist 升级到 dict
- Set 可从 intset 升级到 dict
- ZSet 可从 ziplist 升级到 skiplist + dict

相比 C 版本的 `void*`，这种写法让“当前对象到底可能持有什么数据”在类型层面可见，也让析构和移动语义更自然。

### 2. 字典：保留渐进式 rehash，用模板和透明查找增强可用性

Redis 的 `dict` 是全项目最核心的基础结构之一，支持双哈希表和渐进式 rehash。HyperRedis 的 `dict` 保留了这个设计：

- 内部维护 `hash_tables_[0]` 和 `hash_tables_[1]`
- rehash 过程中逐步迁移 bucket
- 插入、删除、查找时顺带推进 rehash step
- 支持扩容和收缩
- 支持随机 key 获取，用于 `RANDOMKEY` 等行为

现代 C++ 版本的主要变化是把 key/value 类型模板化：

```cpp
template <typename K, typename V, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
class dict;
```

同时对字符串 key 提供透明查找：

```cpp
dict<std::string, RedisObjectPtr, transparentStringHash, transparentStringEqual>
```

这意味着数据库里实际保存的是 `std::string` key，但查询时可以直接传入 `std::string_view`，避免为了查找临时构造字符串。

### 3. 紧凑编码：保留 Redis 的内存意识

Redis 很多高性能设计并不只是算法选择，还包括内存布局选择。HyperRedis 在学习实现中保留了这部分重点。

`intset`：

- 只保存整数成员
- 根据元素范围在 16/32/64 位整数编码之间升级
- 数据保持有序，查找和插入可用二分定位
- 用于小型整数 Set 的紧凑编码

`ziplist`：

- 使用连续字节存储多个 entry
- entry 支持字符串和整数两类 payload
- 字符串长度使用不同宽度编码
- 整数支持 int8/int16/int24/int32/int64 和立即数编码
- 删除或插入时处理 prevlen 变化导致的级联更新

这类结构在 C++ 中没有直接用 `std::list` 或 `std::map` 替代，因为项目目标是理解 Redis 为什么要引入这些紧凑编码，而不仅仅是复刻命令表面行为。

### 4. 有序集合：skiplist + dict 的 Redis 思路

Redis ZSet 的典型展开编码是“跳表负责有序遍历，字典负责按 member O(1) 查 score”。HyperRedis 也保留了这个方向：

- `skipList` 按 `(score, member)` 排序
- 支持 rank 查询和按 rank 获取元素
- 支持 score range 查找和区间删除
- ZSet 对象在小规模时使用 ziplist，超过阈值后升级到 skiplist 编码

这种实现重点不是调用标准库容器快速完成，而是把 Redis 的组合结构拆开学习：为什么需要跳表、rank 如何维护、相同 score 时如何按 member 排序、区间删除如何保持结构一致。

### 5. 数据库与过期机制：主字典 + 过期字典

Redis 数据库中 key-value 数据和过期时间是分开存储的。HyperRedis 的 `RedisDb` 同样维护：

- `main_dict_`：key 到 `RedisObjectPtr`
- `expire_dict_`：key 到 Unix milliseconds 过期时间

已实现的过期行为包括：

- `expireAt` / `expireAfter`
- `TTL` / `PTTL`
- `PERSIST`
- 惰性过期：访问 key 时检查并删除过期 key
- 主动过期：`activeExpireCycle`
- `EXPIRE` / `PEXPIRE` 的 `NX`、`XX`、`GT`、`LT` 条件选项

时间相关代码使用 `std::chrono`，相比直接传递裸整数，能减少“秒、毫秒、绝对时间、相对时间”混用的问题。

### 6. 命令层：脱离网络的可测试 Redis 行为

当前项目还没有 TCP server，因此命令执行器不处理 socket，也不解析客户端 RESP 请求。它接收已经拆好的参数数组：

```cpp
using Args = std::span<const std::string_view>;
```

然后返回统一的 RESP 值模型：

```cpp
using RespValue = std::variant<
    RespSimpleString,
    RespError,
    RespInteger,
    RespBulkString,
    std::shared_ptr<RespArray>
>;
```

这种拆分的好处是：

- 命令语义可以不依赖网络层独立测试
- 错误、整数、bulk string、数组等响应类型有明确结构
- 后续接入 RESP parser 和 TCP server 时，可以把网络层作为薄封装

---

## 已实现功能

### A. 工程基础

- [x] CMake 工程初始化
- [x] `HyperRedisCore` 静态库目标
- [x] `include/hyper/` + `src/` 分层目录
- [x] GoogleTest 测试框架
- [x] C++20 构建标准
- [x] 显式维护 CMake 源文件和测试目标列表

### B. 底层数据结构

- [x] `dict`：哈希表、双表渐进式 rehash、透明字符串查找、随机 key
- [x] `linked_list`：双向链表、插入、删除、遍历、移动语义
- [x] `intset`：有序整数集合、编码升级、紧凑存储
- [x] `ziplist`：紧凑 entry 编码、整数编码、插入删除、级联更新
- [x] `skipList`：score/rank 查询、同分排序、区间查找和区间删除

### C. Redis 对象模型

- [x] `RedisObject` 支持 String、List、Hash、Set、ZSet
- [x] String 支持整数编码、追加、范围读写、整数/浮点自增和溢出检查
- [x] List 支持 ziplist / linked-list 编码切换
- [x] Hash 支持 ziplist / dict 编码切换
- [x] Set 支持 intset / dict 编码切换
- [x] ZSet 支持 ziplist / skiplist 编码切换
- [x] 编码转换后保持 Redis 可见行为一致

### D. 数据库与客户端上下文

- [x] `RedisDb` 主 key-value 字典
- [x] 过期字典和 TTL / PTTL 语义
- [x] 惰性过期和主动过期
- [x] `rename`、`randomKey`、`clear`
- [x] `RedisManager` 多 DB 管理，默认 16 个 DB
- [x] `RedisClientContext` 保存客户端当前 DB 选择

### E. 命令执行器

`CommandExecutor` 已支持命令名大小写归一、参数数量校验、Redis 风格错误返回和 `RespValue` 响应模型。

当前已实现的命令包括：

| 分类 | 命令 |
| --- | --- |
| 连接 / DB / 通用 | `PING`、`SELECT`、`DBSIZE`、`DEL`、`EXISTS`、`TYPE`、`TTL`、`PTTL`、`PERSIST`、`EXPIRE`、`PEXPIRE`、`FLUSHDB`、`FLUSHALL`、`RANDOMKEY`、`RENAME`、`RENAMENX` |
| String | `SET`、`GET`、`MGET`、`MSET`、`STRLEN`、`APPEND`、`INCR`、`DECR`、`INCRBY`、`INCRBYFLOAT`、`GETRANGE`、`SETRANGE` |
| List | `LPUSH`、`RPUSH`、`LPOP`、`RPOP`、`LLEN`、`LRANGE`、`LINDEX`、`LSET`、`LINSERT`、`LREM`、`LTRIM` |
| Hash | `HSET`、`HGET`、`HDEL`、`HLEN`、`HGETALL`、`HEXISTS`、`HKEYS`、`HVALS` |
| Set | `SADD`、`SREM`、`SISMEMBER`、`SCARD`、`SMEMBERS`、`SPOP`、`SRANDMEMBER` |
| ZSet | `ZADD`、`ZREM`、`ZSCORE`、`ZCARD`、`ZRANGE`、`ZRANK`、`ZREVRANK`、`ZCOUNT`、`ZREVRANGE`、`ZINCRBY`、`ZREMRANGEBYRANK`、`ZREMRANGEBYSCORE` |

### F. RDB-like Snapshot / RdbSaver

- [x] `Snapshot` 接口
- [x] header、DB 选择、过期时间和值对象保存与加载
- [x] String / List / Set / Hash / ZSet round-trip
- [x] 保存时过滤已过期 key
- [x] 加载时跳过已经过期的 key，坏数据不会污染现有 manager
- [x] 对 key、Set member、Hash field 排序，提升快照字节稳定性
- [x] `RdbSaver` 文件保存与加载包装
- [x] 文件级 round-trip、缺失文件和坏文件测试
- [x] checksum 校验

---

## 测试覆盖

当前测试集中覆盖以下方向：

- 底层数据结构：`linked_list`、`dict`、`skipList`、`intset`、`ziplist`
- 对象模型：String、List、Hash、Set、ZSet 及编码转换
- Redis 可见行为：范围、rank、随机成员、错误语义、边界值
- 数据库层：过期、TTL/PTTL、rename、random key、active expire
- 多 DB：`RedisManager` 与 `RedisClientContext`
- RDB-like 持久化：`Snapshot` 字节 round-trip、`RdbSaver` 文件 round-trip 和坏文件保护
- 命令层：命令分发、参数校验、wrong type、Redis 风格响应

运行方式：

```bash
ctest --test-dir build --output-on-failure
```

---

## 性能基准测试

> 当前项目尚未接入网络层，也没有进行系统性 benchmark。这里先保留结构，后续在 RESP parser、TCP server 和持久化路径稳定后补充。

### 待测试场景

- [ ] 纯 `CommandExecutor` 内存命令吞吐
- [ ] String / Hash / Set / ZSet 典型操作延迟
- [ ] `dict` 渐进式 rehash 压力测试
- [ ] ziplist 与展开编码的内存占用对比
- [ ] 未来网络层接入后的 Redis-benchmark 对比

### 待记录指标

| 场景 | QPS | 平均延迟 | P99 延迟 | 内存占用 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 待补充 | - | - | - | - | - |

---

## 与原版 Redis 的设计对比

### 1. 对象模型

| 维度 | Redis 原版 | HyperRedis |
| --- | --- | --- |
| 对象表示 | `redisObject` + `void* ptr` | `RedisObject` + `std::variant` |
| 类型标识 | 整数常量 / 宏 | `enum class ObjectType` |
| 编码标识 | 整数常量 / 宏 | `enum class ObjectEncoding` |
| 生命周期 | 引用计数 + 手动释放 | 智能指针 + RAII |
| 错误风险 | 错误 cast 依赖运行时约定避免 | 编译期可见的数据候选集合 |

### 2. 字符串

| 维度 | Redis 原版 | HyperRedis |
| --- | --- | --- |
| 字符串结构 | SDS | `std::string` |
| 只读参数 | `char*` + length | `std::string_view` |
| 整数编码 | `OBJ_ENCODING_INT` | `ObjectEncoding::Int` + `long` |
| 内存管理 | 手动分配释放 | 标准库自动管理 |

### 3. 字典

| 维度 | Redis 原版 | HyperRedis |
| --- | --- | --- |
| 数据结构 | `dict` + `dictht[2]` | `dict<K, V>` + 双表 |
| rehash | 渐进式 rehash | 渐进式 rehash |
| 类型适配 | `dictType` 函数指针 | 模板参数 `Hash` / `KeyEqual` |
| 字符串查找 | C 字符串 / SDS 约定 | `std::string_view` 透明查找 |

### 4. 紧凑列表

| 维度 | Redis 原版 | HyperRedis |
| --- | --- | --- |
| 结构 | ziplist / listpack 类紧凑字节布局 | `ziplist` 使用连续字节存储 |
| entry 解析 | 宏和指针偏移 | entry view 封装 |
| 整数编码 | 多宽度整数和立即数 | 保留 int8/int16/int24/int32/int64/imm |
| 级联更新 | 手动处理 prevlen 变化 | 显式测试覆盖级联更新 |

### 5. 命令执行

| 维度 | Redis 原版 | HyperRedis |
| --- | --- | --- |
| 网络与命令 | 命令直接作用于客户端连接和 DB | 命令层暂时脱离网络 |
| 回复写入 | 写入 client reply buffer | 返回 `RespValue` |
| 测试方式 | 集成式服务测试为主 | 命令执行器可直接单测 |

---

## 设计与实现原则

- 先实现核心行为，再考虑网络层和性能调优
- 保留 Redis 关键设计：编码转换、紧凑结构、渐进式 rehash、跳表、过期字典
- 对 C 语言中依赖约定的地方，用 C++ 类型系统表达边界
- 不为了“现代化”直接替换掉 Redis 值得学习的底层结构
- 测试优先覆盖行为和边界条件，再逐步补充压力和性能测试
- `CMakeLists.txt` 是构建输入的来源，新增 `.cpp` 和测试目标需要显式登记

## 指针与所有权约定

- 默认不使用裸指针表达所有权
- 独占所有权使用 `std::unique_ptr`
- 对象值共享使用 `std::shared_ptr<RedisObject>`
- 非拥有观察和可空返回允许使用裸指针或 `std::optional`
- 字符串只读参数优先使用 `std::string_view`
- 命令参数使用 `std::span<const std::string_view>` 表达连续只读视图

---

## 后续计划

近期重点：

- 将 RDB-like 持久化接入未来服务器启动/关闭流程
- 设计同步 `SAVE` 和基于 fork/COW 的 `BGSAVE` 入口
- 补齐 RESP 请求解析和响应序列化
- 增加完整服务器目标和 TCP 连接管理

中长期方向：

- 接入事件驱动网络层，实现可运行 Redis-like server
- 增加持久化校验和更完整的 RDB-like 格式支持
- 补充 Redis 行为兼容测试
- 增加 benchmark，量化内存结构和命令执行性能
- 继续对照 Redis 源码补齐更多命令和边界语义

## 致谢

本项目的设计思想与学习路径主要来自 Redis 源码、《Redis 设计与实现》以及 Redis 社区资料。这个仓库是个人学习过程中的手写实践与复盘，不是 Redis 的替代品。
