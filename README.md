# HyperRedis

HyperRedis 是一个用于学习和实践 Redis 内部设计的现代 C++ 项目。

本项目以《Redis 设计与实现》和 Redis 原版源码为参考，使用 C++20 从零实现了一个可测试的 Redis Core。项目目前包含核心数据结构、对象系统、数据库机制、网络服务器（基于事件循环）以及 AOF/RDB 持久化功能，并提供了一个可直接通过 `redis-cli` 交互的青春版服务端。

---

## 核心组件与功能

HyperRedis 在设计上划分为 `HyperRedisCore` 静态核心库和 `hyper_redis_server` 可执行服务端，目前已实现以下核心能力：

- **底层数据结构**：透明哈希表 (`dict`)、双向链表 (`linked_list`)、紧凑整数集合 (`intset`)、紧凑列表 (`ziplist`)、跳表 (`skipList`)。
- **Redis 对象模型**：String, List, Hash, Set, ZSet，均支持类似 Redis 的编码转换（例如：短小 List 使用 ziplist，超出阈值升级为 linked_list）。
- **数据库与过期机制**：支持多 DB (`SELECT`)、键空间主字典、独立过期字典。支持主动与惰性过期，支持 `TTL`、`EXPIRE` 等时间语义。
- **持久化 (AOF & RDB)**：
  - RDB-like 快照：支持全量数据二进制保存与加载，支持 checksum 校验。
  - AOF-like 日志：支持命令追加与重放、支持 `always`/`everysec`/`no` 刷盘策略，实现了当前上下文的 AOF 重写 (`REWRITEAOF`) 以及无阻塞的后台重写 (`BGREWRITEAOF`)，并带防数据丢失的 Rewrite Buffer。
- **网络与服务端**：基于 epoll (poll 模拟) 的单线程文件事件循环 (`EventLoop`)，原生 TCP listener。完整实现 RESP2 协议解析与响应序列化，支持处理网络半包、连续命令。

> **当前限制与规划**：暂不支持集群 (Cluster)、主从复制、哨兵 (Sentinel)。单线程事件循环本身无后台任务线程，但通过多进程 (`fork`) 实现了真正的无阻塞后台持久化 (`BGSAVE` / `BGREWRITEAOF`)。

---

## 支持的命令

当前命令由 `CommandRegistry` 统一登记，支持 `redis-cli` 调用及单元测试直接执行。

| 分类           | 涵盖命令                                                                                                                                                        |
|--------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| **连接/DB/通用** | `PING`, `SELECT`, `DBSIZE`, `DEL`, `EXISTS`, `TYPE`, `TTL`, `PTTL`, `PERSIST`, `EXPIRE`, `PEXPIRE`, `PEXPIREAT`, `FLUSHDB`, `FLUSHALL`, `RANDOMKEY`, `RENAME`, `RENAMENX`, `SAVE`, `BGSAVE`, `LASTSAVE`, `INFO`, `TIME`, `OBJECT`, `CONFIG`, `COMMAND`, `REWRITEAOF`, `BGREWRITEAOF` |
| **String**   | `SET`, `GET`, `MGET`, `MSET`, `STRLEN`, `APPEND`, `INCR`, `DECR`, `INCRBY`, `INCRBYFLOAT`, `GETRANGE`, `SETRANGE`                                                    |
| **List**     | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LINSERT`, `LREM`, `LTRIM`                                                                    |
| **Hash**     | `HSET`, `HGET`, `HDEL`, `HLEN`, `HGETALL`, `HEXISTS`, `HKEYS`, `HVALS`                                                                                           |
| **Set**      | `SADD`, `SREM`, `SISMEMBER`, `SCARD`, `SMEMBERS`, `SPOP`, `SRANDMEMBER`                                                                                         |
| **ZSet**     | `ZADD`, `ZREM`, `ZSCORE`, `ZCARD`, `ZRANGE`, `ZRANK`, `ZREVRANK`, `ZCOUNT`, `ZREVRANGE`, `ZINCRBY`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`                            |

---

## 性能表现 (Benchmark)

在 Ubuntu 22.04 / WSL2 环境下使用 `redis-benchmark` 进行压力测试（Release 模式构建）：

### 1. 标准并发测试 (No Pipelining)
命令：`redis-benchmark -p 8080 -t set,get,incr -n 100000 -c 50 -r 10000 -q`

| 命令 | 每秒处理请求数 (RPS) |
| :--- | :--- |
| **SET** | **90,252.70** |
| **GET** | **89,206.06** |
| **INCR** | **90,334.23** |

### 2. 极限吞吐测试 (Pipelining)
命令：`redis-benchmark -p 8080 -t set,get -n 1000000 -P 16 -q`

| 命令 | 每秒处理请求数 (RPS) |
| :--- | :--- |
| **SET** | **1,375,559.88** (1.37 Million) |
| **GET** | **1,273,926.12** (1.27 Million) |

> 结论：HyperRedis 在现代 C++20 的加持下，核心逻辑处理能力极强。开启 Pipeline 后吞吐量突破百万大关，证明了其高效的事件循环和数据结构实现。

---

## 系统架构

HyperRedis 遵循单线程事件驱动架构，从下到上严格分层：

```text
redis-cli / TCP client
        │ RESP2
        ▼
RedisServerRunner (TcpListener + RedisServer + EventLoop)
        │ 监听、Accept、读取网络缓冲
        ▼
ClientSession (Query Buffer & Parse RESP)
        │ 分发
        ▼
CommandProcessor / CommandExecutor (权限与命令执行，AOF 协调)
        │ 读写
        ▼
RedisDb (主字典、过期字典)
        │
        ▼
RedisObject (String / List / Hash / Set / ZSet 编码管理)
        │
        ▼
dict / intset / ziplist / skipList / linked_list
```

### 与 Redis 原版的设计异同

| Redis 设计                               | HyperRedis 表达                                                   |
|----------------------------------------|-----------------------------------------------------------------|
| `redisObject` 采用 type + encoding + ptr | 使用现代 C++ `std::variant` 保存对象值，结合 `ObjectType` 与 `ObjectEncoding` |
| SDS 字符串                                | 使用原生 `std::string` 和视图 `std::string_view`，减少字符串拷贝                 |
| `dict` 双表渐进式 rehash                    | C++ 模板 `dict<K, V>` + 双表 + rehash step                          |
| 小对象紧凑编码                                | 实现 `intset`、`ziplist` 及自动编码升级机制                                |
| ZSet (skiplist + dict)                 | `skipList` 支持 score range、rank 查询及区间删除                          |
| ae 文件事件                                | `EventLoop` 使用 `poll()` 分发 readable/writable callback           |

---

## 快速开始 (Quick Start)

### 1. 依赖安装

系统需要支持 **C++20** 编译器、**CMake 3.20+**。

Ubuntu/Debian 示例：
```bash
sudo apt update
sudo apt install build-essential cmake libspdlog-dev libgtest-dev redis-tools
```

### 2. 配置与构建

```bash
# 配置与构建 (Release 模式获取最佳性能)
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

如果只想构建服务端或运行测试：
```bash
# 仅构建服务端
cmake --build build --target hyper_redis_server

# 运行单元测试
ctest --test-dir build --output-on-failure
```

### 3. 启动服务端

默认监听 `127.0.0.1:8080`：
```bash
./build/hyper_redis_server
```

**持久化启动示例：**
```bash
# 启用 AOF 追加，并在启动时从 AOF 恢复
./build/hyper_redis_server --port 8080 --aof /tmp/hr.aof --load-aof

# 启用 RDB-like 退出保存，并在下次启动加载
./build/hyper_redis_server --port 8080 --rdb /tmp/hr.rdb --save-rdb-on-stop --load-rdb
```

### 4. 连接使用

另开一个终端，使用 `redis-cli` 连接：
```bash
redis-cli -h 127.0.0.1 -p 8080
```

```text
127.0.0.1:8080> PING
PONG
127.0.0.1:8080> SET name hyper
OK
127.0.0.1:8080> GET name
"hyper"
```

---

## 开发约定

- **代码结构**：
  - `include/hyper/`：公共头文件
  - `src/`：核心与服务端实现
  - `apps/`：服务端可执行入口
  - `test/`：GoogleTest 测试用例
- **命名规范**：
  - Public API 使用 `lowerCamelCase` (无后缀)
  - 私有 helper 方法使用 `lowerCamelCase_` (带下划线后缀)
  - 私有数据成员使用 `lower_snake_case_` (带下划线后缀)
- **构建规范**：新增源文件和测试目标必须显式加入 `CMakeLists.txt`，禁止使用 `file(GLOB ...)`。
- **质量保证**：核心行为（数据结构、对象语义、RESP 编解码、事件循环等）优先使用 GoogleTest 固化回归测试。
