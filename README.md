# HyperRedis

HyperRedis 是一个用于学习和实践 Redis 内部设计的现代 C++ 项目。

这个仓库记录了我在阅读《Redis 设计与实现》和理解 Redis 源码过程中，对核心数据结构、对象系统、数据库、命令执行、持久化和网络服务器生命周期的手写实现。项目目标不是替代生产级 Redis，而是在保留 Redis 经典设计思想的前提下，用现代 C++ 重新表达原版 C 实现中的核心能力。

当前项目已经从纯核心库推进到一个可以被 `redis-cli` 连接的青春版 Redis-like server：

- `HyperRedisCore`：静态核心库，包含数据结构、对象系统、数据库、命令执行、RESP、AOF/RDB-like 持久化、事件循环、客户端会话和 TCP server 组件。
- `hyper_redis_server`：可执行服务端入口，支持 `--host` 和 `--port` 参数，可以通过 `redis-cli` 发送命令。

---

## 项目目标

- 以 Redis 的核心数据结构和对象模型为参考，实现一个可测试的现代 C++ Redis core。
- 用工程化方式沉淀 Redis 学习过程，而不是只停留在读书和笔记。
- 对照原版 C 实现理解设计取舍：紧凑编码、渐进式 rehash、对象编码转换、跳表、有序集合、过期字典、AOF/RDB、事件循环和客户端 buffer。
- 用 RAII、类型安全、智能指针和标准库容器降低手写内存管理成本。
- 保持核心功能可测试：底层结构、对象行为、命令语义、RESP 编解码、socket I/O、事件循环和 TCP server 集成都有 GoogleTest 覆盖。

## 技术取向

- C++20
- CMake 显式维护源文件和测试目标，不使用 `file(GLOB ...)`
- GoogleTest 做行为测试和回归测试
- spdlog 做服务端日志
- `std::chrono` 表达过期时间和 TTL 语义
- `std::string_view`、透明哈希、`std::span` 减少不必要的字符串拷贝
- `std::variant`、智能指针、RAII 表达 Redis 对象和资源所有权

---

## Quick Start

### 1. 安装依赖

HyperRedis 当前依赖：

| 依赖          | 用途           |
|-------------|--------------|
| CMake 3.20+ | 项目构建         |
| C++20 编译器   | 编译核心库、服务端和测试 |
| spdlog      | 服务端日志        |
| GoogleTest  | 单元测试         |
| redis-cli   | 可选，用于手动验证服务端 |

Ubuntu/Debian 示例：

```bash
sudo apt install build-essential cmake libspdlog-dev libgtest-dev redis-tools
```

### 2. 配置和构建

```bash
cmake -S . -B build
cmake --build build
```

如果只想构建服务端：

```bash
cmake --build build --target hyper_redis_server
```

### 3. 启动 HyperRedis 服务端

默认监听 `127.0.0.1:8080`：

```bash
./build/hyper_redis_server
```

也可以指定 host 和 port：

```bash
./build/hyper_redis_server --host 127.0.0.1 --port 8080
```

如果传 `--port 0`，系统会自动分配端口，启动日志会打印真实监听端口。

### 4. 使用 redis-cli 交互

另开一个终端：

```bash
redis-cli -h 127.0.0.1 -p 8080
```

示例：

```text
127.0.0.1:8080> PING
PONG
127.0.0.1:8080> SET name hyper
OK
127.0.0.1:8080> GET name
"hyper"
127.0.0.1:8080> HSET info zhr 27
(integer) 1
127.0.0.1:8080> HGET info zhr
"27"
```

也可以直接发送 RESP：

```bash
printf '*1\r\n$4\r\nPING\r\n' | nc 127.0.0.1 8080
```

期望输出：

```text
+PONG
```

### 5. 运行测试

```bash
ctest --test-dir build --output-on-failure
```

也可以做一次干净构建检查，避免复用 IDE 构建目录：

```bash
cmake -S . -B /tmp/hyperredis-check
cmake --build /tmp/hyperredis-check
ctest --test-dir /tmp/hyperredis-check --output-on-failure
```

部分真实 TCP listener 集成测试默认需要显式开启：

```bash
HYPERREDIS_RUN_TCP_LISTENER_TESTS=1 ./build/redis_server_test --gtest_filter='RedisServerTcpIntegrationTest.*'
```

---

## 当前项目结构

```text
HyperRedis/
├── apps/
│   └── hyperredis_server.cpp       # 可执行服务端入口
├── include/hyper/
│   ├── time.hpp                    # ExpireClock / ExpireTimePoint / Milliseconds
│   ├── datastructures/             # Redis 风格底层数据结构
│   │   ├── dict.hpp                # 哈希表 + 渐进式 rehash
│   │   ├── intset.hpp              # 紧凑整数集合
│   │   ├── linked_list.hpp         # 双向链表
│   │   ├── skip_list.hpp           # 跳表
│   │   └── ziplist.hpp             # 紧凑列表编码
│   ├── storage/                    # 对象系统、数据库、持久化
│   │   ├── object.hpp              # RedisObject 与编码转换
│   │   ├── database.hpp            # RedisDb、过期字典、TTL
│   │   ├── redis_manager.hpp       # 多 DB 管理
│   │   ├── snapshot.hpp            # RDB-like 字节编解码
│   │   ├── rdb_saver.hpp           # RDB-like 文件落盘包装
│   │   ├── aof_appender.hpp        # AOF-like 命令追加
│   │   ├── aof_replayer.hpp        # AOF-like 命令重放
│   │   └── aof_rewriter.hpp        # 从当前 DB 状态生成紧凑 AOF
│   └── server/                     # 命令、RESP、会话、事件循环和 TCP server
│       ├── resp_value.hpp          # RESP 响应值模型
│       ├── resp_codec.hpp          # RESP 命令解析与响应序列化
│       ├── client_context.hpp      # 客户端当前 DB 上下文
│       ├── client_session.hpp      # query buffer / reply buffer / client context
│       ├── client_socket_io.hpp    # fd read/write 与 ClientSession 适配
│       ├── command_registry.hpp    # 命令元信息、arity、写命令分类
│       ├── command_executor.hpp    # Redis 命令执行器
│       ├── command_processor.hpp   # 命令执行 + AOF 追加协调
│       ├── event_loop.hpp          # poll-based 文件事件循环
│       ├── redis_server.hpp        # server 状态、client 管理、listener 接入
│       ├── redis_server_runner.hpp # 组合 TcpListener / RedisServer / EventLoop
│       └── tcp_listener.hpp        # TCP socket/bind/listen 封装
├── src/
│   ├── storage/                    # 存储与持久化实现
│   └── server/                     # server、命令、RESP、事件循环实现
├── test/                           # GoogleTest 测试
├── CMakeLists.txt
└── README.md
```

---

## 架构概览

```text
redis-cli / TCP client
        │ RESP2
        ▼
apps/hyperredis_server
        │
        ▼
RedisServerRunner
  - TcpListener
  - RedisServer
  - EventLoop
        │
        ▼
RedisServer
  - listen fd readable callback
  - accept client fd
  - fd -> ClientSession
  - readable callback: read query, parse RESP, execute command
  - writable callback: flush reply buffer
        │
        ▼
CommandProcessor / CommandExecutor
  - command registry
  - arity check
  - write command classification
  - AOF append coordination
        │
        ▼
RedisManager / RedisDb
  - multi DB
  - main dict
  - expire dict
  - lazy expire / active expire
        │
        ▼
RedisObject
  - String / List / Hash / Set / ZSet
  - Raw / Int / ZipList / IntSet / HashTable / SkipList / LinkedList
        │
        ▼
dict / intset / ziplist / skipList / linked_list
```

### 关键职责边界

| 组件                  | 职责                                                                                      |
|---------------------|-----------------------------------------------------------------------------------------|
| `TcpListener`       | 创建监听 socket，完成 `socket` / `setsockopt` / `bind` / `listen` / `getsockname`，拥有 listen fd |
| `EventLoop`         | 基于 `poll()` 等待 fd readable/writable 事件并分发 callback，不拥有 fd                               |
| `RedisServer`       | 管理客户端 session、accept 新连接、注册读写事件、执行命令、清理 owned client fd                                 |
| `RedisServerRunner` | 组合 listener、server 和 event loop，提供 `start` / `runOnce` / `stop`                         |
| `ClientSession`     | 保存单个客户端的 query buffer、reply buffer、当前 DB 上下文和关闭标记                                       |
| `client_socket_io`  | 将 fd read/write 结果转换成 `ClientIoStatus`，并驱动 `ClientSession` buffer                       |
| `CommandProcessor`  | 调用命令执行器，并在成功写命令后协调 AOF 追加                                                               |
| `CommandExecutor`   | 参数校验、命令分发、读写 Redis 数据结构并生成 `RespValue`                                                  |

时间语义上，事件注册不携带命令执行时间。客户端 readable callback 真正触发时才调用 `ExpireClock::now()`，因此 `PEXPIRE`、`TTL`、`GET` 等命令会按真实执行时刻处理过期逻辑。

---

## 已实现功能

### 工程与构建

- [x] CMake 工程初始化
- [x] `HyperRedisCore` 静态库
- [x] `hyper_redis_server` 可执行服务端
- [x] `include/hyper/`、`src/`、`apps/`、`test/` 分层
- [x] GoogleTest 测试注册和 CTest 集成
- [x] C++20 构建标准
- [x] spdlog 日志

### 底层数据结构

- [x] `dict`：双表渐进式 rehash、透明字符串查找、随机 key
- [x] `linked_list`：双向链表、插入、删除、遍历、移动语义
- [x] `intset`：有序整数集合、编码升级、紧凑存储
- [x] `ziplist`：紧凑 entry 编码、整数编码、插入删除、级联更新
- [x] `skipList`：score/rank 查询、同分排序、区间查找和区间删除

### Redis 对象模型

- [x] String：raw/int 编码、追加、范围读写、整数/浮点自增和溢出检查
- [x] List：ziplist / linked-list 编码切换
- [x] Hash：ziplist / hash table 编码切换
- [x] Set：intset / hash table 编码切换
- [x] ZSet：ziplist / skiplist 编码切换
- [x] 编码转换后保持 Redis 可见行为一致

### 数据库与过期机制

- [x] `RedisDb` 主 key-value 字典
- [x] 独立过期字典
- [x] `TTL` / `PTTL`
- [x] `EXPIRE` / `PEXPIRE` / `PEXPIREAT`
- [x] `NX` / `XX` / `GT` / `LT` 过期条件
- [x] `PERSIST`
- [x] 惰性过期：访问 key 时检查并删除过期 key
- [x] 主动过期：`activeExpireCycle`
- [x] `RedisManager` 多 DB 管理，默认 16 个 DB
- [x] `RedisClientContext` 保存客户端当前 DB

### RESP、客户端会话与网络

- [x] RESP2 响应序列化
- [x] RESP2 array-of-bulk-strings 命令解析
- [x] 半包、连续命令、协议错误和二进制 bulk 参数处理
- [x] `ClientSession` query buffer / reply buffer
- [x] `client_socket_io` fd read/write 适配
- [x] 非阻塞 socket `WouldBlock`、peer close、部分写入消费
- [x] `EventLoop` readable/writable 文件事件
- [x] `TcpListener` 生产版 TCP listener 封装
- [x] `RedisServer` listen/accept/client read/write 事件接入
- [x] `RedisServerRunner` 启动生命周期封装
- [x] `redis-cli` 可连接并执行命令

### 持久化

- [x] RDB-like `Snapshot` 字节编解码
- [x] `RdbSaver` 文件保存与加载
- [x] String / List / Set / Hash / ZSet round-trip
- [x] 快照保存时过滤已过期 key
- [x] 快照加载时跳过已过期 key
- [x] checksum 校验
- [x] AOF-like 命令追加
- [x] AOF replay 使用同一命令执行路径恢复数据
- [x] AOF append 失败后进入 broken 状态并拒绝后续写命令
- [x] appendfsync `no` / `always` / `everysec`
- [x] AOF rewrite 从当前 DB 生成紧凑命令序列
- [x] AOF rewrite 使用 `PEXPIREAT` 保存绝对过期时间

---

## 已支持命令

当前命令入口由 `CommandRegistry` 统一登记，包含 arity 和写命令分类。

| 分类           | 命令                                                                                                                                                        |
|--------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| 连接 / DB / 通用 | `PING`、`SELECT`、`DBSIZE`、`DEL`、`EXISTS`、`TYPE`、`TTL`、`PTTL`、`PERSIST`、`EXPIRE`、`PEXPIRE`、`PEXPIREAT`、`FLUSHDB`、`FLUSHALL`、`RANDOMKEY`、`RENAME`、`RENAMENX` |
| String       | `SET`、`GET`、`MGET`、`MSET`、`STRLEN`、`APPEND`、`INCR`、`DECR`、`INCRBY`、`INCRBYFLOAT`、`GETRANGE`、`SETRANGE`                                                    |
| List         | `LPUSH`、`RPUSH`、`LPOP`、`RPOP`、`LLEN`、`LRANGE`、`LINDEX`、`LSET`、`LINSERT`、`LREM`、`LTRIM`                                                                    |
| Hash         | `HSET`、`HGET`、`HDEL`、`HLEN`、`HGETALL`、`HEXISTS`、`HKEYS`、`HVALS`                                                                                           |
| Set          | `SADD`、`SREM`、`SISMEMBER`、`SCARD`、`SMEMBERS`、`SPOP`、`SRANDMEMBER`                                                                                         |
| ZSet         | `ZADD`、`ZREM`、`ZSCORE`、`ZCARD`、`ZRANGE`、`ZRANK`、`ZREVRANK`、`ZCOUNT`、`ZREVRANGE`、`ZINCRBY`、`ZREMRANGEBYRANK`、`ZREMRANGEBYSCORE`                            |

这些命令既可以通过单元测试直接调用命令层，也可以通过 TCP server 使用 `redis-cli` 发送 RESP 命令。

---

## 测试覆盖

当前测试覆盖：

- 底层结构：`linked_list`、`dict`、`skipList`、`intset`、`ziplist`
- 对象模型：String、List、Hash、Set、ZSet 及编码转换
- Redis 可见行为：范围、rank、随机成员、错误语义、边界值
- 数据库：过期、TTL/PTTL、绝对过期时间读取、rename、random key、active expire
- 多 DB：`RedisManager` 与 `RedisClientContext`
- RDB-like：快照 round-trip、文件 round-trip、坏文件保护、checksum
- AOF-like：append、DB 切换、replay、rewrite、fsync 策略、坏文件保护、append 失败语义
- 命令层：分发、参数校验、wrong type、Redis 风格响应、dirty count
- RESP：响应序列化、命令数组解析、半包、错误帧、连续命令缓冲、二进制 bulk
- 客户端会话：query/reply buffer、半包、连续命令、协议错误、二进制 bulk reply
- socket I/O：read/write、`WouldBlock`、peer close、部分写入消费
- 事件循环：readable/writable 触发、事件删除、同 fd 部分 mask 删除、callback 内删除事件
- TCP/server：listener nonblocking、accept、client read/write、runner 生命周期、真实 TCP PING
- 时间回归：同一连接中 `PEXPIRE` 后按真实当前时间过期，而不是使用注册回调时冻结的时间

运行全部测试：

```bash
ctest --test-dir build --output-on-failure
```

运行 runner 测试：

```bash
./build/redis_server_runner_test
```

运行生产 TCP listener 集成测试：

```bash
HYPERREDIS_RUN_TCP_LISTENER_TESTS=1 ./build/redis_server_test --gtest_filter='RedisServerTcpIntegrationTest.*'
```

---

## 与 Redis 设计的对应关系

| Redis 设计                               | HyperRedis 表达                                                   |
|----------------------------------------|-----------------------------------------------------------------|
| `redisObject` 使用 type + encoding + ptr | `RedisObject` 使用 `ObjectType`、`ObjectEncoding` 和 `std::variant` |
| SDS 字符串                                | `std::string` / `std::string_view`                              |
| `dict` 双表渐进式 rehash                    | 模板 `dict<K, V>` + 双表 + rehash step                              |
| 小对象紧凑编码                                | `intset`、`ziplist`、ziplist/list/hash/set/zset 编码升级              |
| ZSet skiplist + dict                   | `skipList` 支持 score range、rank 和区间删除                            |
| 主字典 + 过期字典                             | `RedisDb` 维护 `main_dict_` 和 `expire_dict_`                      |
| RESP client query buffer               | `ClientSession` query buffer + `parseRespCommand`               |
| client reply buffer                    | `ClientSession` reply buffer + `serializeRespValue`             |
| ae 文件事件                                | `EventLoop` 使用 `poll()` 分发 readable/writable callback           |
| TCP listener + accept                  | `TcpListener` + `RedisServer::attachListener`                   |
| server lifecycle                       | `RedisServerRunner` 组合 listener/server/event loop               |
| AOF append/replay/rewrite              | `AofAppender` / `AofReplayer` / `AofRewriter`                   |

---

## 当前限制

HyperRedis 现在已经可以通过 `redis-cli` 交互，但仍是学习版 server，不是生产级 Redis：

- 暂未实现 `COMMAND`、`INFO`、`CONFIG` 等 redis-cli 辅助命令。
- 暂未实现 AUTH、ACL、复制、Sentinel、Cluster。
- 服务端目前是单线程事件循环，没有后台任务线程。
- AOF/RDB-like 组件已经实现，但还没有完整接入服务端启动加载和关闭保存流程。
- 暂未实现 serverCron/time event 骨架，主动过期、AOF everysec fsync 等周期任务还没有统一调度。
- 暂未实现优雅退出信号处理。
- 启动失败时错误信息仍较粗，需要进一步携带 bind/listen errno。
- 尚未做系统性 benchmark。

---

## 后续计划

### 近期

- [x] 客户端 query buffer / reply buffer
- [x] RESP parser 接入客户端会话
- [x] socket read/write I/O 适配层
- [x] `poll`-based `EventLoop`
- [x] TCP listener 封装
- [x] `RedisServer` 接入 listen/accept/client read/write
- [x] `RedisServerRunner` 启动生命周期
- [x] `hyper_redis_server` 可执行入口
- [x] `redis-cli` 手动交互验证
- [x] 修正网络命令执行时间：事件触发时取当前时间
- [ ] `--help` 区分正常退出和参数错误
- [ ] 启动失败返回更具体 errno 信息
- [ ] 支持 Ctrl-C 优雅停止 server
- [ ] 设计 serverCron/time event，周期执行主动过期和 AOF everysec fsync

### 持久化接入

- [ ] 服务端启动时按配置加载 RDB/AOF
- [ ] 服务端关闭时执行同步保存或关闭 AOF
- [ ] 将 AOF append 接入真实 TCP 写命令路径
- [ ] 在 serverCron 中调度 AOF everysec fsync
- [ ] 增加同步 `SAVE` 命令入口
- [ ] 设计 `BGSAVE` / `BGWRITEAOF` 的后台任务状态模型

### 兼容性与性能

- [ ] 增加 `COMMAND` / `INFO` 等 redis-cli 友好命令的最小兼容实现
- [ ] 增加更多 Redis 行为兼容测试
- [ ] 增加 benchmark，量化命令执行、数据结构和网络层性能
- [ ] 完善大请求、大响应、最大客户端数和 idle timeout
- [ ] 在单机服务稳定后，再评估复制、Sentinel、Cluster 等分布式机制

---

## 开发约定

- 公共头文件放在 `include/hyper/`
- 实现文件放在 `src/`
- 可执行入口放在 `apps/`
- 测试放在 `test/`
- 新增 `.cpp` 文件和测试目标必须显式加入 `CMakeLists.txt`
- Public API 使用 lowerCamelCase
- 私有 helper 使用 lowerCamelCase_ 后缀
- 私有数据成员使用 lower_snake_case_ 后缀
- 核心行为优先用 GoogleTest 固化回归

---

## 致谢

本项目的设计思想与学习路径主要来自 Redis 源码、《Redis 设计与实现》以及 Redis 社区资料。这个仓库是个人学习过程中的手写实践与复盘，不是 Redis 的替代品。
